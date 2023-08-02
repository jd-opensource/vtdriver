/*
Copyright 2021 JD Project Authors. Licensed under Apache-2.0.

Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.jd.jdbc.srvtopo;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jd.jdbc.common.util.Crc32Utill;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.context.VtContext;
import com.jd.jdbc.key.CurrentShard;
import com.jd.jdbc.monitor.SrvKeyspaceCollector;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.topo.Topo;
import com.jd.jdbc.topo.TopoException;
import com.jd.jdbc.topo.TopoExceptionCode;
import com.jd.jdbc.topo.TopoServer;
import com.jd.jdbc.util.threadpool.impl.VtDaemonExecutorService;
import io.etcd.jetcd.Watch;
import io.vitess.proto.Topodata;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import vschema.Vschema;

public class ResilientServer implements SrvTopoServer {
    private static final Log log = LogFactory.getLog(SrvTopo.class);

    private static final ScheduledThreadPoolExecutor srvKeyspaceTimer;

    static {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("srvKeyspace-Timer").build();
        srvKeyspaceTimer = new ScheduledThreadPoolExecutor(1, threadFactory, new ThreadPoolExecutor.DiscardPolicy());
        srvKeyspaceTimer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        srvKeyspaceTimer.setRemoveOnCancelPolicy(true);
    }

    private final TopoServer topoServer;

    private final Duration cacheTtl;

    private final Duration cacheRefresh;

    private final ReentrantReadWriteLock rwLock;

    private final Map<String, SrvKeyspaceNamesEntry> srvKeyspaceNamesCache;

    private final Map<String, SrvKeyspaceEntry> srvKeyspaceCache;

    private final Set<String> keyspaceCellSet = ConcurrentHashMap.newKeySet();

    ResilientServer(TopoServer topoServer, Duration cacheTtl, Duration cacheRefresh, Map<String, SrvKeyspaceNamesEntry> srvKeyspaceNamesCache, Map<String, SrvKeyspaceEntry> srvKeyspaceCache) {
        this.topoServer = topoServer;
        this.cacheTtl = cacheTtl;
        this.cacheRefresh = cacheRefresh;
        this.rwLock = new ReentrantReadWriteLock(true);
        this.srvKeyspaceNamesCache = srvKeyspaceNamesCache;
        this.srvKeyspaceCache = srvKeyspaceCache;
        SrvKeyspaceCollector.getInstance().add(this);
        srvKeyspaceTimer.scheduleAtFixedRate(new GetSrvKeyspaceTask(), 3, 30, TimeUnit.SECONDS);
    }

    public List<SrvKeyspaceCollector.Info> getSrvKeyspaceCollectorInfo() {
        List<SrvKeyspaceCollector.Info> infoList = new ArrayList<>();
        Map<String, SrvKeyspaceEntry> map = new HashMap<>(srvKeyspaceCache);

        for (Map.Entry<String, SrvKeyspaceEntry> entry : map.entrySet()) {
            String keyspaceCell = entry.getKey();
            String[] split = keyspaceCell.split("\\.");
            Topodata.SrvKeyspace srvKeyspace = entry.getValue().value;
            long infoCrc32;

            if (srvKeyspace == null) {
                infoCrc32 = 0;
            } else {
                infoCrc32 = Crc32Utill.checksumByCrc32(srvKeyspace.toString().getBytes());
            }
            SrvKeyspaceCollector.Info info = new SrvKeyspaceCollector.Info(Long.toString(infoCrc32), split[0], split[1]);
            infoList.add(info);
        }
        return infoList;
    }

    /**
     * @return
     */
    @Override
    public TopoServer getTopoServer() {
        return this.topoServer;
    }

    /**
     * @param ctx
     * @param cell
     * @param staleOk
     * @return
     */
    @Override
    public GetSrvKeyspaceNamesResponse getSrvKeyspaceNames(IContext ctx, String cell, Boolean staleOk) {
        // find the entry in the cache, add it if not there
        this.fullyLock();
        SrvKeyspaceNamesEntry entry;
        try {
            if (!this.srvKeyspaceNamesCache.containsKey(cell)) {
                this.srvKeyspaceNamesCache.put(cell, new SrvKeyspaceNamesEntry(cell));
            }
            entry = this.srvKeyspaceNamesCache.get(cell);
        } finally {
            this.fullyUnlock();
        }

        // Lock the entry, and do everything holding the lock except
        // querying the underlying topo server.
        //
        // This means that even if the topo server is very slow, two concurrent
        // requests will only issue one underlying query.
        entry.lock.lock();
        try {
            boolean cacheValid = entry.value != null && Duration.between(entry.insertionTime, LocalDateTime.now()).compareTo(this.cacheTtl) < 0;
            if (!cacheValid && staleOk) {
                // Only allow stale results for a bounded period
                cacheValid = entry.value != null && Duration.between(entry.insertionTime, LocalDateTime.now()).compareTo(this.cacheTtl.plus(cacheRefresh.multipliedBy(2L))) < 0;
            }
            boolean shouldRefresh = Duration.between(entry.lastQueryTime, LocalDateTime.now()).compareTo(this.cacheRefresh) > 0;

            // If it is not time to check again, then return either the cached
            // value or the cached error but don't ask topo again.
            if (!shouldRefresh) {
                if (cacheValid) {
                    return new GetSrvKeyspaceNamesResponse(entry.value, null);
                }
                return new GetSrvKeyspaceNamesResponse(null, entry.lastError);
            }

            // Refresh the state in a background goroutine if no refresh is already
            // in progress none is already running. This way queries are not blocked
            // while the cache is still valid but past the refresh time, and avoids
            // calling out to the topo service while the lock is held.
            if (entry.refreshingChan == null) {
                entry.refreshingChan = new LinkedBlockingQueue<>(16);
                entry.lastQueryTime = LocalDateTime.now();

                VtDaemonExecutorService.execute(() -> {

                    Topo.GetSrvKeyspaceNamesResponse response = this.topoServer.getSrvKeyspaceNames(ctx, cell);
                    List<String> result = response.getKeyspaceNames();
                    Exception ex = response.getException();

                    entry.lock.lock();
                    try {
                        if (ex == null) {
                            // save the value we got and the current time in the cache
                            entry.insertionTime = LocalDateTime.now();
                            // Avoid a tiny race if TTL == refresh time (the default)
                            entry.lastQueryTime = entry.insertionTime;
                            entry.value = result;
                        } else {
                            if (entry.insertionTime.getSecond() == 0 && entry.insertionTime.getNano() == 0) {
                                log.error("GetSrvKeyspaceNames(" + ctx + ", " + cell + ") failed: " + ex.getMessage() + " (no cached value, caching and returning error)");
                            } else if (entry.value != null && Duration.between(entry.insertionTime, LocalDateTime.now()).compareTo(this.cacheTtl) < 0) {
                                log.error("GetSrvKeyspaceNames(" + ctx + ", " + cell + ") failed: " + ex.getMessage() + " (keeping cached value: " + entry.value + ")");
                            } else {
                                log.error("GetSrvKeyspaceNames(" + ctx + ", " + cell + ") failed: " + ex.getMessage() + " (cached value expired)");
                                entry.insertionTime = LocalDateTime.of(1, 1, 1, 1, 1, 1, 1);
                                entry.value = null;
                            }
                        }
                        entry.lastError = ex;
                        entry.lastErrorCtx = ctx;
                    } finally {
                        entry.refreshingChan.clear();
                        try {
                            entry.refreshingChan.put(new Object());
                        } catch (InterruptedException e) {
                            log.error(e.getMessage(), e);
                        }
                        entry.lock.unlock();
                    }
                });
            }

            // If the cached entry is still valid then use it, otherwise wait
            // for the refresh attempt to complete to get a more up to date
            // response.
            //
            // In the event that the topo service is slow or unresponsive either
            // on the initial fetch or if the cache TTL expires, then several
            // requests could be blocked on refreshingCond waiting for the response
            // to come back.
            if (cacheValid) {
                return new GetSrvKeyspaceNamesResponse(entry.value, null);
            }
        } finally {
            entry.lock.unlock();
        }

        LinkedBlockingQueue<Object> refreshingChan = (LinkedBlockingQueue<Object>) entry.refreshingChan;
        try {
            refreshingChan.take();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }

        if (entry.value != null) {
            return new GetSrvKeyspaceNamesResponse(entry.value, null);
        }

        return new GetSrvKeyspaceNamesResponse(null, entry.lastError);
    }

    /**
     * @param ctx
     * @param cell
     * @param keyspace
     * @return
     */
    @Override
    public GetSrvKeyspaceResponse getSrvKeyspace(IContext ctx, String cell, String keyspace) {
        SrvKeyspaceEntry entry = this.getSrvKeyspaceEntry(cell, keyspace);
        keyspaceCellSet.add(cell + "." + keyspace);
        entry.rwLock.readLock().lock();
        try {
            if (entry.watchState == WatchState.WATCH_STATE_RUNNING) {
                return new GetSrvKeyspaceResponse(entry.value, entry.lastError);
            }
        } finally {
            entry.rwLock.readLock().unlock();
        }

        synchronized (this) {
            entry.fullyLock();
            try {
                if (entry.watchState == WatchState.WATCH_STATE_RUNNING) {
                    return new GetSrvKeyspaceResponse(entry.value, entry.lastError);
                }

                int shouldRefresh = Duration.between(entry.lastErrorTime, LocalDateTime.now()).compareTo(this.cacheRefresh);
                if (shouldRefresh > 0 && entry.watchState == WatchState.WATCH_STATE_IDLE) {
                    entry.watchState = WatchState.WATCH_STATE_STARTING;
                    entry.watchStartingChan = new LinkedBlockingQueue<>(16);
                    VtDaemonExecutorService.execute(() -> watchSrvKeyspace(ctx, entry, cell, keyspace));
                }

            boolean cacheValid = entry.value != null
                && (Duration.between(entry.lastValueTime, LocalDateTime.now()).compareTo(this.cacheTtl) < 0);
            if (cacheValid) {
                return new GetSrvKeyspaceResponse(entry.value, null);
            }

                if (entry.watchState == WatchState.WATCH_STATE_STARTING) {
                    LinkedBlockingQueue<Object> watchStartingChan = (LinkedBlockingQueue<Object>) entry.watchStartingChan;
                    entry.fullyUnlock();
                    try {
                        watchStartingChan.take();
                    } catch (InterruptedException e) {
                        log.error(e.getMessage(), e);
                    }
                    entry.fullyLock();
                }

                if (entry.value != null) {
                    return new GetSrvKeyspaceResponse(entry.value, null);
                }
                return new GetSrvKeyspaceResponse(null, entry.lastError);
            } finally {
                entry.fullyUnlock();
            }
        }
    }

    /**
     * @param ctx
     * @param cell
     * @param callback
     */
    @Override
    public void watchSrvVschema(IContext ctx, String cell, Function<Vschema.SrvVSchema, Void> callback) {
        // TODO
    }

    /**
     * @param cell
     * @param keyspace
     * @return
     */
    private synchronized SrvKeyspaceEntry getSrvKeyspaceEntry(String cell, String keyspace) {
        String key = cell + "." + keyspace;

        SrvKeyspaceEntry entry = this.srvKeyspaceCache.get(key);
        if (entry == null) {
            entry = new SrvKeyspaceEntry(cell, keyspace);
            this.srvKeyspaceCache.put(key, entry);
        }
        return entry;
    }

    /**
     * @param callerCtx
     * @param entry
     * @param cell
     * @param keyspace
     */
    private void watchSrvKeyspace(IContext callerCtx, SrvKeyspaceEntry entry, String cell, String keyspace) {
        Topo.WatchSrvKeyspaceResponse watchSrvKeyspaceResponse;

        try {
            watchSrvKeyspaceResponse = this.topoServer.watchSrvKeyspace(callerCtx, cell, keyspace);
        } catch (TopoException e) {
            entry.fullyLock();
            try {
                entry.lastError = e;
                entry.lastErrorCtx = callerCtx;
                entry.lastErrorTime = LocalDateTime.now();
                if (TopoException.isErrType(e, TopoExceptionCode.NO_NODE)) {
                    entry.value = null;
                }
                entry.watchState = WatchState.WATCH_STATE_IDLE;
                entry.watchStartingChan.put(new Object());
            } catch (InterruptedException ex) {
                log.error(ex.getMessage(), ex);
            } finally {
                entry.fullyUnlock();
            }
            return;
        }
        Topo.WatchSrvKeyspaceData current = watchSrvKeyspaceResponse.getCurrent();
        BlockingQueue<Topo.WatchSrvKeyspaceData> change = watchSrvKeyspaceResponse.getChange();
        try (Watch.Watcher watcher = watchSrvKeyspaceResponse.getWatcher()) {
            entry.fullyLock();
            try {
                if (current.getTopoException() != null) {
                    entry.lastError = current.getTopoException();
                    entry.lastErrorCtx = callerCtx;
                    entry.lastErrorTime = LocalDateTime.now();
                    if (TopoException.isErrType(current.getTopoException(), TopoExceptionCode.NO_NODE)) {
                        entry.value = null;
                    }
                    log.error("Initial WatchSrvKeyspace failed for " + cell + "/" + keyspace + ": " + current.getTopoException().getMessage());

                    if (Duration.between(entry.lastValueTime, LocalDateTime.now()).compareTo(this.cacheTtl) > 0) {
                        log.error("WatchSrvKeyspace clearing cached entry for " + cell + "/" + keyspace);
                        entry.value = null;
                    }
                    entry.watchState = WatchState.WATCH_STATE_IDLE;
                    entry.watchStartingChan.put(new Object());
                    return;
                }
                entry.watchState = WatchState.WATCH_STATE_RUNNING;
                entry.watchStartingChan.put(new Object());
                entry.value = current.getValue();
                entry.lastValueTime = LocalDateTime.now();
                entry.lastError = null;
                entry.lastErrorCtx = null;
                entry.lastErrorTime = LocalDateTime.of(1, 1, 1, 1, 1, 1, 1);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            } finally {
                entry.fullyUnlock();
            }
            try {
                while (true) {
                    Topo.WatchSrvKeyspaceData c = change.take();
                    if (c.getTopoException() != null) {
                        String err = "WatchSrvKeyspace failed for " + cell + "/" + keyspace + ": " + c.getTopoException().getMessage();
                        log.error(err);
                        entry.fullyLock();
                        try {
                            if (TopoException.isErrType(current.getTopoException(), TopoExceptionCode.NO_NODE)) {
                                entry.value = null;
                            }
                            entry.watchState = WatchState.WATCH_STATE_IDLE;
                            entry.lastValueTime = LocalDateTime.now();
                            entry.lastError = new SrvTopoException(err);
                            entry.lastErrorCtx = null;
                            entry.lastErrorTime = LocalDateTime.now();
                            return;
                        } finally {
                            entry.fullyUnlock();
                        }
                    }
                    entry.fullyLock();
                    try {
                        entry.value = c.getValue();
                        entry.lastError = null;
                        entry.lastErrorCtx = null;
                        entry.lastErrorTime = LocalDateTime.of(1, 1, 1, 1, 1, 1, 1);
                    } finally {
                        entry.fullyUnlock();
                    }
                }
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private void fullyLock() {
        rwLock.writeLock().lock();
        rwLock.readLock().lock();
    }

    private void fullyUnlock() {
        rwLock.readLock().unlock();
        rwLock.writeLock().unlock();
    }

    /**
     *
     */
    private enum WatchState {
        /**
         *
         */
        WATCH_STATE_IDLE(0),
        WATCH_STATE_STARTING(1),
        WATCH_STATE_RUNNING(2);

        Integer value;

        WatchState(Integer value) {
            this.value = value;
        }
    }

    /**
     *
     */
    public static class GetSrvKeyspaceResponse {
        private final Topodata.SrvKeyspace srvKeyspace;

        private final Exception exception;

        GetSrvKeyspaceResponse(Topodata.SrvKeyspace srvKeyspace, Exception exception) {
            this.srvKeyspace = srvKeyspace;
            this.exception = exception;
        }

        public Topodata.SrvKeyspace getSrvKeyspace() {
            return srvKeyspace;
        }

        public Exception getException() {
            return exception;
        }
    }

    /**
     *
     */
    public static class GetSrvKeyspaceNamesResponse {
        private final List<String> keyspaceNameList;

        private final Exception exception;

        GetSrvKeyspaceNamesResponse(List<String> keyspaceNameList, Exception exception) {
            this.keyspaceNameList = keyspaceNameList;
            this.exception = exception;
        }

        public List<String> getKeyspaceNameList() {
            return keyspaceNameList;
        }

        public Exception getException() {
            return exception;
        }
    }

    /**
     *
     */
    private static class SrvKeyspaceNamesEntry {
        String cell;

        ReentrantLock lock;

        BlockingQueue<Object> refreshingChan;

        LocalDateTime insertionTime;

        LocalDateTime lastQueryTime;

        List<String> value;

        Exception lastError;

        IContext lastErrorCtx;

        SrvKeyspaceNamesEntry(String cell) {
            this.cell = cell;
            this.lock = new ReentrantLock(true);
            this.insertionTime = LocalDateTime.of(1, 1, 1, 1, 1, 1, 1);
            this.lastQueryTime = LocalDateTime.of(1, 1, 1, 1, 1, 1, 1);
        }
    }

    /**
     *
     */
    private static class SrvKeyspaceEntry {
        String cell;

        String keyspace;

        ReentrantReadWriteLock rwLock;

        WatchState watchState;

        BlockingQueue<Object> watchStartingChan;

        Topodata.SrvKeyspace value;

        Exception lastError;

        LocalDateTime lastValueTime;

        IContext lastErrorCtx;

        LocalDateTime lastErrorTime;

        SrvKeyspaceEntry(String cell, String keyspace) {
            this.cell = cell;
            this.keyspace = keyspace;
            this.rwLock = new ReentrantReadWriteLock(true);
            this.watchState = WatchState.WATCH_STATE_IDLE;
            this.watchStartingChan = null;
            this.value = null;
            this.lastError = null;
            this.lastValueTime = LocalDateTime.of(1, 1, 1, 1, 1, 1, 1);
            this.lastErrorCtx = null;
            this.lastErrorTime = LocalDateTime.of(1, 1, 1, 1, 1, 1, 1);
        }

        void fullyLock() {
            rwLock.writeLock().lock();
            rwLock.readLock().lock();
        }

        void fullyUnlock() {
            rwLock.readLock().unlock();
            rwLock.writeLock().unlock();
        }
    }

    class GetSrvKeyspaceTask implements Runnable {
        private final IContext ctx = VtContext.withCancel(VtContext.background());

        public GetSrvKeyspaceTask() {
        }

        @Override
        public void run() {
            for (String keyspaceCell : keyspaceCellSet) {
                String[] split = keyspaceCell.split("\\.");
                String cell = split[0];
                String keyspace = split[1];
                try {
                    Topodata.SrvKeyspace srvKeyspace = topoServer.getSrvKeyspace(ctx, cell, keyspace);
                    SrvKeyspaceEntry entry = getSrvKeyspaceEntry(cell, keyspace);
                    if (!Objects.equals(entry.value, srvKeyspace)) {
                        entry.value = srvKeyspace;
                    }
                    List<Topodata.SrvKeyspace.KeyspacePartition> partitionsList = srvKeyspace.getPartitionsList();
                    List<Topodata.ShardReference> shardReferencesList = null;
                    for (Topodata.SrvKeyspace.KeyspacePartition keyspacePartition : partitionsList) {
                        if (!Topodata.TabletType.MASTER.equals(keyspacePartition.getServedType())) {
                            continue;
                        }
                        shardReferencesList = keyspacePartition.getShardReferencesList();
                    }
                    CurrentShard.setShardReferences(keyspace, shardReferencesList);
                    SrvKeyspaceCollector.getSrvKeyspaceTaskCounter().labels(keyspace, cell).inc();
                } catch (TopoException e) {
                    if (TopoException.isErrType(e, TopoExceptionCode.NO_NODE)) {
                        log.warn("getSrvKeyspace error,cause by" + e.getMessage());
                        continue;
                    }
                    log.error("srvKeyspace-Timer getSrvKeyspace failed for " + cell + "/" + keyspace + ": " + e.getMessage());
                    SrvKeyspaceCollector.getSrvKeyspaceTaskErrorCounter().labels(keyspace, cell).inc();
                } catch (Exception e) {
                    log.error("srvKeyspace-Timer failed for " + cell + "/" + keyspace + ": " + e.getMessage());
                    SrvKeyspaceCollector.getSrvKeyspaceTaskErrorCounter().labels(keyspace, cell).inc();
                }
            }
        }
    }
}
