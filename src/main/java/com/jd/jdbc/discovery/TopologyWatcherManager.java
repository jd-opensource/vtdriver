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

package com.jd.jdbc.discovery;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jd.jdbc.common.util.CollectionUtils;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.topo.TopoException;
import com.jd.jdbc.topo.TopoServer;
import com.jd.jdbc.util.threadpool.VtThreadFactoryBuilder;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public enum TopologyWatcherManager {
    /**
     * INSTANCE
     */
    INSTANCE;

    private Map<String, TopologyWatcher> cellTopologyWatcherMap = null;

    private Map<String, Set<String>> globalKeyspacesMap = null;

    private final Lock lock = new ReentrantLock();

    private static final Log LOGGER = LogFactory.getLog(TopologyWatcherManager.class);

    private ScheduledThreadPoolExecutor scheduledExecutor;

    TopologyWatcherManager() {
        cellTopologyWatcherMap = new ConcurrentHashMap<>(16);
        globalKeyspacesMap = new ConcurrentHashMap<>(16);

        scheduledExecutor = new ScheduledThreadPoolExecutor(1, VtThreadFactoryBuilder.build("reload-cell-schedule"));
        scheduledExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        scheduledExecutor.setRemoveOnCancelPolicy(true);
    }

    public void startWatch(IContext ctx, TopoServer topoServer, String cell, String tabletKeyspace) {
        lock.lock();
        try {
            String serverAddress = topoServer.getServerAddress();
            if (!globalKeyspacesMap.containsKey(serverAddress)) {
                globalKeyspacesMap.put(serverAddress, new HashSet<>());

                startTickerReloadCell(ctx, topoServer, TimeUnit.MINUTES);
            }
            globalKeyspacesMap.get(serverAddress).add(tabletKeyspace);

            if (!cellTopologyWatcherMap.containsKey(cell)) {
                TopologyWatcher topologyWatcher = new TopologyWatcher(topoServer, cell, tabletKeyspace);
                topologyWatcher.start(ctx);
                cellTopologyWatcherMap.put(cell, topologyWatcher);
            }
        } finally {
            lock.unlock();
        }
    }

    public void watch(IContext ctx, String cell, String tabletKeyspace) {
        if (!cellTopologyWatcherMap.containsKey(cell)) {
            throw new RuntimeException("topo watcher for cell " + cell + " is not started");
        }
        cellTopologyWatcherMap.get(cell).watchKeyspace(ctx, tabletKeyspace);
    }

    public void close() {
        closeScheduledExecutor();

        for (Map.Entry<String, TopologyWatcher> entry : cellTopologyWatcherMap.entrySet()) {
            TopologyWatcher topologyWatcher = entry.getValue();
            topologyWatcher.close();
        }
        cellTopologyWatcherMap.clear();
        globalKeyspacesMap.clear();
    }

    public void resetScheduledExecutor() {
        closeScheduledExecutor();

        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("reload-cell-schedule").setDaemon(true).build();
        scheduledExecutor = new ScheduledThreadPoolExecutor(1, threadFactory);
    }

    public void closeScheduledExecutor() {
        scheduledExecutor.shutdownNow();
        try {
            int tryAgain = 3;
            while (tryAgain > 0 && !scheduledExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                tryAgain--;
            }
        } catch (InterruptedException e) {
            // We're shutting down anyway, so just ignore.
        }
    }

    public boolean isWatching(String cell) {
        return cellTopologyWatcherMap.containsKey(cell);
    }

    public void startTickerReloadCell(IContext globalContext, TopoServer topoServer, TimeUnit timeUnit) {
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            try {
                tickerUpdateCells(globalContext, topoServer);
            } catch (Throwable e) {
                LOGGER.error("tickerUpdateCells error: " + e);
            }
        }, 5, 10, timeUnit);
    }

    private void tickerUpdateCells(IContext globalContext, TopoServer topoServer) throws TopoException {
        String serverAddress = topoServer.getServerAddress();
        Set<String> keyspaceSet = globalKeyspacesMap.get(serverAddress);
        if (CollectionUtils.isEmpty(keyspaceSet)) {
            throw new RuntimeException("not found keyspace in " + serverAddress + " of TopologyWatcherManager.globalKeyspacesMap .");
        }
        List<String> allCells = topoServer.getAllCells(globalContext);
        for (String cell : allCells) {
            if (!isWatching(cell)) {
                lock.lock();
                try {
                    TopologyWatcher topologyWatcher = new TopologyWatcher(topoServer, cell, keyspaceSet);
                    topologyWatcher.start(globalContext);
                    cellTopologyWatcherMap.put(cell, topologyWatcher);
                } finally {
                    lock.unlock();
                }
            }
        }
    }
}
