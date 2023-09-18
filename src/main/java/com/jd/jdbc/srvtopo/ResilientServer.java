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

import com.jd.jdbc.context.IContext;
import com.jd.jdbc.context.VtContext;
import com.jd.jdbc.monitor.SrvKeyspaceCollector;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.topo.TopoException;
import com.jd.jdbc.topo.TopoExceptionCode;
import com.jd.jdbc.topo.TopoServer;
import com.jd.jdbc.util.threadpool.VtThreadFactoryBuilder;
import io.vitess.proto.Topodata;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import vschema.Vschema;

public class ResilientServer implements SrvTopoServer {
    private static final Log log = LogFactory.getLog(SrvTopo.class);

    private static final ScheduledThreadPoolExecutor srvKeyspaceTimer;

    static {
        ThreadFactory threadFactory = new VtThreadFactoryBuilder.DefaultThreadFactory("srvKeyspace-Timer", true);
        srvKeyspaceTimer = new ScheduledThreadPoolExecutor(1, threadFactory, new ThreadPoolExecutor.DiscardPolicy());
        srvKeyspaceTimer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        srvKeyspaceTimer.setRemoveOnCancelPolicy(true);
    }

    private final TopoServer topoServer;

    private final Set<String> keyspaceCellSet = ConcurrentHashMap.newKeySet();

    ResilientServer(TopoServer topoServer) {
        this.topoServer = topoServer;
        SrvKeyspaceCollector.getInstance().add(this);
        srvKeyspaceTimer.scheduleAtFixedRate(new GetSrvKeyspaceTask(), 3, 30, TimeUnit.SECONDS);
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
     * @param keyspace
     * @return
     */
    @Override
    public GetSrvKeyspaceResponse getSrvKeyspace(IContext ctx, String cell, String keyspace) {
        keyspaceCellSet.add(cell + "." + keyspace);
        try {
            Topodata.SrvKeyspace srvKeyspace = topoServer.getAndWatchSrvKeyspace(ctx, cell, keyspace);
            return new GetSrvKeyspaceResponse(srvKeyspace, null);
        } catch (TopoException e) {
            return new GetSrvKeyspaceResponse(null, e);
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
                    Topodata.SrvKeyspace srvKeyspaceFromCache = TopoServer.getSrvKeyspaceFromCache(keyspace);
                    if (!Objects.equals(srvKeyspace, srvKeyspaceFromCache)) {
                        TopoServer.updateSrvKeyspaceCache(keyspace, srvKeyspace);
                        log.error("Depending on the srvKeyspace-Timer update, the watch may be invalid." + cell + "/" + keyspace);
                        SrvKeyspaceCollector.getSrvKeyspaceTaskUpdateCounter().labels(keyspace, cell).inc();
                    }
                    SrvKeyspaceCollector.getSrvKeyspaceTaskCounter().labels(keyspace, cell).inc();
                } catch (TopoException e) {
                    if (TopoException.isErrType(e, TopoExceptionCode.NO_NODE)) {
                        log.warn("srvKeyspace-Timer getSrvKeyspace error,cause by" + e.getMessage());
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
