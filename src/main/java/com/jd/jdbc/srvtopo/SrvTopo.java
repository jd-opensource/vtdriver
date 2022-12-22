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

import com.jd.jdbc.concurrency.AllErrorRecorder;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.key.CurrentShard;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.topo.TopoException;
import com.jd.jdbc.topo.TopoServer;
import com.jd.jdbc.util.threadpool.impl.VtDaemonExecutorService;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static com.jd.jdbc.topo.TopoExceptionCode.NO_NODE;

public class SrvTopo {
    private static final Log log = LogFactory.getLog(SrvTopo.class);

    private static final Duration SRV_TOPO_CACHE_TTL = Duration.ofHours(30L * 24L);

    private static final Duration SRV_TOPO_CACHE_REFRESH = Duration.ofSeconds(5L);

    private static final Map<TopoServer, ResilientServer> resilientServer_global = new HashMap<>();

    /**
     * @param base
     * @param counterPrefix
     * @return
     * @throws SrvTopoException
     */
    public static ResilientServer newResilientServer(TopoServer base, String counterPrefix) throws SrvTopoException {
        if (SRV_TOPO_CACHE_REFRESH.compareTo(SRV_TOPO_CACHE_TTL) > 0) {
            throw new SrvTopoException("srv_topo_cache_refresh must be less than or equal to srv_topo_cache_ttl");
        }

        synchronized (ResilientServer.class) {
            if (resilientServer_global.containsKey(base)) {
                return resilientServer_global.get(base);
            }

            ResilientServer resilientServer = new ResilientServer(base,
                SRV_TOPO_CACHE_TTL,
                SRV_TOPO_CACHE_REFRESH,
                new ConcurrentHashMap<>(16),
                new ConcurrentHashMap<>(16));

            resilientServer_global.put(base, resilientServer);
            return resilientServer;
        }
    }

    /**
     * @param ctx
     * @param srvTopoServer
     * @param cell
     * @param tabletTypeList
     * @return
     * @throws SrvTopoException
     */
    public static List<Query.Target> findAllTargets(IContext ctx, SrvTopoServer srvTopoServer, String cell, Set<String> keyspaceNameSet, List<Topodata.TabletType> tabletTypeList)
        throws InterruptedException, SQLException {
        List<Query.Target> targetList = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(keyspaceNameSet.size());
        ReentrantLock lock = new ReentrantLock(true);
        AllErrorRecorder errRecorder = new AllErrorRecorder();

        for (String keyspace : keyspaceNameSet) {
            VtDaemonExecutorService.execute(() -> {
                try {
                    // Get SrvKeyspace for cell/keyspace.
                    ResilientServer.GetSrvKeyspaceResponse srvKeyspace = srvTopoServer.getSrvKeyspace(ctx, cell, keyspace);
                    Topodata.SrvKeyspace ks = srvKeyspace.getSrvKeyspace();
                    Exception err = srvKeyspace.getException();
                    if (err != null) {
                        if (TopoException.isErrType(err, NO_NODE)) {
                            // Possibly a race condition, or leftover
                            // crud in the topology service. Just log it.
                            log.error("GetSrvKeyspace(" + cell + ", " + keyspace + ") returned ErrNoNode, skipping that SrvKeyspace");
                        } else {
                            // More serious error, abort.
                            errRecorder.recordError(err);
                        }
                        return;
                    }

                    // Get all shard names that are used for serving.
                    for (Topodata.SrvKeyspace.KeyspacePartition ksPartition : ks.getPartitionsList()) {
                        // Check we're waiting for tablets of that type.
                        boolean waitForIt = false;
                        for (Topodata.TabletType tt : tabletTypeList) {
                            if (tt.equals(ksPartition.getServedType())) {
                                waitForIt = true;
                                break;
                            }
                        }
                        if (!waitForIt) {
                            continue;
                        }

                        // Add all the shards. Note we can't have
                        // duplicates, as there is only one entry per
                        // TabletType in the Partitions list.
                        lock.lock();
                        try {
                            CurrentShard.setShardReferences(keyspace, ksPartition.getShardReferencesList());
                            for (Topodata.ShardReference shard : ksPartition.getShardReferencesList()) {
                                targetList.add(Query.Target.newBuilder()
                                    .setCell(cell)
                                    .setKeyspace(keyspace)
                                    .setShard(shard.getName())
                                    .setTabletType(ksPartition.getServedType())
                                    .build());
                            }
                        } finally {
                            lock.unlock();
                        }
                    }
                } finally {
                    countDownLatch.countDown();
                }
            });
        }
        countDownLatch.await(10, TimeUnit.SECONDS);
        if (errRecorder.hasErrors()) {
            throw errRecorder.error();
        }
        return targetList;
    }
}
