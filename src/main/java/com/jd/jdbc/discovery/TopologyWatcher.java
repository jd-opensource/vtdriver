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

import com.jd.jdbc.context.IContext;
import com.jd.jdbc.monitor.TopologyCollector;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.topo.TopoException;
import com.jd.jdbc.topo.TopoServer;
import com.jd.jdbc.topo.TopoTabletInfo;
import com.jd.jdbc.topo.topoproto.TopoProto;
import com.jd.jdbc.util.threadpool.impl.VtHealthCheckExecutorService;
import io.vitess.proto.Topodata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.jd.jdbc.topo.TopoExceptionCode.NO_NODE;

public class TopologyWatcher {

    private static final Log log = LogFactory.getLog(TopologyWatcher.class);

    private final ReentrantLock lock = new ReentrantLock();

    private final TopoServer ts;

    private final Set<String> ksSet = new ConcurrentHashMap().newKeySet();

    private final IgnoreTopo ignoreTopo = new IgnoreTopo();

    private final Timer timer = new Timer(true);

    private final HealthCheck hc;

    private final String cell;

    private boolean firstLoadTablets;

    private Map<String, TopoTabletInfo> currentTablets = new ConcurrentHashMap<>(16);

    public TopologyWatcher(TopoServer ts, String cell, Set<String> keyspaces) {
        this.ts = ts;
        this.hc = HealthCheck.INSTANCE;
        this.cell = cell;
        this.ksSet.addAll(keyspaces);
        this.firstLoadTablets = true;
        log.info("start topo watcher for cell: " + cell);
    }

    private void loadTablets(IContext ctx) {
        this.lock.lock();
        try {
            Map<String, TopoTabletInfo> newTablets;
            if (firstLoadTablets) {
                newTablets = new HashMap<>();
                List<TopoTabletInfo> topoTabletInfoList;
                try {
                    topoTabletInfoList = ts.getTabletsByRange(ctx, cell);
                } catch (TopoException e) {
                    if (TopoException.isErrType(e, NO_NODE)) {
                        return;
                    }
                    log.error("get topoTabletInfo fail", e);
                    //Exception will be thrown when tablet does not exist, so ignore and continue.
                    //Avoid network abnormalities that cause a large amount of Tablet metadata to be deleted from memory
                    TopologyCollector.getErrorCounter().labels(cell).inc();
                    return;
                }
                firstLoadTablets = false;
                for (TopoTabletInfo topoTabletInfo : topoTabletInfoList) {
                    if (!ksSet.contains(topoTabletInfo.getTablet().getKeyspace())) {
                        ignoreTopo.ignore(topoTabletInfo.getTablet().getKeyspace(), TopoProto.tabletAliasString(topoTabletInfo.getTablet().getAlias()));
                        continue;
                    }
                    newTablets.put(TopoProto.tabletAliasString(topoTabletInfo.getTablet().getAlias()), topoTabletInfo);
                }
            } else {
                newTablets = new ConcurrentHashMap<>();
                List<Topodata.TabletAlias> tablets;
                try {
                    tablets = ts.getTabletAliasByCell(ctx, cell);
                } catch (TopoException e) {
                    log.error("Build Tablets fail,current ksSet=" + ksSet, e);
                    TopologyCollector.getErrorCounter().labels(cell).inc();
                    return;
                }
                if (null == tablets || tablets.isEmpty()) {
                    return;
                }

                CountDownLatch cnt = new CountDownLatch(tablets.size());
                for (Topodata.TabletAlias alias : tablets) {
                    if (ignoreTopo.isIgnored(TopoProto.tabletAliasString(alias))) {
                        cnt.countDown();
                        continue;
                    }
                    VtHealthCheckExecutorService.execute(() -> {
                        try {
                            TopoTabletInfo topoTabletInfo = ts.getTablet(ctx, alias);
                            if (!ksSet.contains(topoTabletInfo.getTablet().getKeyspace())) {
                                ignoreTopo.ignore(topoTabletInfo.getTablet().getKeyspace(), TopoProto.tabletAliasString(alias));
                                return;
                            }
                            newTablets.put(TopoProto.tabletAliasString(alias), topoTabletInfo);
                        } catch (TopoException e) {
                            log.error("get topoTabletInfo fail", e);
                            //Exception will be thrown when tablet does not exist, so ignore and continue.
                            //Avoid network abnormalities that cause a large amount of Tablet metadata to be deleted from memory
                            TopologyCollector.getErrorCounter().labels(cell).inc();
                        } finally {
                            cnt.countDown();
                        }
                    });
                }
                try {
                    cnt.await();
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                    TopologyCollector.getErrorCounter().labels(cell).inc();
                }
            }

            for (Map.Entry<String, TopoTabletInfo> entry : newTablets.entrySet()) {
                TopoTabletInfo newTablet = entry.getValue();
                TopoTabletInfo oldTablet = currentTablets.get(entry.getKey());
                if (oldTablet == null) {
                    hc.addTablet(newTablet.getTablet());
                } else {
                    String oldKey = tabletToMapKey(oldTablet.getTablet());
                    String newKey = tabletToMapKey(newTablet.getTablet());
                    if (!oldKey.equals(newKey)) {
                        hc.replaceTablet(oldTablet.getTablet(), newTablet.getTablet());
                    }
                }
            }

            for (Map.Entry<String, TopoTabletInfo> entry : currentTablets.entrySet()) {
                if (!newTablets.containsKey(entry.getKey())) {
                    hc.removeTablet(entry.getValue().getTablet());
                }
            }
            currentTablets = newTablets;
            TopologyCollector.getCounter().labels(cell).inc();
        } finally {
            this.lock.unlock();
        }
    }

    public void start(final IContext ctx) {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                loadTablets(ctx);
            }
        }, 0, 30000);
    }

    public void watchKeyspace(IContext ctx, Set<String> keyspace) {
        if (ksSet.containsAll(keyspace)) {
            return;
        }
        this.lock.lock();
        try {
            boolean needNewWatch = false;
            for (String ks : keyspace) {
                if (this.ksSet.contains(ks)) {
                    continue;
                }
                log.info("topo watcher for cell " + cell + " watches: " + ks);
                needNewWatch = true;
                this.ksSet.add(ks);
                this.ignoreTopo.watchKs(ks);
            }
            if (needNewWatch) {
                this.loadTablets(ctx);
            }
        } finally {
            this.lock.unlock();
        }
    }

    public void stop() {
        timer.cancel();
    }

    public String tabletToMapKey(Topodata.Tablet tablet) {
        StringBuilder sb = new StringBuilder();
        sb.append(tablet.getHostname()).append(":").append(tablet.getMysqlPort());

        String[] keys = new ArrayList<>(tablet.getPortMapMap().keySet()).toArray(new String[0]);
        Arrays.sort(keys);

        for (String key : keys) {
            sb.append(',').append(key).append(":").append(tablet.getPortMapMap().get(key).intValue());
        }

        return sb.toString();
    }

    public static class IgnoreTopo {
        /**
         * Note: this is the only way of creating thread-safe Set in Java.
         * https://javatutorial.net/java-concurrenthashset-example
         **/
        private final Set<String> ignoreTablets = new ConcurrentHashMap().newKeySet();

        private final Map<String, Set<String>> ignoreKsTablets = new HashMap<>();

        private final Lock lock = new ReentrantLock();

        public void ignore(String keyspace, String tablet) {
            lock.lock();
            try {
                ignoreTablets.add(tablet);
                Set<String> ksTablets = ignoreKsTablets.get(keyspace);
                if (ksTablets == null) {
                    ksTablets = new HashSet<>();
                }

                ksTablets.add(tablet);
                ignoreKsTablets.putIfAbsent(keyspace, ksTablets);
            } finally {
                lock.unlock();
            }
        }

        public boolean isIgnored(String tablet) {
            return this.ignoreTablets.contains(tablet);
        }

        public void watchKs(String keyspace) {
            lock.lock();
            try {
                Set<String> tablets = this.ignoreKsTablets.get(keyspace);
                if (tablets != null) {
                    this.ignoreTablets.removeAll(tablets);
                    this.ignoreKsTablets.remove(keyspace);
                }
            } finally {
                lock.unlock();
            }
        }
    }
}
