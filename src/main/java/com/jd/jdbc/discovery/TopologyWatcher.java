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

import com.jd.jdbc.common.util.CollectionUtils;
import com.jd.jdbc.common.util.MapUtil;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.monitor.TopologyCollector;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.topo.TopoException;
import com.jd.jdbc.topo.TopoExceptionCode;
import com.jd.jdbc.topo.TopoServer;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class TopologyWatcher {

    private static final Log log = LogFactory.getLog(TopologyWatcher.class);

    private final ReentrantLock lock = new ReentrantLock();

    private final TopoServer ts;

    private final Set<String> ksSet = ConcurrentHashMap.newKeySet();

    private final IgnoreTopo ignoreTopo = new IgnoreTopo();

    private final Timer timer = new Timer(true);

    private final HealthCheck hc;

    private final String cell;

    private boolean firstLoadTabletsFlag;

    private Map<String, Topodata.Tablet> currentTablets = new ConcurrentHashMap<>(16);

    public TopologyWatcher(TopoServer ts, String cell, String tabletKeyspace) {
        this.ts = ts;
        this.hc = HealthCheck.INSTANCE;
        this.cell = cell;
        this.ksSet.add(tabletKeyspace);
        this.firstLoadTabletsFlag = true;
        log.info("start topo watcher for cell: " + cell);
    }

    private void loadTablets(IContext ctx) {
        try {
            Map<String, Topodata.Tablet> newTablets = getTopoTabletInfoMap(ctx);
            connectTablets(newTablets);
            TopologyCollector.getCounter().labels(cell).inc();
        } catch (Throwable t) {
            TopologyCollector.getErrorCounter().labels(cell).inc();
            log.error("Unexpected error occur at loadTablets, cause: " + t.getMessage(), t);
        }
    }

    private void connectTablets(Map<String, Topodata.Tablet> newTablets) {
        if (CollectionUtils.isEmpty(newTablets)) {
            for (Map.Entry<String, Topodata.Tablet> entry : currentTablets.entrySet()) {
                hc.removeTablet(entry.getValue());
            }
            return;
        }
        this.lock.lock();
        try {
            for (Map.Entry<String, Topodata.Tablet> entry : newTablets.entrySet()) {
                Topodata.Tablet newTablet = entry.getValue();
                Topodata.Tablet oldTablet = currentTablets.get(entry.getKey());
                if (oldTablet == null) {
                    hc.addTablet(newTablet);
                } else {
                    String oldKey = tabletToMapKey(oldTablet);
                    String newKey = tabletToMapKey(newTablet);
                    if (!oldKey.equals(newKey)) {
                        hc.replaceTablet(oldTablet, newTablet);
                    }
                }
            }

            for (Map.Entry<String, Topodata.Tablet> entry : currentTablets.entrySet()) {
                if (!newTablets.containsKey(entry.getKey())) {
                    hc.removeTablet(entry.getValue());
                }
            }
            currentTablets = newTablets;
        } finally {
            this.lock.unlock();
        }
    }

    private Map<String, Topodata.Tablet> getTopoTabletInfoMap(IContext ctx) throws TopoException, InterruptedException {
        Map<String, Topodata.Tablet> newTablets;
        if (firstLoadTabletsFlag) {
            List<Topodata.Tablet> tabletList = ts.getTabletsByRange(ctx, cell);
            newTablets = new HashMap<>(16);
            for (Topodata.Tablet tablet : tabletList) {
                if (StringUtils.isEmpty(tablet.getKeyspace())) {
                    continue;
                }
                if (!ksSet.contains(tablet.getKeyspace())) {
                    ignoreTopo.ignore(tablet.getKeyspace(), tablet);
                    continue;
                }
                newTablets.put(TopoProto.tabletAliasString(tablet.getAlias()), tablet);
            }
            firstLoadTabletsFlag = false;
            return newTablets;
        }

        List<Topodata.TabletAlias> tablets = ts.getTabletAliasByCell(ctx, cell);
        if (CollectionUtils.isEmpty(tablets)) {
            return null;
        }

        // 更新ignoreTopo
        Set<String> tabletAliasSet = tablets.stream().map(TopoProto::tabletAliasString).collect(Collectors.toSet());
        for (String ignoreTabletAlias : ignoreTopo.getIgnoreTabletAlias()) {
            if (tabletAliasSet.contains(ignoreTabletAlias)) {
                continue;
            }
            ignoreTopo.expire(ignoreTabletAlias);
        }

        // 过滤tablets
        tablets = tablets.stream()
            .filter(tabletAlias -> !ignoreTopo.isIgnored(TopoProto.tabletAliasString(tabletAlias)))
            .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(tablets)) {
            return null;
        }
        newTablets = new ConcurrentHashMap<>(tablets.size());
        CountDownLatch countDownLatch = new CountDownLatch(tablets.size());
        for (Topodata.TabletAlias alias : tablets) {
            VtHealthCheckExecutorService.execute(() -> {
                try {
                    Topodata.Tablet tablet = ts.getTablet(ctx, alias);
                    if (StringUtils.isEmpty(tablet.getKeyspace())) {
                        return;
                    }
                    if (!ksSet.contains(tablet.getKeyspace())) {
                        ignoreTopo.ignore(tablet.getKeyspace(), tablet);
                        return;
                    }
                    newTablets.put(TopoProto.tabletAliasString(alias), tablet);
                } catch (TopoException e) {
                    if (TopoException.isErrType(e, TopoExceptionCode.NO_NODE)) {
                        log.warn("getTablet error,cause by" + e.getMessage());
                        return;
                    }
                    log.error("get topoTabletInfo fail", e);
                    // Exception will be thrown when tablet does not exist, so ignore and continue.
                    // Avoid network abnormalities that cause a large amount of Tablet metadata to be deleted from memory
                    TopologyCollector.getErrorCounter().labels(cell).inc();
                } finally {
                    countDownLatch.countDown();
                }
            });
        }
        countDownLatch.await(10, TimeUnit.SECONDS);
        return newTablets;
    }

    public void start(final IContext ctx) {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                loadTablets(ctx);
            }
        }, 0, 30000);
    }

    public void watchKeyspace(IContext ctx, String tabletKeyspace) {
        if (ksSet.contains(tabletKeyspace)) {
            return;
        }
        log.info("topo watcher for cell " + cell + " watches: " + tabletKeyspace);
        this.ksSet.add(tabletKeyspace);
        Set<Topodata.Tablet> tablets = this.ignoreTopo.watchKs(tabletKeyspace);
        if (CollectionUtils.isEmpty(tablets)) {
            return;
        }
        for (Topodata.Tablet tablet : tablets) {
            hc.addTablet(tablet);
        }
        this.lock.lock();
        try {
            Map<String, Topodata.Tablet> newTablets = tablets.stream()
                .collect(Collectors.toMap(a -> TopoProto.tabletAliasString(a.getAlias()), s -> s, (s1, s2) -> s1));
            currentTablets.putAll(newTablets);
        } finally {
            this.lock.unlock();
        }
    }

    public void close() {
        timer.cancel();
        for (Map.Entry<String, Topodata.Tablet> entry : currentTablets.entrySet()) {
            hc.removeTablet(entry.getValue());
        }
        currentTablets.clear();
    }

    private String tabletToMapKey(Topodata.Tablet tablet) {
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
        private final Map<String, Topodata.Tablet> ignoreTablets = new ConcurrentHashMap<>();

        private final Map<String, Set<Topodata.Tablet>> ignoreKsTablets = new HashMap<>();

        private final Lock lock = new ReentrantLock();

        public void ignore(String keyspace, Topodata.Tablet tablet) {
            lock.lock();
            try {
                ignoreTablets.put(TopoProto.tabletAliasString(tablet.getAlias()), tablet);
                MapUtil.computeIfAbsent(ignoreKsTablets, keyspace, set -> new HashSet<>()).add(tablet);
            } finally {
                lock.unlock();
            }
        }

        public void expire(String tabletAlias) {
            lock.lock();
            try {
                Topodata.Tablet tablet = ignoreTablets.remove(tabletAlias);
                Set<Topodata.Tablet> ksTablets = ignoreKsTablets.get(tablet.getKeyspace());
                ksTablets.remove(tablet);
                if (ksTablets.isEmpty()) {
                    ignoreKsTablets.remove(tablet.getKeyspace());
                }
            } finally {
                lock.unlock();
            }
        }

        public boolean isIgnored(String tabletAlias) {
            return this.ignoreTablets.containsKey(tabletAlias);
        }

        public Set<String> getIgnoreTabletAlias() {
            return this.ignoreTablets.keySet();
        }

        public Set<Topodata.Tablet> watchKs(String keyspace) {
            lock.lock();
            try {
                Set<Topodata.Tablet> tablets = this.ignoreKsTablets.remove(keyspace);
                if (CollectionUtils.isEmpty(tablets)) {
                    return null;
                }
                for (Topodata.Tablet tablet : tablets) {
                    this.ignoreTablets.remove(TopoProto.tabletAliasString(tablet.getAlias()));
                }
                return tablets;
            } finally {
                lock.unlock();
            }
        }
    }
}
