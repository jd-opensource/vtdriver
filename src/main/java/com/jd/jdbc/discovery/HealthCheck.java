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
import com.jd.jdbc.queryservice.IQueryService;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.topo.topoproto.TopoProto;
import io.netty.util.internal.StringUtil;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


/*
<Effective Java>

Enum singleton - the preferred approach
    public enum Elvis {
        INSTANCE;
        public void leaveTheBuilding() { ... }
    }
This approach is functionally equivalent to the public field approach, except that it is more concise, provides the serialization machinery for free,
 and provides an ironclad guarantee against multiple instantiation, even in the face of sophisticated serialization or reflection attacks.
While this approach has yet to be widely adopted, a single-element enum type is the best way to implement a singleton.
* */
public enum HealthCheck {
    /**
     * Enum singleton
     */
    INSTANCE;

    private static final Log log = LogFactory.getLog(HealthCheck.class);

    private static int secondsBehindsMaster = 7200;

    static {
        String sysProp = System.getProperty("vtdriver.secondsBehindMaster");
        if (sysProp != null) {
            try {
                int parsedValue = Integer.parseInt(sysProp);
                if (parsedValue > 0) {
                    secondsBehindsMaster = parsedValue;
                }
            } catch (NumberFormatException e) {
                log.error("the value of the JVM option parameter vtdriver.secondsBehindMaster is invalid");
            }
        }
    }

    private final ReentrantLock lock = new ReentrantLock(true);

    // authoritative map of tabletHealth by alias
    private final Map<String, TabletHealthCheck> healthByAlias = new ConcurrentHashMap<>(16);

    // a map keyed by keyspace.shard.tabletType
    // contains a map of TabletHealth keyed by tablet alias for each tablet relevant to the keyspace.shard.tabletType
    // has to be kept in sync with healthByAlias
    private final Map<String, Map<String, TabletHealthCheck>> healthData = new ConcurrentHashMap<>(16); //{"keyspace_shard_tabletType":{"ht3a-123":{TabletHealth}}}

    // another map keyed by keyspace.shard.tabletType, this one containing a sorted list of TabletHealth
    private final Map<String, List<TabletHealthCheck>> healthy = new ConcurrentHashMap<>(16);

    private final AtomicInteger tabletCounter = new AtomicInteger(0);

    private final Timer streamWatcherTimer = new Timer(true);

    /**
     * only a HealthCheck impl in a application.
     */
    HealthCheck() {
        this.watchTabltHealthCheckStream();
    }

    public static String keyFromTarget(final Query.Target target) {
        return target.getKeyspace() + "." + target.getShard() + "." + TopoProto.tabletTypeLstring(target.getTabletType());
    }

    public static String keyFromTablet(final Topodata.Tablet tablet) {
        return tablet.getKeyspace() + "." + tablet.getShard() + "." + TopoProto.tabletTypeLstring(tablet.getType());
    }

    public Map<String, TabletHealthCheck> getHealthByAliasCopy() {
        return new HashMap<>(healthByAlias);
    }

    public IQueryService tabletConnection(Topodata.TabletAlias alias) {
        TabletHealthCheck thc = this.healthByAlias.get(TopoProto.tabletAliasString(alias));
        if (thc == null) {
            return null;
        }
        return thc.getQueryService();
    }

    public List<TabletHealthCheck> getHealthyTabletStatsMaybeStandby(Query.Target target) {
        if (target.getShard().isEmpty()) {
            target = target.toBuilder().setShard("0").build();
        }

        List<TabletHealthCheck> healthyTabletStats = getHealthyTabletStats(target);
        if (healthyTabletStats != null && !healthyTabletStats.isEmpty()) {
            return healthyTabletStats;
        }
        if (target.getTabletType() == Topodata.TabletType.REPLICA) {
            return getHealthyTabletStats(target.toBuilder().setTabletType(Topodata.TabletType.RDONLY).build());
        }
        if (target.getTabletType() == Topodata.TabletType.RDONLY) {
            return getHealthyTabletStats(target.toBuilder().setTabletType(Topodata.TabletType.REPLICA).build());
        }
        return healthyTabletStats;
    }

    public List<TabletHealthCheck> getHealthyTabletStats(Query.Target target) {
        this.lock.lock();
        try {
            List<TabletHealthCheck> list = this.healthy.get(keyFromTarget(target));
            if (null == list) {
                return null;
            }
            if (target.getTabletType() == Topodata.TabletType.MASTER) {
                return list;
            }

            List<TabletHealthCheck> servlist = new ArrayList<>();
            list.forEach(entry -> {
                if (entry.getServing().get()) {
                    servlist.add(entry);
                }
            });
            return servlist;
        } finally {
            this.lock.unlock();
        }
    }

    public Topodata.Tablet getHealthyTablets(final String keyspace, final Topodata.TabletType tabletType) {
        this.lock.lock();
        try {
            for (List<TabletHealthCheck> tabletHealthCheckList : healthy.values()) {
                if (tabletHealthCheckList == null || tabletHealthCheckList.isEmpty()) {
                    continue;
                }
                for (TabletHealthCheck tabletHealthCheck : tabletHealthCheckList) {
                    Topodata.Tablet tablet = tabletHealthCheck.getTablet();
                    if (tablet == null || !tablet.getKeyspace().equalsIgnoreCase(keyspace) || !tabletHealthCheck.getServing().get()) {
                        continue;
                    }
                    if (!Objects.equals(tabletType, tablet.getType())) {
                        continue;
                    }
                    return tablet;
                }
            }
            return null;
        } finally {
            this.lock.unlock();
        }
    }

    public void addTablet(Topodata.Tablet tablet) {
        if (tablet.getPortMapMap().get("grpc") == null) {
            return;
        }
        Query.Target target = Query.Target.newBuilder().setKeyspace(tablet.getKeyspace()).setShard(tablet.getShard()).setTabletType(tablet.getType()).build();
        String key = keyFromTarget(target);
        String tabletAlias = TopoProto.tabletAliasString(tablet.getAlias());

        this.lock.lock();
        try {
            if (this.healthByAlias.get(tabletAlias) != null) {
                log.error("BUG Program bug: tried to add existing tablet " + tabletAlias + " to healthcheck, tablet :" + TopoProto.tabletToHumanString(tablet));
                return;
            }
            TabletHealthCheck thc = new TabletHealthCheck(this, tablet, target);
            this.healthByAlias.put(tabletAlias, thc);
            TabletHealthCheck res = thc.simpleCopy();

            Map<String, TabletHealthCheck> targetsMap = healthData.get(key);
            if (targetsMap == null) {
                targetsMap = new HashMap<>();
                targetsMap.put(tabletAlias, res);
                healthData.put(key, targetsMap);
            } else {
                targetsMap.put(tabletAlias, res);
            }
            this.tabletCounterIncAndGet();
            thc.initStreamHealth();
        } finally {
            this.lock.unlock();
        }
        log.info("add tablet: " + TopoProto.tabletToHumanString(tablet));
    }

    public void updateServingState(String key, Topodata.Tablet tablet, boolean serving) {
        this.lock.lock();
        try {
            List<TabletHealthCheck> tablist = this.healthy.get(key);
            if (tablist == null) {
                return;
            }

            for (int i = 0; i < tablist.size(); i++) {
                if (TopoProto.tabletAliasString(tablist.get(i).getTablet().getAlias()).equals(TopoProto.tabletAliasString(tablet.getAlias()))) {
                    tablist.get(i).getServing().set(serving);
                }
            }
        } finally {
            this.lock.unlock();
        }
    }

    public void recomputeHealthy(String key) {
        this.lock.lock();
        try {
            this.recomputeHealthyLocked(key);
        } finally {
            this.lock.unlock();
        }
    }

    public void recomputeHealthyLocked(String key) {

        Map<String, TabletHealthCheck> targetHealthData = this.healthData.get(key);
        if (targetHealthData == null) {
            return;
        }
        List<TabletHealthCheck> list = filterStatsByReplicationLag(new CopyOnWriteArrayList<>(targetHealthData.values()));
        if (list == null || list.isEmpty()) {
            this.healthy.remove(key);
            return;
        }
        this.healthy.put(key, list);
    }

    public int tabletCounterIncAndGet() {
        return this.tabletCounter.incrementAndGet();
    }

    public int tabletCounterDecAndGet() {
        return this.tabletCounter.decrementAndGet();
    }

    public void removeTablet(Topodata.Tablet tablet) {
        String key = keyFromTablet(tablet);
        String tabletAlias = TopoProto.tabletAliasString(tablet.getAlias());

        this.lock.lock();
        TabletHealthCheck thc = null;
        try {
            thc = this.healthByAlias.get(tabletAlias);
            if (thc == null) {
                log.error("we have no health data for tablet " + TopoProto.tabletToHumanString(tablet) + ", it might have been delete already");
                return;
            }

            thc.cancelHealthCheckCtx();
            tabletCounterDecAndGet();

            this.healthByAlias.remove(tabletAlias);
            log.info("remove tablet: " + TopoProto.tabletToHumanString(tablet));
            Map<String, TabletHealthCheck> tabletHealthCheckMap = this.healthData.get(key);
            if (tabletHealthCheckMap == null) {
                log.error("we have no health data for target " + key);
                return;
            }
            tabletHealthCheckMap.remove(tabletAlias);
            this.recomputeHealthyLocked(key);
        } finally {
            this.lock.unlock();
            if (thc != null) {
                thc.shutdown();
            }
        }
    }

    public void replaceTablet(Topodata.Tablet oldTablet, Topodata.Tablet newTablet) {
        log.info("replace tablet: " + TopoProto.tabletToHumanString(oldTablet) + " --> " + TopoProto.tabletToHumanString(newTablet));
        this.removeTablet(oldTablet);
        this.addTablet(newTablet);
    }

    //
    public void updateHealth(final TabletHealthCheck th, final Query.Target preTarget,
                             final boolean trivialUpdate, boolean up) {
        String tabletAlias = TopoProto.tabletAliasString(th.getTablet().getAlias());
        String targetKey = keyFromTarget(th.getTarget());
        boolean targetChanged = preTarget.getTabletType() != th.getTarget().getTabletType()
            || !preTarget.getKeyspace().equals(th.getTarget().getKeyspace())
            || !preTarget.getShard().equals(th.getTarget().getShard());

        this.lock.lock();
        try {
            if (th.healthCheckCtxIsCanceled()) {
                return;
            }
            if (targetChanged) {
                String oldTargetKey = keyFromTarget(preTarget);
                this.healthData.get(oldTargetKey).remove(tabletAlias);
                this.healthData.computeIfAbsent(targetKey, key -> new HashMap<>());
                Topodata.Tablet tablet = th.getTablet().toBuilder().setType(th.getTarget().getTabletType()).setKeyspace(th.getTarget().getKeyspace()).setShard(th.getTarget().getShard()).build();
                th.getQueryService().setTablet(tablet);
            }
            this.healthData.get(targetKey).put(tabletAlias, th);
            boolean isPrimary = th.getTarget().getTabletType() == Topodata.TabletType.MASTER;
            if (isPrimary) {
                if (up) {
                    if (this.healthy.get(targetKey) == null || this.healthy.get(targetKey).isEmpty()) {
                        List<TabletHealthCheck> targetKeyValue = new ArrayList<>();
                        targetKeyValue.add(th);
                        this.healthy.put(targetKey, targetKeyValue);
                    } else {
                        if (th.getMasterTermStartTime() < this.healthy.get(targetKey).get(0).getMasterTermStartTime()) {
                            TabletHealthCheck tabletHealthCheck = this.healthy.get(targetKey).get(0);
                            log.info("not marking healthy master :" + tabletAlias + " as Up for :" + targetKey
                                + " because its MasterTermStartTime is smaller than the highest known timestamp from previous MASTERs "
                                + TopoProto.tabletAliasString(tabletHealthCheck.getTablet().getAlias()) + ": " + th.getMasterTermStartTime() + " < " + tabletHealthCheck.getMasterTermStartTime());
                        } else {
                            List<TabletHealthCheck> healthCheckList = new ArrayList<>();
                            healthCheckList.add(th);
                            this.healthy.put(targetKey, healthCheckList);
                        }
                    }
                } else {
                    List<TabletHealthCheck> healthCheckList = new ArrayList<>();
                    this.healthy.put(targetKey, healthCheckList);
                }
            }
            if (!trivialUpdate) {
                if (th.getTarget().getTabletType() != Topodata.TabletType.MASTER) {
                    this.recomputeHealthyLocked(targetKey);
                }
                if (targetChanged && preTarget.getTabletType() != Topodata.TabletType.MASTER) {
                    String oldTargetKey = keyFromTarget(preTarget);
                    this.recomputeHealthyLocked(oldTargetKey);
                }
            }
            boolean isNewPrimary = isPrimary && preTarget.getTabletType() != Topodata.TabletType.MASTER;
            if (isNewPrimary) {
                log.error("Adding 1 to MasterPromoted counter for target: " + keyFromTarget(preTarget) + "tablet: " + tabletAlias + " tabletType: " + preTarget.getTabletType());
            }
        } finally {
            this.lock.unlock();
            if (targetChanged) {
                th.closeNativeQueryService();
            }
        }
    }

    public void watchTabltHealthCheckStream() {
        this.streamWatcherTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                long currentTimeMillis = System.currentTimeMillis();
                healthByAlias.forEach((alias, thc) -> {
                    if (thc.healthCheckCtxIsCanceled()) {
                        thc.finalizeConn();
                        return;
                    }
                    //当前时间减去上次返回健康检查包的时间，超过了阈值，认为超时，需要重建healthCheck
                    if (thc.getLastResponseTimestamp() > 0 && currentTimeMillis - thc.getLastResponseTimestamp() > (thc.getHealthCheckTimeout() * 1000)) {
                        thc.getServing().set(false);
                        thc.getRetrying().set(true);
                        thc.getLastError().set("health check timed out latest " + thc.getLastResponseTimestamp());
                        thc.startHealthCheckStream();
                        return;
                    }

                    TabletHealthCheck.TabletStreamHealthDetailStatus tabletStreamHealthDetailStatus = thc.getTabletStreamHealthDetailStatus().get();
                    if (tabletStreamHealthDetailStatus.status == TabletHealthCheck.TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_RESPONSE) {
                        return;
                    }

                    if (tabletStreamHealthDetailStatus.status == TabletHealthCheck.TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_ERROR_PACKET) {
                        return;
                    }

                    if (tabletStreamHealthDetailStatus.status == TabletHealthCheck.TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_ERROR) {
                        thc.getServing().set(false);
                        thc.getRetrying().set(true);
                        thc.getLastError().set("health check error :" + tabletStreamHealthDetailStatus.message);
                        thc.startHealthCheckStream();
                        return;
                    }

                    if (tabletStreamHealthDetailStatus.status == TabletHealthCheck.TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_NONE) {
                        //tabletHealthCheck刚刚加入到healthByAlias,还没收到过任何健康检查消息和信号
                        return;
                    }

                    log.error("unreachable, BUG, tablet :" + TopoProto.tabletToHumanString(thc.getTablet()) + ", now ts: " + currentTimeMillis + ", last health check response: "
                        + thc.getLastResponseTimestamp());

                });
            }
        }, 5000, 15000);
    }

    private List<TabletHealthCheck> filterStatsByReplicationLag(List<TabletHealthCheck> lst) {
        return lst.stream()
            .filter(t -> t.getServing().get() && (StringUtil.isNullOrEmpty(t.getLastError().get())) && t.getStats() != null && t.getStats().getSecondsBehindMaster() < secondsBehindsMaster)
            .sorted(Comparator.comparingInt(o -> o.getStats().getSecondsBehindMaster())).collect(Collectors.toList());
    }

    public void waitForAllServingTablets(IContext ctx, List<Query.Target> targetList) throws SQLException {
        if (targetList == null) {
            return;
        }

        while (true) {
            Iterator<Query.Target> iter = targetList.iterator();
            while (iter.hasNext()) {
                Query.Target target = iter.next();
                List<TabletHealthCheck> tablets = getHealthyTabletStatsMaybeStandby(target);
                if (tablets != null) {
                    iter.remove();
                }
            }

            if (targetList.isEmpty()) {
                return;
            }

            if (ctx.isDone()) {
                log.info(ctx.error());
                StringBuilder sb = new StringBuilder("Wait for tablets: ");
                for (Query.Target target : targetList) {
                    sb.append("[cell:").append(target.getCell())
                        .append(", keyspace:").append(target.getKeyspace())
                        .append(", shard:").append(target.getShard())
                        .append("] ");
                }
                throw new SQLException(sb.append("timeout").toString());
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {

            }
        }
    }
}
