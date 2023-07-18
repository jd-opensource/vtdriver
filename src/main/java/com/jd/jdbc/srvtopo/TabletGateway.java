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

import com.google.common.collect.Lists;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.context.VtContext;
import com.jd.jdbc.discovery.HealthCheck;
import com.jd.jdbc.queryservice.IQueryService;
import com.jd.jdbc.queryservice.RetryTabletQueryService;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Data;

/**
 * TabletGateway implements the Gateway interface.
 * This implementation uses the new healthcheck module.
 */
@Data
public class TabletGateway extends Gateway {
    private static final Log log = LogFactory.getLog(TabletGateway.class);

    private static Map<SrvTopoServer, TabletGateway> tabletGateways = new HashMap<>();

    private HealthCheck hc;

    private SrvTopoServer srvTopoServer;

    private Integer retryCount;

    private Set<String> tabletGatewayManager;

    /**
     * mu protects the fields of this group.
     */
    private ReentrantLock lock;

    private TabletGateway(SrvTopoServer srvTopoServer, Integer retryCount) {
        this.hc = HealthCheck.INSTANCE;
        this.srvTopoServer = srvTopoServer;
        this.retryCount = retryCount;
        this.tabletGatewayManager = new HashSet<>(16, 1);
        this.lock = new ReentrantLock();
    }

    public static TabletGateway build(SrvTopoServer serv) throws Exception {
        synchronized (TabletGateway.class) {
            if (tabletGateways.containsKey(serv)) {
                return tabletGateways.get(serv);
            }

            TabletGateway gw = new TabletGateway(serv, 2);
            gw.setQueryService(new RetryTabletQueryService(gw));
            tabletGateways.put(serv, gw);

            return gw;
        }
    }

    @Override
    public void waitForTablets(IContext ctx, String cell, String keyspace, List<Topodata.TabletType> tabletTypeList) throws SQLException {
        Map<String, String> noWatchedMap = new HashMap<>();
        for (Topodata.TabletType tabletType : tabletTypeList) {
            String cacheKey = cell + "." + keyspace + "." + tabletType;
            if (!this.tabletGatewayManager.contains(cacheKey)) {
                noWatchedMap.put(cacheKey, keyspace);
            }
        }
        if (noWatchedMap.isEmpty()) {
            return;
        }
        List<Query.Target> targetList = SrvTopo.findAllTargets(ctx, srvTopoServer, cell, keyspace, tabletTypeList);
        hc.waitForAllServingTablets(VtContext.withDeadline(ctx, 30, TimeUnit.SECONDS), Lists.newArrayList(targetList));
        noWatchedMap.forEach((cacheKey, v) -> this.tabletGatewayManager.add(cacheKey));
    }

    @Override
    public IQueryService queryServiceByAlias(Topodata.TabletAlias alias) {
        return hc.tabletConnection(alias);
    }
}
