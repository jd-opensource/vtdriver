/*
Copyright 2021 JD Project Authors.

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

package com.jd.jdbc.monitor;

import com.google.common.collect.Lists;
import com.jd.jdbc.discovery.HealthCheck;
import com.jd.jdbc.discovery.TabletHealthCheck;
import com.jd.jdbc.topo.topoproto.TopoProto;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class HealthCheckCollector extends Collector {
    private static final List<String> LABEL_NAMES = Lists.newArrayList("Cell", "Keyspace", "Shard", "TabletType", "Serving", "TabletAlias", "Uid", "IP");

    private static final String COLLECT_NAME = "health_check";

    private static final String COLLECT_HELP = "health check info.";

    private static final HealthCheckCollector healthCheckCollector = new HealthCheckCollector();

    private HealthCheckCollector() {
    }

    public static HealthCheckCollector getInstance() {
        return healthCheckCollector;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        HealthCheck healthCheck = HealthCheck.INSTANCE;
        Map<String, TabletHealthCheck> healthByAlias = healthCheck.getHealthByAliasCopy();
        if (healthByAlias.isEmpty()) {
            return null;
        }

        GaugeMetricFamily labeledGauge = new GaugeMetricFamily(COLLECT_NAME, COLLECT_HELP, LABEL_NAMES);
        int notServing = -1;
        for (Map.Entry<String, TabletHealthCheck> entry : healthByAlias.entrySet()) {
            TabletHealthCheck tabletHealthCheck = entry.getValue();
            Topodata.Tablet tablet = tabletHealthCheck.getTablet();
            Query.Target target = tabletHealthCheck.getTarget();
            List<String> labelValues = Lists.newArrayList(tablet.getAlias().getCell(),
                target.getKeyspace(),
                target.getShard(),
                TopoProto.tabletTypeLstring(target.getTabletType()),
                String.valueOf(tabletHealthCheck.getServing()),
                TopoProto.tabletAliasString(tablet.getAlias()),
                TopoProto.getPoolName(tablet),
                tablet.getMysqlHostname());

            labeledGauge.addMetric(labelValues, tabletHealthCheck.getServing().get() ? 1 : notServing--);
        }
        return Collections.singletonList(labeledGauge);
    }
}
