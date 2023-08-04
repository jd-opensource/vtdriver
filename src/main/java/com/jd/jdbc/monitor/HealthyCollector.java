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
import com.jd.jdbc.common.util.Crc32Utill;
import com.jd.jdbc.discovery.HealthCheck;
import com.jd.jdbc.discovery.TabletHealthCheck;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public final class HealthyCollector extends Collector {
    private static final String COLLECT_NAME = "healthy";

    private static final String COLLECT_HELP = "healthy info in HealthCheck";

    private static final HealthyCollector HEALTH_CHECK_COLLECTOR = new HealthyCollector();

    private static final List<String> LABEL_NAME_HEALTHY = Lists.newArrayList("HealthyChecksum");

    private static final String COLLECT_NAME_HEALTHY = "HealthyChecksum";

    private static final String COLLECT_HELP_HEALTHY = "crc32 checksum of the current healthCheck.healthy state ";

    private HealthyCollector() {
    }

    public static HealthyCollector getInstance() {
        return HEALTH_CHECK_COLLECTOR;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        HealthCheck healthCheck = HealthCheck.INSTANCE;
        Map<String, List<TabletHealthCheck>> healthyCopy = healthCheck.getHealthyCopy();
        if (healthyCopy.isEmpty()) {
            return null;
        }

        GaugeMetricFamily labeledGauge = new GaugeMetricFamily(COLLECT_NAME, COLLECT_HELP, DefaultConfig.HEALTH_CHECK_LABEL_NAMES);

        for (Map.Entry<String, List<TabletHealthCheck>> entry : healthyCopy.entrySet()) {
            List<TabletHealthCheck> tabletHealthCheckList = entry.getValue();
            for (TabletHealthCheck tabletHealthCheck : tabletHealthCheckList) {
                HealthCheckCollector.buildGaugeMetric(labeledGauge, tabletHealthCheck);
            }
        }

        GaugeMetricFamily labeledGaugeCheckSum = collectChecksum(healthyCopy);

        List<MetricFamilySamples> ret = new ArrayList<>();
        ret.add(labeledGauge);
        ret.add(labeledGaugeCheckSum);
        return ret;
    }

    public GaugeMetricFamily collectChecksum(Map<String, List<TabletHealthCheck>> healthy) {
        GaugeMetricFamily labeledGaugeSum = new GaugeMetricFamily(COLLECT_NAME_HEALTHY, COLLECT_HELP_HEALTHY, LABEL_NAME_HEALTHY);

        long crc32Val = stateHealthyChecksum(healthy);
        List<String> healthyLV = Lists.newArrayList(Long.toString(crc32Val));
        labeledGaugeSum.addMetric(healthyLV, crc32Val);
        return labeledGaugeSum;
    }

    public static long stateHealthyChecksum(Map<String, List<TabletHealthCheck>> healthy) {
        StringBuilder sb = new StringBuilder();

        for (List<TabletHealthCheck> tabletHealthCheckList : healthy.values()) {
            tabletHealthCheckList.sort(new Comparator<TabletHealthCheck>() {
                @Override
                public int compare(TabletHealthCheck o1, TabletHealthCheck o2) {
                    return Long.compare(o1.getTablet().getAlias().getUid(), o2.getTablet().getAlias().getUid());
                }
            });

            for (TabletHealthCheck tabletHealthCheck : tabletHealthCheckList) {
                if (!tabletHealthCheck.getServing().get()) {
                    // ignore noserving
                    continue;
                }
                sb.append(tabletHealthCheck.getTarget().getCell());
                sb.append(tabletHealthCheck.getTarget().getKeyspace());
                sb.append(tabletHealthCheck.getTarget().getShard());
                sb.append(tabletHealthCheck.getTarget().getTabletType());
                sb.append("\n");
                sb.append(tabletHealthCheck.getTablet().getAlias());
                sb.append(tabletHealthCheck.getTablet().getHostname());
                sb.append(tabletHealthCheck.getTablet().getMysqlPort());
                sb.append("\n");
                sb.append(tabletHealthCheck.getMasterTermStartTime());
                sb.append("\n");
            }
        }
        return Crc32Utill.checksumByCrc32(sb.toString().getBytes());
    }

}
