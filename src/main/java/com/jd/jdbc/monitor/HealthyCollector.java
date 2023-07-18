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

import com.jd.jdbc.discovery.HealthCheck;
import com.jd.jdbc.discovery.TabletHealthCheck;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class HealthyCollector extends Collector {
    private static final String COLLECT_NAME = "healthy";

    private static final String COLLECT_HELP = "healthy info in HealthCheck";

    private static final HealthyCollector HEALTH_CHECK_COLLECTOR = new HealthyCollector();

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
        int notServing = -1;

        for (Map.Entry<String, List<TabletHealthCheck>> entry : healthyCopy.entrySet()) {
            List<TabletHealthCheck> tabletHealthCheckList = entry.getValue();
            for (TabletHealthCheck tabletHealthCheck : tabletHealthCheckList) {
                HealthCheckCollector.buildGaugeMetric(labeledGauge, notServing, tabletHealthCheck);
            }
        }
        return Collections.singletonList(labeledGauge);
    }
}
