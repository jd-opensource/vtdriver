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
import com.jd.jdbc.srvtopo.ResilientServer;
import io.prometheus.client.Collector;
import io.prometheus.client.Counter;
import io.prometheus.client.GaugeMetricFamily;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class SrvKeyspaceCollector extends Collector {
    private static final List<String> LABEL_NAMES = Lists.newArrayList("Cell", "Keyspace", "MD5");

    private static final String COLLECT_NAME = "SrvKeyspaceCollector";

    private static final String COLLECT_HELP = "SrvKeyspaceCollector info.";

    private static final Counter COUNTER = Counter.build()
        .name("watch_SrvKeyspace_counter")
        .labelNames("Keyspace", "Cell")
        .help("watch SrvKeyspace counter info")
        .register(MonitorServer.getCollectorRegistry());

    private static final Counter ERROR_COUNTER = Counter.build()
        .name("watch_SrvKeyspace_error_counter")
        .labelNames("Keyspace", "Cell")
        .help("watch SrvKeyspace error counter info")
        .register(MonitorServer.getCollectorRegistry());

    private static final Counter SRV_KEYSPACE_TASK_COUNTER = Counter.build()
        .name("SrvKeyspaceTask_counter")
        .labelNames("Keyspace", "Cell")
        .help("SrvKeyspaceTask counter info")
        .register(MonitorServer.getCollectorRegistry());

    private static final Counter SRV_KEYSPACE_TASK_ERROR_COUNTER = Counter.build()
        .name("SrvKeyspaceTask_error_counter")
        .labelNames("Keyspace", "Cell")
        .help("SrvKeyspaceTaskerror counter info")
        .register(MonitorServer.getCollectorRegistry());

    private static final SrvKeyspaceCollector srvKeyspaceCollector = new SrvKeyspaceCollector();

    private final List<ResilientServer> resilientServerList = new ArrayList<>();

    private SrvKeyspaceCollector() {
    }

    public static SrvKeyspaceCollector getInstance() {
        return srvKeyspaceCollector;
    }

    public static Counter getCounter() {
        return COUNTER;
    }

    public static Counter getErrorCounter() {
        return ERROR_COUNTER;
    }

    public static Counter getSrvKeyspaceTaskCounter() {
        return SRV_KEYSPACE_TASK_COUNTER;
    }

    public static Counter getSrvKeyspaceTaskErrorCounter() {
        return SRV_KEYSPACE_TASK_ERROR_COUNTER;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        GaugeMetricFamily labeledGauge = new GaugeMetricFamily(COLLECT_NAME, COLLECT_HELP, LABEL_NAMES);
        for (ResilientServer resilientServer : resilientServerList) {
            List<Info> infoList = resilientServer.getSrvKeyspaceCollectorInfo();
            for (Info info : infoList) {
                List<String> labelValues = Lists.newArrayList(info.cell, info.keyspace, info.md5);
                labeledGauge.addMetric(labelValues, Math.abs(info.md5.hashCode()));
            }
        }
        return Collections.singletonList(labeledGauge);
    }

    public void add(final ResilientServer resilientServer) {
        resilientServerList.add(resilientServer);
    }

    public static class Info {
        private final String md5;

        private final String cell;

        private final String keyspace;

        public Info(final String md5, final String cell, final String keyspace) {
            this.md5 = md5;
            this.cell = cell;
            this.keyspace = keyspace;
        }
    }
}
