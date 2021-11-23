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

import io.prometheus.client.Counter;

public class TopologyCollector {

    private static final Counter COUNTER = Counter.build()
        .name("Topology_counter")
        .labelNames("Cell")
        .help("watch SrvKeyspace counter info")
        .register(MonitorServer.getCollectorRegistry());

    private static final Counter ERROR_COUNTER = Counter.build()
        .name("Topology_error_counter")
        .labelNames("Cell")
        .help("watch SrvKeyspace error counter info")
        .register(MonitorServer.getCollectorRegistry());

    public static Counter getCounter() {
        return COUNTER;
    }

    public static Counter getErrorCounter() {
        return ERROR_COUNTER;
    }
}
