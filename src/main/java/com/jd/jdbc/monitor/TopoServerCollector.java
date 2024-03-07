/*
Copyright 2024 JD Project Authors.

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

public class TopoServerCollector {

    private static final Counter EXEC_COUNTER = Counter.build()
        .name("TopoServer_get_all_cell_counter")
        .labelNames("Address")
        .help("TopoServer get all cells counter")
        .register(MonitorServer.getCollectorRegistry());

    private static final Counter NEW_CELLS_COUNTER = Counter.build()
        .name("TopoServer_new_cell_counter")
        .labelNames("Address")
        .help("TopoServer add new cell counter")
        .register(MonitorServer.getCollectorRegistry());

    public static Counter getExecCounterCounter() {
        return EXEC_COUNTER;
    }

    public static Counter geCellsCounter() {
        return NEW_CELLS_COUNTER;
    }
}
