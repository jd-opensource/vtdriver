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

public class PlanCollector {

    private static final Counter TOTAL_COUNTER = Counter.build()
        .name("plan_hit_total")
        .help("plan hit total info")
        .labelNames("sqlType")
        .register(MonitorServer.getCollectorRegistry());

    private static final Counter CACHE_COUNTER = Counter.build()
        .name("plan_hit_cache")
        .help("plan hit cache info")
        .labelNames("sqlType")
        .register(MonitorServer.getCollectorRegistry());

    public static Counter getTotalCounter() {
        return TOTAL_COUNTER;
    }

    public static Counter getCacheCounter() {
        return CACHE_COUNTER;
    }
}
