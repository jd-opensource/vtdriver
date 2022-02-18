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
import io.prometheus.client.Histogram;

public class StatementCollector {

    private static final Histogram HISTOGRAM = Histogram.build()
        .name("statement_histogram")
        .labelNames("sqlType", "Keyspace", "Role")
        .help("statement Histogram in seconds.")
        .buckets(DefaultConfig.BUCKETS)
        .register(MonitorServer.getCollectorRegistry());

    private static final Counter COUNTER = Counter.build()
        .name("statement_counter_total")
        .help("statement counter info")
        .register(MonitorServer.getCollectorRegistry());

    private static final Counter ERROR_COUNTER = Counter.build()
        .name("statement_error_counter_total")
        .labelNames("Keyspace", "Role")
        .help("statement error counter info")
        .register(MonitorServer.getCollectorRegistry());

    public static Histogram getStatementTypeHistogram() {
        return HISTOGRAM;
    }

    public static Counter getStatementCounter() {
        return COUNTER;
    }

    public static Counter getStatementErrorCounter() {
        return ERROR_COUNTER;
    }
}
