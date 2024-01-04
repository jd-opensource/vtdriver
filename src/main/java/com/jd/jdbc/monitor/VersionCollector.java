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

import io.prometheus.client.Gauge;

public class VersionCollector {
    private static final Gauge VTDRIVER_VERSION_COUNTER = Gauge.build()
        .name("VtDriver_version")
        .help("VtDriver version info")
        .labelNames("version")
        .register(MonitorServer.getCollectorRegistry());

    public static Gauge getVersionGauge() {
        return VTDRIVER_VERSION_COUNTER;
    }
}
