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

package com.jd.jdbc.queryservice;

import io.vitess.proto.Topodata;

public class TabletDialerAgent {
    public static void registerTabletCache(final Topodata.Tablet tablet, final IParentQueryService combinedQueryService) {
        TabletDialer.registerTabletCache(tablet, combinedQueryService);
    }

    public static void clearTabletCache() {
        TabletDialer.clearTabletCache();
    }

    public static void close(final Topodata.Tablet tablet) {
        TabletDialer.close(tablet);
    }
}
