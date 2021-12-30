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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.vitess.proto.Topodata;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class TabletDialer {

    /**
     * TabletQueryService Cache.
     */
    private static final Map<String, IParentQueryService> TABLETQUERYSERVICECACHE = new ConcurrentHashMap<>(128 + 1);

    public static IParentQueryService dial(final Topodata.Tablet tablet) {
        final String addr = tablet.getHostname() + ":" + tablet.getPortMapMap().get("grpc");
        if (TABLETQUERYSERVICECACHE.containsKey(addr)) {
            return TABLETQUERYSERVICECACHE.get(addr);
        }

        ManagedChannel channel = ManagedChannelBuilder.forTarget(addr).usePlaintext().keepAliveTimeout(10, TimeUnit.SECONDS).keepAliveTime(10, TimeUnit.SECONDS).keepAliveWithoutCalls(true).build();

        IParentQueryService combinedQueryService = new CombinedQueryService(channel, tablet);
        TABLETQUERYSERVICECACHE.putIfAbsent(addr, combinedQueryService);
        return combinedQueryService;
    }

    public static void close(final Topodata.Tablet tablet) {
        final String addr = tablet.getHostname() + ":" + tablet.getPortMapMap().get("grpc");
        TABLETQUERYSERVICECACHE.remove(addr);
    }
}
