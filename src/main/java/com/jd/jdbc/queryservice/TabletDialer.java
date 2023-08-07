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

import com.jd.jdbc.util.threadpool.JdkUtil;
import com.jd.jdbc.util.threadpool.VtThreadFactoryBuilder;
import com.jd.jdbc.util.threadpool.impl.TabletNettyExecutorService;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.vitess.proto.Topodata;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class TabletDialer {

    private static final NioEventLoopGroup EVENT_EXECUTORS = new NioEventLoopGroup(JdkUtil.getQueryExecutorCorePoolSize(), VtThreadFactoryBuilder.build("NioEventLoop-"));

    /**
     * TabletQueryService Cache.
     */
    private static final Map<String, IParentQueryService> TABLET_QUERY_SERVICE_CACHE = new ConcurrentHashMap<>(128 + 1);

    public static IParentQueryService dial(final Topodata.Tablet tablet) {
        final String addr = tablet.getHostname() + ":" + tablet.getPortMapMap().get("grpc");
        if (TABLET_QUERY_SERVICE_CACHE.containsKey(addr)) {
            return TABLET_QUERY_SERVICE_CACHE.get(addr);
        }

        ManagedChannel channel = NettyChannelBuilder.forTarget(addr).usePlaintext()
            .offloadExecutor(TabletNettyExecutorService.getNettyExecutorService())
            .executor(TabletNettyExecutorService.getNettyExecutorService())
            .channelType(NioSocketChannel.class)
            .eventLoopGroup(EVENT_EXECUTORS)
            .keepAliveTimeout(10, TimeUnit.SECONDS).keepAliveTime(10, TimeUnit.SECONDS).keepAliveWithoutCalls(true).build();

        IParentQueryService combinedQueryService = new CombinedQueryService(channel, tablet);
        TABLET_QUERY_SERVICE_CACHE.putIfAbsent(addr, combinedQueryService);
        return combinedQueryService;
    }

    public static void close(final Topodata.Tablet tablet) {
        final String addr = tablet.getHostname() + ":" + tablet.getPortMapMap().get("grpc");
        TABLET_QUERY_SERVICE_CACHE.remove(addr);
    }

    protected static void registerTabletCache(final Topodata.Tablet tablet, final IParentQueryService combinedQueryService) {
        final String addr = tablet.getHostname() + ":" + tablet.getPortMapMap().get("grpc");
        TABLET_QUERY_SERVICE_CACHE.putIfAbsent(addr, combinedQueryService);
    }

    protected static void clearTabletCache() {
        TABLET_QUERY_SERVICE_CACHE.clear();
    }

    private void shutdown() {
        if (EVENT_EXECUTORS.isShuttingDown()) {
            return;
        }
        EVENT_EXECUTORS.shutdownGracefully();
    }
}
