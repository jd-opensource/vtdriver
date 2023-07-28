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

package com.jd.jdbc.discovery;

import com.jd.jdbc.queryservice.CombinedQueryService;
import com.jd.jdbc.queryservice.IParentQueryService;
import com.jd.jdbc.queryservice.MockQueryServer;
import com.jd.jdbc.queryservice.TabletDialerAgent;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.vitess.proto.Topodata;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.Getter;

@Getter
public class MockTablet {

    private final Topodata.Tablet tablet;

    private final BlockingQueue<MockQueryServer.HealthCheckMessage> healthMessage;

    private final MockQueryServer queryServer;

    private final Server server;

    private final ManagedChannel channel;

    private MockTablet(Topodata.Tablet tablet, BlockingQueue<MockQueryServer.HealthCheckMessage> healthMessage, MockQueryServer queryServer, Server server, ManagedChannel channel) {
        this.tablet = tablet;
        this.healthMessage = healthMessage;
        this.queryServer = queryServer;
        this.server = server;
        this.channel = channel;
    }

    public static MockTablet buildMockTablet(Topodata.Tablet tablet, GrpcCleanupRule grpcCleanup) {
        String serverName = InProcessServerBuilder.generateName();
        BlockingQueue<MockQueryServer.HealthCheckMessage> healthMessage = new ArrayBlockingQueue<>(2);
        MockQueryServer queryServer = new MockQueryServer(healthMessage);
        Server server = null;
        try {
            server = InProcessServerBuilder.forName(serverName).directExecutor().addService(queryServer).build().start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        grpcCleanup.register(server);

        ManagedChannel channel =
            InProcessChannelBuilder.forName(serverName).directExecutor().keepAliveTimeout(10, TimeUnit.SECONDS).keepAliveTime(10, TimeUnit.SECONDS).keepAliveWithoutCalls(true).build();
        grpcCleanup.register(channel);

        IParentQueryService combinedQueryService = new CombinedQueryService(channel, tablet);
        TabletDialerAgent.registerTabletCache(tablet, combinedQueryService);

        return new MockTablet(tablet, healthMessage, queryServer, server, channel);
    }

    public static MockTablet buildMockTablet(GrpcCleanupRule grpcCleanup, String cell, Integer uid, String hostName, String keyspaceName, String shard, Map<String, Integer> portMap,
                                             Topodata.TabletType type) {
        return buildMockTablet(grpcCleanup, cell, uid, hostName, keyspaceName, shard, portMap, type, 3358);
    }

    public static MockTablet buildMockTablet(GrpcCleanupRule grpcCleanup, String cell, Integer uid, String hostName, String keyspaceName, String shard, Map<String, Integer> portMap,
                                             Topodata.TabletType type, int defaultMysqlPort) {
        Topodata.Tablet tablet = buildTablet(cell, uid, hostName, keyspaceName, shard, portMap, type, defaultMysqlPort);
        return buildMockTablet(tablet, grpcCleanup);
    }

    private static Topodata.Tablet buildTablet(String cell, Integer uid, String hostName, String keyspaceName, String shard, Map<String, Integer> portMap, Topodata.TabletType type,
                                               int defaultMysqlPort) {
        Topodata.TabletAlias tabletAlias = Topodata.TabletAlias.newBuilder().setCell(cell).setUid(uid).build();
        Topodata.Tablet.Builder tabletBuilder = Topodata.Tablet.newBuilder()
            .setHostname(hostName).setAlias(tabletAlias).setKeyspace(keyspaceName).setShard(shard).setType(type).setMysqlHostname(hostName).setMysqlPort(defaultMysqlPort);
        for (Map.Entry<String, Integer> portEntry : portMap.entrySet()) {
            tabletBuilder.putPortMap(portEntry.getKey(), portEntry.getValue());
        }
        return tabletBuilder.build();
    }

    public static void closeQueryService(MockTablet... tablets) {
        MockQueryServer.HealthCheckMessage close = new MockQueryServer.HealthCheckMessage(MockQueryServer.MessageType.Close, null);
        for (MockTablet tablet : tablets) {
            try {
                tablet.getHealthMessage().put(close);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            TabletDialerAgent.close(tablet.getTablet());
        }
    }

    public void close() {
        healthMessage.clear();
        server.shutdownNow();
        channel.shutdownNow();
    }
}
