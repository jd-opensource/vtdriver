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

import com.google.common.collect.Lists;
import com.jd.jdbc.queryservice.CombinedQueryService;
import com.jd.jdbc.queryservice.IParentQueryService;
import com.jd.jdbc.queryservice.MockQueryServer;
import com.jd.jdbc.queryservice.RoleType;
import com.jd.jdbc.queryservice.TabletDialerAgent;
import com.jd.jdbc.queryservice.util.RoleUtils;
import com.jd.jdbc.util.threadpool.impl.VtHealthCheckExecutorService;
import com.jd.jdbc.util.threadpool.impl.VtQueryExecutorService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import testsuite.TestSuite;

public class ReadWriteSplitTest extends TestSuite {
    private static final Map<String, Integer> PORT_MAP = new HashMap<>();

    private static RoleType rrRoleType;

    private static RoleType rwRoleType;

    private static RoleType roRoleType;

    private static RoleType rrmRoleType;

    @Rule
    public GrpcCleanupRule grpcCleanup;

    static {
        PORT_MAP.put("vt", 1);
        PORT_MAP.put("grpc", 2);
        try {
            rrRoleType = RoleUtils.buildRoleType("rr");
            rwRoleType = RoleUtils.buildRoleType("rw");
            roRoleType = RoleUtils.buildRoleType("ro");
            rrmRoleType = RoleUtils.buildRoleType("rrm");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    private final String cell1 = "cell1";

    private final String cell2 = "cell2";

    private final String keyspace = "vtdriver2";

    private final String shard = "-80";

    @BeforeClass
    public static void initPool() {
        VtHealthCheckExecutorService.initialize(null, null, null, null);
        VtQueryExecutorService.initialize(null, null, null, null);
    }

    @Before
    public void init() throws IOException {
        grpcCleanup = new GrpcCleanupRule();
    }

    @After
    public void resetHealthCheck() {
        HealthCheck.resetHealthCheck();
    }

    @Test
    public void testServing() {
        // init HealthCheck
        HealthCheck hc = HealthCheck.INSTANCE;
        MockTablet mockTablet1 = buildMockTablet(cell1, RandomUtils.nextInt(), "a", keyspace, shard, PORT_MAP, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet1.getTablet());
        MockTablet mockTablet2 = buildMockTablet(cell2, RandomUtils.nextInt(), "b", keyspace, shard, PORT_MAP, Topodata.TabletType.RDONLY);
        hc.addTablet(mockTablet2.getTablet());
        MockTablet mockTablet3 = buildMockTablet(cell1, RandomUtils.nextInt(), "c", keyspace, shard, PORT_MAP, Topodata.TabletType.MASTER);
        hc.addTablet(mockTablet3.getTablet());
        MockTablet mockTablet4 = buildMockTablet(cell2, RandomUtils.nextInt(), "d", keyspace, shard, PORT_MAP, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet4.getTablet());

        sleepMillisSeconds(200);
        sendOnNextMessage(mockTablet1, mockTablet2, mockTablet3, mockTablet4);

        // check HealthCheck.getTabletHealthChecks
        List<TabletHealthCheck> tabletHealthChecks = hc.getTabletHealthChecks(createTarget(Topodata.TabletType.REPLICA), rrRoleType);
        Assert.assertNotNull(tabletHealthChecks);
        Assert.assertEquals(2, tabletHealthChecks.size());
        List<Topodata.Tablet> tablets = new ArrayList<>();
        for (TabletHealthCheck tabletHealthCheck : tabletHealthChecks) {
            tablets.add(tabletHealthCheck.getTablet());
        }
        Assert.assertTrue(CollectionUtils.isEqualCollection(Lists.newArrayList(mockTablet1.getTablet(), mockTablet4.getTablet()), tablets));

        tabletHealthChecks = hc.getTabletHealthChecks(createTarget(Topodata.TabletType.REPLICA), rrmRoleType);
        Assert.assertNotNull(tabletHealthChecks);
        Assert.assertEquals(2, tabletHealthChecks.size());
        tablets = new ArrayList<>();
        for (TabletHealthCheck tabletHealthCheck : tabletHealthChecks) {
            tablets.add(tabletHealthCheck.getTablet());
        }
        Assert.assertTrue(CollectionUtils.isEqualCollection(Lists.newArrayList(mockTablet1.getTablet(), mockTablet4.getTablet()), tablets));

        tabletHealthChecks = hc.getTabletHealthChecks(createTarget(Topodata.TabletType.RDONLY), roRoleType);
        Assert.assertNotNull(tabletHealthChecks);
        Assert.assertEquals(1, tabletHealthChecks.size());
        Assert.assertEquals(mockTablet2.getTablet(), tabletHealthChecks.get(0).getTablet());

        tabletHealthChecks = hc.getTabletHealthChecks(createTarget(Topodata.TabletType.MASTER), rwRoleType);
        Assert.assertNotNull(tabletHealthChecks);
        Assert.assertEquals(1, tabletHealthChecks.size());
        Assert.assertEquals(mockTablet3.getTablet(), tabletHealthChecks.get(0).getTablet());

        // close resource
        closeQueryService(mockTablet1, mockTablet2, mockTablet3, mockTablet4);
    }

    @Test
    public void testNoServing() {
        // init HealthCheck
        HealthCheck hc = HealthCheck.INSTANCE;
        MockTablet mockTablet1 = buildMockTablet(cell2, RandomUtils.nextInt(), "e", keyspace, shard, PORT_MAP, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet1.getTablet());
        MockTablet mockTablet2 = buildMockTablet(cell2, RandomUtils.nextInt(), "f", keyspace, shard, PORT_MAP, Topodata.TabletType.RDONLY);
        hc.addTablet(mockTablet2.getTablet());
        MockTablet mockTablet3 = buildMockTablet(cell1, RandomUtils.nextInt(), "g", keyspace, shard, PORT_MAP, Topodata.TabletType.MASTER);
        hc.addTablet(mockTablet3.getTablet());
        MockTablet mockTablet4 = buildMockTablet(cell2, RandomUtils.nextInt(), "h", keyspace, shard, PORT_MAP, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet4.getTablet());

        sleepMillisSeconds(200);
        sendOnNextMessage(mockTablet3);

        // check HealthCheck.getTabletHealthChecks
        List<TabletHealthCheck> tabletHealthChecks = hc.getTabletHealthChecks(createTarget(Topodata.TabletType.REPLICA), rrRoleType);
        Assert.assertTrue(CollectionUtils.isEmpty(tabletHealthChecks));

        tabletHealthChecks = hc.getTabletHealthChecks(createTarget(Topodata.TabletType.RDONLY), roRoleType);
        Assert.assertTrue(CollectionUtils.isEmpty(tabletHealthChecks));

        tabletHealthChecks = hc.getTabletHealthChecks(createTarget(Topodata.TabletType.MASTER), rwRoleType);
        Assert.assertNotNull(tabletHealthChecks);
        Assert.assertEquals(1, tabletHealthChecks.size());
        Assert.assertEquals(mockTablet3.getTablet(), tabletHealthChecks.get(0).getTablet());

        tabletHealthChecks = hc.getTabletHealthChecks(createTarget(Topodata.TabletType.REPLICA), rrmRoleType);
        Assert.assertNotNull(tabletHealthChecks);
        Assert.assertEquals(1, tabletHealthChecks.size());
        Assert.assertEquals(mockTablet3.getTablet(), tabletHealthChecks.get(0).getTablet());

        // close resource
        closeQueryService(mockTablet1, mockTablet2, mockTablet3, mockTablet4);
    }

    @Test
    public void testNoServing2() {
        // init HealthCheck
        HealthCheck hc = HealthCheck.INSTANCE;
        MockTablet mockTablet1 = buildMockTablet(cell2, RandomUtils.nextInt(), "i", keyspace, shard, PORT_MAP, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet1.getTablet());
        MockTablet mockTablet2 = buildMockTablet(cell1, RandomUtils.nextInt(), "j", keyspace, shard, PORT_MAP, Topodata.TabletType.RDONLY);
        hc.addTablet(mockTablet2.getTablet());
        MockTablet mockTablet3 = buildMockTablet(cell2, RandomUtils.nextInt(), "k", keyspace, shard, PORT_MAP, Topodata.TabletType.MASTER);
        hc.addTablet(mockTablet3.getTablet());
        MockTablet mockTablet4 = buildMockTablet(cell2, RandomUtils.nextInt(), "l", keyspace, shard, PORT_MAP, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet4.getTablet());

        sleepMillisSeconds(200);
        sendOnNextMessage(mockTablet3, mockTablet2);

        // check HealthCheck.getTabletHealthChecks
        List<TabletHealthCheck> tabletHealthChecks = hc.getTabletHealthChecks(createTarget(Topodata.TabletType.REPLICA), rrRoleType);
        Assert.assertNotNull(tabletHealthChecks);
        Assert.assertEquals(1, tabletHealthChecks.size());
        Assert.assertEquals(mockTablet2.getTablet(), tabletHealthChecks.get(0).getTablet());

        tabletHealthChecks = hc.getTabletHealthChecks(createTarget(Topodata.TabletType.RDONLY), roRoleType);
        Assert.assertNotNull(tabletHealthChecks);
        Assert.assertEquals(1, tabletHealthChecks.size());
        Assert.assertEquals(mockTablet2.getTablet(), tabletHealthChecks.get(0).getTablet());

        tabletHealthChecks = hc.getTabletHealthChecks(createTarget(Topodata.TabletType.REPLICA), rrmRoleType);
        Assert.assertNotNull(tabletHealthChecks);
        Assert.assertEquals(1, tabletHealthChecks.size());
        Assert.assertEquals(mockTablet2.getTablet(), tabletHealthChecks.get(0).getTablet());

        tabletHealthChecks = hc.getTabletHealthChecks(createTarget(Topodata.TabletType.MASTER), rwRoleType);
        Assert.assertNotNull(tabletHealthChecks);
        Assert.assertEquals(1, tabletHealthChecks.size());
        Assert.assertEquals(mockTablet3.getTablet(), tabletHealthChecks.get(0).getTablet());

        // close resource
        closeQueryService(mockTablet1, mockTablet2, mockTablet3, mockTablet4);
    }

    private void sendOnNextMessage(MockTablet... mockTablets) {
        for (MockTablet mockTablet : mockTablets) {
            Query.Target target = createTarget(mockTablet.getTablet().getType());
            Query.StreamHealthResponse streamHealthResponse = Query.StreamHealthResponse.newBuilder()
                .setTabletAlias(mockTablet.getTablet().getAlias())
                .setTarget(target)
                .setServing(true)
                .setTabletExternallyReparentedTimestamp(0)
                .setRealtimeStats(Query.RealtimeStats.newBuilder().setCpuUsage(0.5).setSecondsBehindMaster(0).build())
                .build();
            MockQueryServer.HealthCheckMessage message = new MockQueryServer.HealthCheckMessage(MockQueryServer.MessageType.Next, streamHealthResponse);
            try {
                mockTablet.getHealthMessage().put(message);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        sleepMillisSeconds(200);
    }

    protected Query.Target createTarget(Topodata.TabletType type) {
        return Query.Target.newBuilder().setKeyspace(keyspace).setShard(shard).setTabletType(type).build();
    }

    protected MockTablet buildMockTablet(String cell, Integer uid, String hostName, String keyspaceName, String shard, Map<String, Integer> portMap, Topodata.TabletType type) {
        BlockingQueue<MockQueryServer.HealthCheckMessage> healthMessage = new ArrayBlockingQueue<>(10);
        MockQueryServer queryServer = new MockQueryServer(healthMessage);
        Server server;
        try {
            server = InProcessServerBuilder.forName(hostName).directExecutor().addService(queryServer).build().start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        grpcCleanup.register(server);
        ManagedChannel channel = InProcessChannelBuilder.forName(hostName).directExecutor().keepAliveTimeout(10, TimeUnit.SECONDS)
            .keepAliveTime(10, TimeUnit.SECONDS).keepAliveWithoutCalls(true).build();
        grpcCleanup.register(channel);
        Topodata.Tablet tablet = buildTablet(cell, uid, hostName, keyspaceName, shard, portMap, type);
        IParentQueryService combinedQueryService = new CombinedQueryService(channel, tablet);
        TabletDialerAgent.registerTabletCache(tablet, combinedQueryService);
        return new MockTablet(tablet, healthMessage);
    }

    private Topodata.Tablet buildTablet(String cell, Integer uid, String hostName, String keyspaceName, String shard, Map<String, Integer> portMap, Topodata.TabletType type) {
        Topodata.TabletAlias tabletAlias = Topodata.TabletAlias.newBuilder().setCell(cell).setUid(uid).build();
        int defaultMysqlPort = 3358;
        Topodata.Tablet.Builder tabletBuilder = Topodata.Tablet.newBuilder().setHostname(hostName).setAlias(tabletAlias).setKeyspace(keyspaceName)
            .setShard(shard).setType(type).setMysqlHostname(hostName).setMysqlPort(defaultMysqlPort);
        for (Map.Entry<String, Integer> portEntry : portMap.entrySet()) {
            tabletBuilder.putPortMap(portEntry.getKey(), portEntry.getValue());
        }
        System.out.printf("buildTablet:  keyspace:%s, shard:%s, tablet_type:%s, cell:%s, uid:%s\n", keyspaceName, shard, type, cell, uid);
        return tabletBuilder.build();
    }

    private void closeQueryService(MockTablet... tablets) {
        MockQueryServer.HealthCheckMessage close = new MockQueryServer.HealthCheckMessage(MockQueryServer.MessageType.Close, null);
        for (MockTablet tablet : tablets) {
            try {
                tablet.getHealthMessage().put(close);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @AllArgsConstructor
    @Getter
    private static class MockTablet {

        private final Topodata.Tablet tablet;

        private final BlockingQueue<MockQueryServer.HealthCheckMessage> healthMessage;
    }
}
