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
import com.jd.jdbc.common.util.CollectionUtils;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.context.VtContext;
import com.jd.jdbc.discovery.TabletHealthCheck.TabletStreamHealthStatus;
import com.jd.jdbc.monitor.HealthyCollector;
import com.jd.jdbc.queryservice.MockQueryServer;
import com.jd.jdbc.queryservice.TabletDialerAgent;
import com.jd.jdbc.topo.MemoryTopoFactory;
import com.jd.jdbc.topo.TopoException;
import com.jd.jdbc.topo.TopoServer;
import com.jd.jdbc.topo.topoproto.TopoProto;
import com.jd.jdbc.util.threadpool.impl.VtHealthCheckExecutorService;
import com.jd.jdbc.util.threadpool.impl.VtQueryExecutorService;
import io.grpc.testing.GrpcCleanupRule;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.io.IOException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AllArgsConstructor;
import lombok.ToString;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import testsuite.TestSuite;

public class HealthCheckTest extends TestSuite {

    @Rule
    public GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private final IContext globalContext = VtContext.withCancel(VtContext.background());

    private static final ExecutorService executorService = getThreadPool(10, 10);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private final Map<String, Integer> portMap = new HashMap<>();

    private int defaultMysqlPort = 3358;

    @BeforeClass
    public static void initPool() {
        HealthCheck.resetHealthCheck();
        TabletDialerAgent.clearTabletCache();
        TopologyWatcherManager.INSTANCE.close();
        VtHealthCheckExecutorService.initialize(null, null, null, null);
        VtQueryExecutorService.initialize(null, null, null, null);
    }

    @Before
    public void init() throws IOException {
        portMap.put("vt", 1);
        portMap.put("grpc", 2);
    }

    @AfterClass
    public static void afterClass() {
        executorService.shutdownNow();
    }

    @After
    public void resetHealthCheck() {
        HealthCheck.resetHealthCheck();
        TabletDialerAgent.clearTabletCache();
        TopologyWatcherManager.INSTANCE.close();
    }

    /**
     * 1. testing if addTablet function can work well
     * 2. testing if changing tablet status can be watched right;
     * 3. testing if removeTable function can work well
     */
    @Test
    public void testHealthCheck() {

        printComment("1. HealthCheck Test");
        printComment("a. Get Health");
        HealthCheck hc = getHealthCheck();

        printComment("b. Add a no-serving Tablet");
        MockTablet mockTablet = MockTablet.buildMockTablet(grpcCleanup, "cell", 0, "a", "k", "s", portMap, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet.getTablet());

        Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 0, hc.getHealthyCopy().size());
        sleepMillisSeconds(200);

        printComment("c. Modify the status of Tablet to serving");
        sendOnNextMessage(mockTablet, Topodata.TabletType.REPLICA, true, 0, 0.5, 1);

        sleepMillisSeconds(200);

        Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 1, hc.getHealthyCopy().size());

        hc.removeTablet(mockTablet.getTablet());

        sleepMillisSeconds(2000);

        Assert.assertEquals("Wrong Tablet data", 0, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 0, hc.getHealthyCopy().size());

        MockTablet.closeQueryService(mockTablet);
        printOk();
    }

    /**
     * 1. testing if addTablet function can work well
     * 2. testing if changing tablet status can be watched right;
     * 3. testing if tablet can be remove from healthy when receive error message from tablet server
     */
    @Test
    public void testHealthCheckStreamError() {
        printComment("2. HealthCheck Test for Error Stream Message");
        printComment("a. Get Health");
        HealthCheck hc = getHealthCheck();

        printComment("b. Add a no-serving Tablet");

        MockTablet mockTablet = MockTablet.buildMockTablet(grpcCleanup, "cell", 0, "a", "k", "s", portMap, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet.getTablet());

        Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 0, hc.getHealthyCopy().size());
        sleepMillisSeconds(200);

        printComment("c. Modify the status of Tablet to serving");
        sendOnNextMessage(mockTablet, Topodata.TabletType.REPLICA, true, 0, 0.5, 1);

        sleepMillisSeconds(200);

        Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 1, hc.getHealthyCopy().size());

        sendOnErrorMessage(mockTablet);

        sleepMillisSeconds(200);
        Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
        List<TabletHealthCheck> hcList = hc.getHealthyTabletStats(createTarget(Topodata.TabletType.REPLICA));
        if (hcList != null) {
            Assert.assertEquals(0, hcList.size());
        }

        MockTablet.closeQueryService(mockTablet);
        printOk();
    }

    /**
     * 1. testing if addTablet function can work well
     * 2. testing if changing tablet status can be watched right;
     * 3. testing if changing the type of tablet to primary;
     */
    @Test
    public void testHealthCheckExternalReparent() {
        printComment("3. HealthCheck Test one tablet External Reparent");
        printComment("a. Get Health");
        HealthCheck hc = getHealthCheck();

        printComment("b. Add a no-serving Tablet");

        MockTablet mockTablet = MockTablet.buildMockTablet(grpcCleanup, "cell", 0, "a", "k", "s", portMap, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet.getTablet());

        sleepMillisSeconds(200);
        Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 0, hc.getHealthyCopy().size());

        printComment("c. Modify the status of Tablet to serving");
        sendOnNextMessage(mockTablet, Topodata.TabletType.REPLICA, true, 0, 0.5, 1);

        sleepMillisSeconds(200);

        Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 1, hc.getHealthyCopy().size());

        printComment("d. Change the Tablet role to primary");
        sendOnNextMessage(mockTablet, Topodata.TabletType.MASTER, true, 0, 0.2, 0);

        sleepMillisSeconds(200);

        Topodata.Tablet tablet = hc.getHealthyTablets("k", Topodata.TabletType.MASTER);
        Assert.assertNotNull(tablet);

        List<TabletHealthCheck> healthCheckList = hc.getHealthyTabletStats(createTarget(Topodata.TabletType.MASTER));

        Assert.assertEquals(1, healthCheckList.size());
        assertTabletHealthCheck(healthCheckList.get(0),
            mockTablet.getTablet(),
            createTarget(Topodata.TabletType.MASTER),
            new AtomicBoolean(true),
            new AtomicBoolean(false),
            Query.RealtimeStats.newBuilder().setCpuUsage(0.2).setSecondsBehindMaster(0).build());

        MockTablet.closeQueryService(mockTablet);

        printOk();
    }

    @Test
    public void testHealthCheckTwoExternalReparent() {
        printComment("4. HealthCheck Test two tablets External Reparent");
        printComment("a. Get Health");
        HealthCheck hc = getHealthCheck();

        printComment("b. Add two Tablets");
        MockTablet mockTablet1 = MockTablet.buildMockTablet(grpcCleanup, "cell", 0, "a", "k", "s", portMap, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet1.getTablet());

        MockTablet mockTablet2 = MockTablet.buildMockTablet(grpcCleanup, "cell", 1, "b", "k", "s", portMap, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet2.getTablet());

        Assert.assertEquals("Wrong Tablet data", 2, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 0, hc.getHealthyCopy().size());
        sleepMillisSeconds(200);

        printComment("c. Change two Tablets status");
        sendOnNextMessage(mockTablet2, Topodata.TabletType.REPLICA, true, 0, 0.5, 10);
        sendOnNextMessage(mockTablet1, Topodata.TabletType.MASTER, true, 10, 0.5, 0);

        sleepMillisSeconds(200);
        Assert.assertEquals("Wrong Tablet data", 2, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 2, hc.getHealthyCopy().size());
        List<TabletHealthCheck> healthCheckList = hc.getHealthyTabletStats(Query.Target.newBuilder().setKeyspace("k").setShard("s").setTabletType(Topodata.TabletType.MASTER).build());
        Assert.assertEquals("Wrong number of master Tablet, only one master is accepted", 1, healthCheckList.size());
        // tablet 1 is the primary now
        assertTabletHealthCheck(healthCheckList.get(0),
            mockTablet1.getTablet(),
            Query.Target.newBuilder().setKeyspace("k").setShard("s").setTabletType(Topodata.TabletType.MASTER).build(),
            new AtomicBoolean(true),
            new AtomicBoolean(false),
            Query.RealtimeStats.newBuilder().setCpuUsage(0.5).setSecondsBehindMaster(0).build());

        sendOnNextMessage(mockTablet2, Topodata.TabletType.MASTER, true, 20, 0.5, 0);

        sleepMillisSeconds(200);
        Assert.assertEquals("Wrong Tablet data", 2, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 1, hc.getHealthyCopy().size());
        healthCheckList = hc.getHealthyTabletStats(Query.Target.newBuilder().setKeyspace("k").setShard("s").setTabletType(Topodata.TabletType.MASTER).build());
        Assert.assertEquals("Wrong number of master Tablet, only one master is accepted", 1, healthCheckList.size());
        // tablet 2 is the primary now
        assertTabletHealthCheck(healthCheckList.get(0),
            mockTablet2.getTablet(),
            createTarget(Topodata.TabletType.MASTER),
            new AtomicBoolean(true),
            new AtomicBoolean(false),
            Query.RealtimeStats.newBuilder().setCpuUsage(0.5).setSecondsBehindMaster(0).build());

        MockTablet.closeQueryService(mockTablet1, mockTablet2);

        printOk();
    }

    @Test
    public void testHealthCheckVerifiesTabletAlias() throws InterruptedException {
        printComment("5. HealthCheck Test receive a mismatch tablet info");
        printComment("a. Get Health");
        HealthCheck hc = getHealthCheck();

        printComment("b. Add a no-serving Tablet");

        MockTablet mockTablet = MockTablet.buildMockTablet(grpcCleanup, "cell", 0, "a", "k", "s", portMap, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet.getTablet());

        sleepMillisSeconds(200);
        Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 0, hc.getHealthyCopy().size());

        printComment("c. Modify the status of Tablet to serving");
        Query.Target target = Query.Target.newBuilder().setKeyspace("k").setShard("s").setTabletType(Topodata.TabletType.REPLICA).build();
        Query.StreamHealthResponse streamHealthResponse = createStreamHealthResponse(Topodata.TabletAlias.newBuilder().setCell("cellb").setUid(20).build(), target, true, 0, 0.5, 1);
        MockQueryServer.HealthCheckMessage message = new MockQueryServer.HealthCheckMessage(MockQueryServer.MessageType.Next, streamHealthResponse);
        mockTablet.getHealthMessage().put(message);

        sleepMillisSeconds(200);
        TabletHealthCheck thc = hc.getHealthByAliasCopy().get(TopoProto.tabletAliasString(mockTablet.getTablet().getAlias()));
        Assert.assertEquals(TabletStreamHealthStatus.TABLET_STREAM_HEALTH_STATUS_MISMATCH, thc.getTabletStreamHealthDetailStatus().get().getStatus());

        MockTablet.closeQueryService(mockTablet);
        printOk();
    }

    @Test
    public void testHealthCheckRemoveTabletAfterReparent() {
        printComment("6. HealthCheck Test remove tablet after reparent");
        printComment("a. Get Health");
        HealthCheck hc = getHealthCheck();

        printComment("b. Add a no-serving Tablet");

        // add master tablet
        MockTablet mockTablet = MockTablet.buildMockTablet(grpcCleanup, "cell", 0, "a", "k", "s", portMap, Topodata.TabletType.MASTER);
        hc.addTablet(mockTablet.getTablet());
        // add replica tablet
        MockTablet mockTablet1 = MockTablet.buildMockTablet(grpcCleanup, "cell", 1, "b", "k", "s", portMap, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet1.getTablet());

        sleepMillisSeconds(200);
        Assert.assertEquals("Wrong Tablet data", 2, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 0, hc.getHealthyCopy().size());

        printComment("c. Modify the status of Tablet to serving");
        sendOnNextMessage(mockTablet, Topodata.TabletType.MASTER, true, 0, 0.5, 0);
        sendOnNextMessage(mockTablet1, Topodata.TabletType.REPLICA, true, 0, 0.5, 1);

        sleepMillisSeconds(200);

        Assert.assertEquals("Wrong Tablet data", 2, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 2, hc.getHealthyCopy().size());

        printComment("d. Modify the role of tablet");
        sendOnNextMessage(mockTablet, Topodata.TabletType.REPLICA, true, 0, 0.5, 1);
        sendOnNextMessage(mockTablet1, Topodata.TabletType.MASTER, true, 10, 0.5, 0);
        sleepMillisSeconds(200);

        Assert.assertEquals("Wrong Tablet data", 2, hc.getHealthByAliasCopy().size());

        sendOnErrorMessage(mockTablet);

        printComment("remove tablet");
        hc.removeTablet(mockTablet.getTablet());

        sleepMillisSeconds(200);
        // healthcheck shouldn't response onNext message
        List<TabletHealthCheck> hcList = hc.getHealthyTabletStats(createTarget(Topodata.TabletType.REPLICA));
        Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
        if (hcList != null) {
            Assert.assertEquals(0, hcList.size());
        }
        hcList = hc.getHealthyTabletStats(createTarget(Topodata.TabletType.MASTER));
        Assert.assertEquals(1, hcList.size());
        MockTablet.closeQueryService(mockTablet, mockTablet1);
        printOk();
    }

    @Test
    public void testHealthCheckOnNextBeforeRemove() {
        printComment("6a. HealthCheck Test onNext before remove tablet");
        printComment("a. Get Health");
        HealthCheck hc = getHealthCheck();

        printComment("b. Add a no-serving Tablet");

        MockTablet mockTablet = MockTablet.buildMockTablet(grpcCleanup, "cell", 0, "a", "k", "s", portMap, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet.getTablet());

        sleepMillisSeconds(200);
        Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 0, hc.getHealthyCopy().size());

        printComment("c. Modify the status of Tablet to serving");
        sendOnNextMessage(mockTablet, Topodata.TabletType.REPLICA, true, 0, 0.5, 1);

        sleepMillisSeconds(200);

        Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 1, hc.getHealthyCopy().size());

        // remove tablet and send a onNext message parallel
        sendOnNextMessage(mockTablet, Topodata.TabletType.REPLICA, true, 0, 0.2, 2);
        executorService.execute(() -> hc.removeTablet(mockTablet.getTablet()));

        sleepMillisSeconds(200);
        // healthcheck shouldn't response onNext message
        Assert.assertEquals("Wrong Tablet data", 0, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 0, hc.getHealthyCopy().size());
        MockTablet.closeQueryService(mockTablet);
        printOk();
    }

    @Test
    public void testHealthCheckOnNextAfterRemove() {
        printComment("6b. HealthCheck Test onNext after remove tablet");
        printComment("a. Get Health");
        HealthCheck hc = getHealthCheck();

        printComment("b. Add a no-serving Tablet");

        MockTablet mockTablet = MockTablet.buildMockTablet(grpcCleanup, "cell", 0, "a", "k", "s", portMap, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet.getTablet());

        sleepMillisSeconds(200);
        Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 0, hc.getHealthyCopy().size());

        printComment("c. Modify the status of Tablet to serving");
        sendOnNextMessage(mockTablet, Topodata.TabletType.REPLICA, true, 0, 0.5, 1);

        sleepMillisSeconds(200);

        Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 1, hc.getHealthyCopy().size());

        // remove tablet and send a onNext message parallel
        executorService.execute(() -> hc.removeTablet(mockTablet.getTablet()));
        sendOnNextMessage(mockTablet, Topodata.TabletType.REPLICA, true, 0, 0.2, 2);

        sleepMillisSeconds(200);
        // healthcheck shouldn't response onNext message
        Assert.assertEquals("Wrong Tablet data", 0, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 0, hc.getHealthyCopy().size());
        MockTablet.closeQueryService(mockTablet);
        printOk();
    }

    @Test
    public void testHealthCheckTimeout() {
        printComment("7. HealthCheck Test when health check timeout");
        printComment("a. Get Health");
        HealthCheck hc = getHealthCheck();

        printComment("b. Add a no-serving Tablet");

        MockTablet mockTablet = MockTablet.buildMockTablet(grpcCleanup, "cell", 0, "a", "k", "s", portMap, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet.getTablet());

        sleepMillisSeconds(200);
        Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 0, hc.getHealthyCopy().size());
        Assert.assertEquals(1, mockTablet.getQueryServer().getConnectCount());

        printComment("c. Modify the status of Tablet to serving");
        sendOnNextMessage(mockTablet, Topodata.TabletType.REPLICA, true, 0, 0.5, 1);

        sleepMillisSeconds(200);

        Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 1, hc.getHealthyCopy().size());

        printComment("d. Sleep and wait for check timeout");

        sleepMillisSeconds(90 * 1000);
        Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 1, hc.getHealthyCopy().size());
        // user shouldn't get a checking timeout tablet
        List<TabletHealthCheck> healthCheckList = hc.getHealthyTabletStats(Query.Target.newBuilder().setKeyspace("k").setShard("s").setTabletType(Topodata.TabletType.REPLICA).build());
        Assert.assertEquals(0, healthCheckList.size());
        // server should receive retrying connect request
        if (mockTablet.getQueryServer().getConnectCount() < 2) {
            Assert.fail("HealthCheck should try to reconnect tablet query service");
        }

        MockTablet.closeQueryService(mockTablet);
    }

    /**
     * test the functionality of getHealthyTabletStats
     * 建议统一getHealthyTabletStats，不要即返回null，也可能返回empty list
     */
    @Test
    public void testGetHealthyTablet() {
        printComment("9. HealthCheck Test the functionality of getHealthyTabletStats");
        printComment("a. Get Health");
        HealthCheck hc = getHealthCheck();

        printComment("b. Add a no-serving Tablet");
        MockTablet mockTablet = MockTablet.buildMockTablet(grpcCleanup, "cell", 0, "a", "k", "s", portMap, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet.getTablet());

        sleepMillisSeconds(200);
        Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 0, hc.getHealthyCopy().size());
        Assert.assertEquals(1, mockTablet.getQueryServer().getConnectCount());

        printComment("c. Modify the status of Tablet to serving");
        sendOnNextMessage(mockTablet, Topodata.TabletType.REPLICA, true, 0, 0.5, 1);
        sleepMillisSeconds(200);

        List<TabletHealthCheck> hcList = hc.getHealthyTabletStats(Query.Target.newBuilder().setKeyspace("k").setShard("s").setTabletType(Topodata.TabletType.REPLICA).build());
        Assert.assertEquals(1, hcList.size());
        assertTabletHealthCheck(hcList.get(0),
            mockTablet.getTablet(),
            Query.Target.newBuilder().setKeyspace("k").setShard("s").setTabletType(Topodata.TabletType.REPLICA).build(),
            new AtomicBoolean(true),
            new AtomicBoolean(false),
            Query.RealtimeStats.newBuilder().setCpuUsage(0.5).setSecondsBehindMaster(1).build());

        // update health with a change that won't change health array
        printComment("d. update health with realtime stats a change that will change health array");
        sendOnNextMessage(mockTablet, Topodata.TabletType.REPLICA, true, 0, 0.2, 35);
        sleepMillisSeconds(200);

        hcList = hc.getHealthyTabletStats(Query.Target.newBuilder().setKeyspace("k").setShard("s").setTabletType(Topodata.TabletType.REPLICA).build());
        Assert.assertEquals(1, hcList.size());
        assertTabletHealthCheck(hcList.get(0),
            mockTablet.getTablet(),
            Query.Target.newBuilder().setKeyspace("k").setShard("s").setTabletType(Topodata.TabletType.REPLICA).build(),
            new AtomicBoolean(true),
            new AtomicBoolean(false),
            Query.RealtimeStats.newBuilder().setCpuUsage(0.2).setSecondsBehindMaster(35).build());

        printComment("e. Add a second tablet");
        MockTablet mockTablet2 = MockTablet.buildMockTablet(grpcCleanup, "cell", 11, "host2", "k", "s", portMap, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet2.getTablet());
        sleepMillisSeconds(200);

        printComment("f. Modify the status of Tablet2 to serving");
        sendOnNextMessage(mockTablet2, Topodata.TabletType.REPLICA, true, 0, 0.2, 10);
        sleepMillisSeconds(200);

        hcList = hc.getHealthyTabletStats(createTarget(Topodata.TabletType.REPLICA));
        Assert.assertEquals("Wrong number of healthy replica tablets", 2, hcList.size());
        TabletHealthCheck thc1 = hcList.get(0);
        TabletHealthCheck thc2 = hcList.get(1);

        if (thc1.getTablet().getAlias().getUid() == 11) {
            TabletHealthCheck tmp = thc1;
            thc1 = thc2;
            thc2 = tmp;
        }
        assertTabletHealthCheck(thc1,
            mockTablet.getTablet(),
            createTarget(Topodata.TabletType.REPLICA),
            new AtomicBoolean(true),
            new AtomicBoolean(false),
            Query.RealtimeStats.newBuilder().setCpuUsage(0.2).setSecondsBehindMaster(35).build());
        assertTabletHealthCheck(thc2,
            mockTablet2.getTablet(),
            createTarget(Topodata.TabletType.REPLICA),
            new AtomicBoolean(true),
            new AtomicBoolean(false),
            Query.RealtimeStats.newBuilder().setCpuUsage(0.2).setSecondsBehindMaster(10).build());

        printComment("g. Modify the status of Tablet2 to no-serving");
        sendOnNextMessage(mockTablet2, Topodata.TabletType.REPLICA, false, 0, 0.2, 10);
        sleepMillisSeconds(200);

        hcList = hc.getHealthyTabletStats(createTarget(Topodata.TabletType.REPLICA));
        Assert.assertEquals("Wrong number of healthy replica tablets", 1, hcList.size());
        assertTabletHealthCheck(hcList.get(0),
            mockTablet.getTablet(),
            createTarget(Topodata.TabletType.REPLICA),
            new AtomicBoolean(true),
            new AtomicBoolean(false),
            Query.RealtimeStats.newBuilder().setCpuUsage(0.2).setSecondsBehindMaster(35).build());

        printComment("h. Second tablet turns into a primary");
        sendOnNextMessage(mockTablet2, Topodata.TabletType.MASTER, true, 10, 0.2, 0);
        sleepMillisSeconds(200);

        hcList = hc.getHealthyTabletStats(Query.Target.newBuilder().setKeyspace("k").setShard("s").setTabletType(Topodata.TabletType.MASTER).build());
        Assert.assertEquals("Wrong number of healthy master tablets", 1, hcList.size());
        assertTabletHealthCheck(hcList.get(0),
            mockTablet2.getTablet(),
            createTarget(Topodata.TabletType.MASTER),
            new AtomicBoolean(true),
            new AtomicBoolean(false),
            Query.RealtimeStats.newBuilder().setCpuUsage(0.2).setSecondsBehindMaster(0).build());

        printComment("i. Old replica goes into primary");
        sendOnNextMessage(mockTablet, Topodata.TabletType.MASTER, true, 20, 0.2, 0);
        sleepMillisSeconds(200);

        // check we lost all replicas, and primary is new one
        hcList = hc.getHealthyTabletStats(createTarget(Topodata.TabletType.REPLICA));
        Assert.assertNull("Wrong number of healthy replica tablets", hcList);

        hcList = hc.getHealthyTabletStats(createTarget(Topodata.TabletType.MASTER));
        Assert.assertEquals("Wrong number of healthy master tablets", 1, hcList.size());
        assertTabletHealthCheck(hcList.get(0),
            mockTablet.getTablet(),
            createTarget(Topodata.TabletType.MASTER),
            new AtomicBoolean(true),
            new AtomicBoolean(false),
            Query.RealtimeStats.newBuilder().setCpuUsage(0.2).setSecondsBehindMaster(0).build());

        // old primary sending an old pin should be ignored
        sendOnNextMessage(mockTablet2, Topodata.TabletType.MASTER, true, 10, 0.2, 0);
        sleepMillisSeconds(200);
        hcList = hc.getHealthyTabletStats(createTarget(Topodata.TabletType.MASTER));
        Assert.assertEquals("Wrong number of healthy master tablets", 1, hcList.size());
        assertTabletHealthCheck(hcList.get(0),
            mockTablet.getTablet(),
            createTarget(Topodata.TabletType.MASTER),
            new AtomicBoolean(true),
            new AtomicBoolean(false),
            Query.RealtimeStats.newBuilder().setCpuUsage(0.2).setSecondsBehindMaster(0).build());

        MockTablet.closeQueryService(mockTablet, mockTablet2);
        printOk();
    }

    @Test
    public void testPrimaryInOtherCell() throws TopoException, InterruptedException {
        TopologyWatcherManager.INSTANCE.resetScheduledExecutor();

        TopoServer topoServer = MemoryTopoFactory.newServerAndFactory("cell1", "cell2").getTopoServer();
        startWatchTopo("k", topoServer, "cell1", "cell2");

        printComment("10. HealthCheck Test Primary in other cell");
        printComment("a. Get Health");
        HealthCheck hc = getHealthCheck();

        printComment("b. Add a no-serving primary Tablet in different cell");
        MockTablet mockTablet = MockTablet.buildMockTablet(grpcCleanup, "cell2", 0, "a", "k", "s", portMap, Topodata.TabletType.MASTER);
        hc.addTablet(mockTablet.getTablet());
        sleepMillisSeconds(200);

        printComment("c. Modify the status of Tablet to serving");
        sendOnNextMessage(mockTablet, Topodata.TabletType.MASTER, true, 0, 0.5, 0);
        sleepMillisSeconds(200);

        printComment("d.// check that PRIMARY tablet from other cell IS in healthy tablet list");
        List<TabletHealthCheck> hcList = hc.getHealthyTabletStats(Query.Target.newBuilder().setKeyspace("k").setShard("s").setTabletType(Topodata.TabletType.MASTER).build());
        Assert.assertEquals(1, hcList.size());

        MockTablet.closeQueryService(mockTablet);

        printOk();
    }

    @Test
    public void testReplicaInOtherCell() throws TopoException, InterruptedException {
        TopologyWatcherManager.INSTANCE.resetScheduledExecutor();

        TopoServer topoServer = MemoryTopoFactory.newServerAndFactory("cell1", "cell2").getTopoServer();
        startWatchTopo("k", topoServer, "cell1", "cell2");

        printComment("11. HealthCheck Test Primary in other cell");
        printComment("a. Get Health");
        HealthCheck hc = getHealthCheck();

        printComment("b. Add a no-serving replica Tablet");
        MockTablet mockTablet = MockTablet.buildMockTablet(grpcCleanup, "cell1", 0, "a", "k", "s", portMap, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet.getTablet());
        sleepMillisSeconds(200);

        printComment("c. Modify the status of Tablet to serving");
        sendOnNextMessage(mockTablet, Topodata.TabletType.REPLICA, true, 0, 0.5, 0);
        sleepMillisSeconds(200);

        printComment("d. check that replica tablet IS in healthy tablet list");
        List<TabletHealthCheck> hcList = hc.getHealthyTabletStats(createTarget(Topodata.TabletType.REPLICA));
        Assert.assertEquals(1, hcList.size());

        printComment("e. Add a tablet as replica in different cell");

        MockTablet mockTablet1 = MockTablet.buildMockTablet(grpcCleanup, "cell2", 1, "b", "k", "s", portMap, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet1.getTablet());
        sleepMillisSeconds(200);

        printComment("f. Modify the status of Tablet to serving");
        sendOnNextMessage(mockTablet1, Topodata.TabletType.REPLICA, true, 0, 0.5, 1);
        sleepMillisSeconds(200);

        printComment("g. check that only REPLICA tablet from cell1 is in healthy tablet list");
        hcList = hc.getHealthyTabletStats(Query.Target.newBuilder().setKeyspace("k").setShard("s").setTabletType(Topodata.TabletType.REPLICA).build());
        Assert.assertEquals(2, hcList.size());

        MockTablet.closeQueryService(mockTablet, mockTablet1);

        printOk();
    }

    @Test
    public void testGetStandbyTablet() {
        printComment("12. HealthCheck Test get healthy tablet maybe standby");
        printComment("a. Get Health");
        HealthCheck hc = getHealthCheck();

        printComment("b. Add a no-serving replica Tablet");
        MockTablet mockTablet = MockTablet.buildMockTablet(grpcCleanup, "cell1", 0, "a", "k", "s", portMap, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet.getTablet());

        MockTablet mockTablet1 = MockTablet.buildMockTablet(grpcCleanup, "cell1", 1, "b", "k", "s", portMap, Topodata.TabletType.RDONLY);
        hc.addTablet(mockTablet1.getTablet());

        sleepMillisSeconds(200);

        printComment("c. Modify the status of REPLICA Tablet to serving");
        sendOnNextMessage(mockTablet, Topodata.TabletType.REPLICA, true, 0, 0.5, 0);
        sleepMillisSeconds(200);

        printComment("d. Try to get RDONLY tablet by standBy func");
        List<TabletHealthCheck> hcList = hc.getHealthyTabletStatsMaybeStandby(Query.Target.newBuilder().setKeyspace("k").setShard("s").setTabletType(Topodata.TabletType.RDONLY).build());
        Assert.assertEquals("Expect get one tablet health check", hcList.size(), 1);
        printComment("Tablet health check should be replica");
        assertTabletHealthCheck(hcList.get(0),
            mockTablet.getTablet(),
            createTarget(Topodata.TabletType.REPLICA),
            new AtomicBoolean(true),
            new AtomicBoolean(false),
            Query.RealtimeStats.newBuilder().setCpuUsage(0.5).setSecondsBehindMaster(0).build());

        printComment("e. Modify the status of RDONLY Tablet to serving");
        sendOnNextMessage(mockTablet1, Topodata.TabletType.RDONLY, true, 0, 0.25, 10);
        sleepMillisSeconds(200);

        printComment("f. send onError the replica GRPC server");
        sendOnErrorMessage(mockTablet);
        sleepMillisSeconds(200);

        printComment("g. Try to get REPLICA tablet by standBy func");
        hcList = hc.getHealthyTabletStatsMaybeStandby(createTarget(Topodata.TabletType.REPLICA));
        Assert.assertEquals("Expect get one tablet health check", hcList.size(), 1);
        printComment("Tablet health check should be RDONLY");
        assertTabletHealthCheck(hcList.get(0),
            mockTablet1.getTablet(),
            createTarget(Topodata.TabletType.RDONLY),
            new AtomicBoolean(true),
            new AtomicBoolean(false),
            Query.RealtimeStats.newBuilder().setCpuUsage(0.25).setSecondsBehindMaster(10).build());

        MockTablet.closeQueryService(mockTablet1);
        printOk();
    }

    @Test
    public void testUnhealthyReplicaAsSecondsBehind() {
        printComment("13. HealthCheck Test get healthy tablet maybe standby");
        printComment("a. Get Health");
        HealthCheck hc = getHealthCheck();

        printComment("b. Add a no-serving replica Tablet");
        MockTablet mockTablet = MockTablet.buildMockTablet(grpcCleanup, "cell1", 0, "a", "k", "s", portMap, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet.getTablet());
        sleepMillisSeconds(200);

        printComment("c. Modify the status of REPLICA Tablet to serving");
        sendOnNextMessage(mockTablet, Topodata.TabletType.REPLICA, true, 0, 0.5, 10);
        sleepMillisSeconds(200);

        List<TabletHealthCheck> hcList = hc.getHealthyTabletStats(createTarget(Topodata.TabletType.REPLICA));
        Assert.assertEquals("Wrong number of healthy replica tablets", 1, hcList.size());

        printComment("e. Modify the value of seconds behind master of REPLICA Tablet");
        sendOnNextMessage(mockTablet, Topodata.TabletType.REPLICA, true, 0, 0.5, 7201);
        sleepMillisSeconds(200);

        hcList = hc.getHealthyTabletStats(createTarget(Topodata.TabletType.REPLICA));
        if (hcList != null) {
            Assert.assertEquals("Wrong number of healthy replica tablets, it should be 0", 0, hcList.size());
        }

        MockTablet.closeQueryService(mockTablet);
        printOk();
    }

    @Test
    public void testMysqlPort0to3358() {
        printComment("14. HealthCheck Test in Tablet MySQL port changed from 0 to 3358");
        printComment("a. Get Health");
        HealthCheck hc = getHealthCheck();

        printComment("b. Add a no-serving replica Tablet (MysqlPort = 0)");
        defaultMysqlPort = 0;
        MockTablet mockTablet = MockTablet.buildMockTablet(grpcCleanup, "cell1", 0, "a", "k", "s", portMap, Topodata.TabletType.REPLICA, defaultMysqlPort);
        hc.addTablet(mockTablet.getTablet());
        sleepMillisSeconds(200);
        Assert.assertEquals("Wrong Tablet data", 0, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 0, hc.getHealthyCopy().size());

        printComment("c. replace a no-serving replica Tablet (MysqlPort = 3358)");
        defaultMysqlPort = 3358;
        Topodata.Tablet tablet = buildTablet("cell1", 0, "a", "k", "s", portMap, Topodata.TabletType.REPLICA);
        hc.replaceTablet(mockTablet.getTablet(), tablet);
        sleepMillisSeconds(200);
        Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 0, hc.getHealthyCopy().size());

        printComment("d. Modify the status of REPLICA Tablet to serving");
        sendOnNextMessage(mockTablet, Topodata.TabletType.REPLICA, true, 0, 0.5, 10);
        sleepMillisSeconds(200);

        Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 1, hc.getHealthyCopy().size());
        List<TabletHealthCheck> hcList = hc.getHealthyTabletStats(createTarget(Topodata.TabletType.REPLICA));
        Assert.assertEquals("Wrong number of healthy replica tablets", 1, hcList.size());

        MockTablet.closeQueryService(mockTablet);
        printOk();
    }

    @Test
    public void testMysqlPort3358to0() {
        printComment("15. HealthCheck Test in Tablet MySQL port changed from 3358 to 0");
        printComment("a. Get Health");
        HealthCheck hc = getHealthCheck();

        printComment("b. Add a no-serving Tablet(MysqlPort = 3358)");
        MockTablet mockTablet = MockTablet.buildMockTablet(grpcCleanup, "cell", 0, "a", "k", "s", portMap, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet.getTablet());

        Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 0, hc.getHealthyCopy().size());
        sleepMillisSeconds(200);

        printComment("c. Modify the status of Tablet to serving");
        sendOnNextMessage(mockTablet, Topodata.TabletType.REPLICA, true, 0, 0.5, 1);

        sleepMillisSeconds(200);

        Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 1, hc.getHealthyCopy().size());
        List<TabletHealthCheck> hcList = hc.getHealthyTabletStats(createTarget(Topodata.TabletType.REPLICA));
        Assert.assertEquals("Wrong number of healthy replica tablets", 1, hcList.size());

        printComment("d. replace a no-serving replica Tablet (MysqlPort = 0)");
        defaultMysqlPort = 0;
        Topodata.Tablet tablet = buildTablet("cell1", 0, "a", "k", "s", portMap, Topodata.TabletType.REPLICA);
        hc.replaceTablet(mockTablet.getTablet(), tablet);
        sleepMillisSeconds(6000);

        Assert.assertEquals("Wrong Tablet data", 0, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 0, hc.getHealthyCopy().size());

        MockTablet.closeQueryService(mockTablet);
        printOk();
    }

    @Test
    public void testDoubleMaster() {
        printComment("16. double master one no serving");
        printComment("a. Get Health");
        HealthCheck hc = getHealthCheck();

        printComment("b. Add a no-serving Tablet");

        // add master tablet
        MockTablet mockTablet = MockTablet.buildMockTablet(grpcCleanup, "cell", 0, "a", "k", "s", portMap, Topodata.TabletType.MASTER);
        hc.addTablet(mockTablet.getTablet());
        // add replica tablet
        MockTablet mockTablet1 = MockTablet.buildMockTablet(grpcCleanup, "cell", 1, "b", "k", "s", portMap, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet1.getTablet());

        sleepMillisSeconds(200);
        Assert.assertEquals("Wrong Tablet data", 2, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 0, hc.getHealthyCopy().size());

        printComment("c. Modify the status of Tablet to serving");
        sendOnNextMessage(mockTablet, Topodata.TabletType.MASTER, true, 0, 0.5, 0);
        sendOnNextMessage(mockTablet1, Topodata.TabletType.REPLICA, true, 0, 0.5, 1);

        sleepMillisSeconds(200);

        Assert.assertEquals("Wrong Tablet data", 2, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 2, hc.getHealthyCopy().size());

        printComment("d. Modify the role of tablet");
        sendOnNextMessage(mockTablet1, Topodata.TabletType.MASTER, true, 10, 0.5, 0);
        sleepMillisSeconds(200);

        printComment("e. Modify old master Tablet to no serving");
        sendOnErrorMessage(mockTablet);
        sleepMillisSeconds(200);

        Assert.assertEquals("Wrong Tablet data", 2, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 1, hc.getHealthyCopy().size());

        List<TabletHealthCheck> healthyTabletStats = hc.getHealthyTabletStats(createTarget(Topodata.TabletType.REPLICA));
        Assert.assertTrue(CollectionUtils.isEmpty(healthyTabletStats));

        healthyTabletStats = hc.getHealthyTabletStats(createTarget(Topodata.TabletType.MASTER));
        Assert.assertTrue(CollectionUtils.isNotEmpty(healthyTabletStats));
        Assert.assertEquals(1, healthyTabletStats.size());

        MockTablet.closeQueryService(mockTablet, mockTablet1);
        printOk();
    }

    @Test
    public void testHealthyListChecksum() {
        HealthCheck hc = getHealthCheck();
        Topodata.Tablet tablet1 = buildTablet("cella", 7, "1.1.1.7", "k", "s", portMap, Topodata.TabletType.REPLICA);
        Topodata.Tablet tablet2 = buildTablet("cella", 8, "1.1.1.8", "k", "s", portMap, Topodata.TabletType.REPLICA);
        Query.Target target = Query.Target.newBuilder().setKeyspace(tablet1.getKeyspace()).setShard(tablet1.getShard()).setTabletType(tablet1.getType()).build();

        Map<String, List<TabletHealthCheck>> healthy1 = hc.getHealthyCopy();
        List<TabletHealthCheck> healthyMap1 = new ArrayList<>();

        TabletHealthCheck thc1 = new TabletHealthCheck(null, tablet1, target);
        thc1.getServing().set(true);
        TabletHealthCheck thc2 = new TabletHealthCheck(null, tablet2, target);
        thc2.getServing().set(true);

        healthyMap1.add(thc1);
        healthyMap1.add(thc2);
        healthy1.put("k1", healthyMap1);

        Map<String, List<TabletHealthCheck>> healthy2 = hc.getHealthyCopy();
        List<TabletHealthCheck> healthyMap2 = new ArrayList<>();
        healthyMap2.add(thc2);
        healthyMap2.add(thc1);
        healthy2.put("k1", healthyMap2);

        long healthy1Crc32 = HealthyCollector.stateHealthyChecksum(healthy1);
        long healthy2Crc32 = HealthyCollector.stateHealthyChecksum(healthy2);
        Assert.assertEquals("Wrong HealthyChecksum", healthy1Crc32, healthy2Crc32);
    }

    @Test
    public void testHealthyChecksumSetBehindMaster() {
        HealthCheck hc = getHealthCheck();
        // add tablet
        String keyInHealthy = "k.s.replica";
        MockTablet mockTablet1 = MockTablet.buildMockTablet(grpcCleanup, "cellb", 67, "1.2.3.67", "k", "s", portMap, Topodata.TabletType.REPLICA);
        MockTablet mockTablet2 = MockTablet.buildMockTablet(grpcCleanup, "cellb", 198, "1.2.3.198", "k", "s", portMap, Topodata.TabletType.REPLICA);

        hc.addTablet(mockTablet1.getTablet());
        hc.addTablet(mockTablet2.getTablet());
        sleepMillisSeconds(200);
        sendOnNextMessage(mockTablet1, Topodata.TabletType.REPLICA, true, 0, 0.5, 1);
        sendOnNextMessage(mockTablet2, Topodata.TabletType.REPLICA, true, 0, 0.5, 2);
        sleepMillisSeconds(200);

        // sort list in healthy order by secondsBehindMaster
        hc.recomputeHealthyLocked(keyInHealthy);
        long firstCrc32 = HealthyCollector.stateHealthyChecksum(hc.getHealthyCopy());

        sendOnNextMessage(mockTablet1, Topodata.TabletType.REPLICA, true, 0, 0.5, 2);
        sendOnNextMessage(mockTablet2, Topodata.TabletType.REPLICA, true, 0, 0.5, 1);
        sleepMillisSeconds(200);

        // sort list in healthy order by secondsBehindMaster
        hc.recomputeHealthyLocked(keyInHealthy);
        long secondCrc32 = HealthyCollector.stateHealthyChecksum(hc.getHealthyCopy());

        Assert.assertNotEquals(hc.getHealthyCopy().get(keyInHealthy).get(0).getTablet().getHostname(), hc.getHealthyCopy().get(keyInHealthy).get(1).getTablet().getHostname());
        Assert.assertEquals("Wrong HealthyChecksum", firstCrc32, secondCrc32);

        MockTablet.closeQueryService(mockTablet1, mockTablet2);
    }

    @Test
    public void testConcurrentModificationException() {
        thrown.expect(ConcurrentModificationException.class);

        List<TabletHealthCheck> tabletHealthCheckList = new ArrayList<>();
        String keyspace = "k";
        String shard = "s";
        Topodata.TabletType type = Topodata.TabletType.REPLICA;
        Query.Target target = Query.Target.newBuilder().setKeyspace(keyspace).setShard(keyspace).setTabletType(type).build();
        Topodata.Tablet tablet1 = buildTablet("cell1", 1, "a", keyspace, shard, portMap, type);
        Topodata.Tablet tablet2 = buildTablet("cell1", 100, "a", keyspace, shard, portMap, type);
        Topodata.Tablet tablet3 = buildTablet("cell1", 10, "a", keyspace, shard, portMap, type);
        TabletHealthCheck tabletHealthCheck1 = new TabletHealthCheck(HealthCheck.INSTANCE, tablet1, target);
        TabletHealthCheck tabletHealthCheck2 = new TabletHealthCheck(HealthCheck.INSTANCE, tablet2, target);
        TabletHealthCheck tabletHealthCheck3 = new TabletHealthCheck(HealthCheck.INSTANCE, tablet3, target);
        tabletHealthCheckList.add(tabletHealthCheck1);
        tabletHealthCheckList.add(tabletHealthCheck2);
        tabletHealthCheckList.add(tabletHealthCheck3);
        Map<String, List<TabletHealthCheck>> healthy = new ConcurrentHashMap<>(16);
        healthy.put(HealthCheck.keyFromTarget(target), tabletHealthCheckList);

        Map<String, List<TabletHealthCheck>> treeMap = new TreeMap<>(healthy);

        int count = 1000000;
        executorService.execute(() -> {
            for (int i = 0; i < count; i++) {
                HealthyCollector.stateHealthyChecksum(treeMap);
            }
        });
        for (int i = 0; i < count; i++) {
            mockGetHealthyTabletStats(healthy, target);
        }

        sleepMillisSeconds(2000);
    }

    @Test
    public void testHealthyConcurrentModificationException() {
        HealthCheck hc = getHealthCheck();

        String keyspace = "k";
        String shard = "s";
        // add replica tablet
        MockTablet mockTablet0 = MockTablet.buildMockTablet(grpcCleanup, "cell", 1, "a", keyspace, shard, portMap, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet0.getTablet());
        // add replica tablet
        MockTablet mockTablet1 = MockTablet.buildMockTablet(grpcCleanup, "cell", 100, "b", keyspace, shard, portMap, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet1.getTablet());
        // add replica tablet
        MockTablet mockTablet2 = MockTablet.buildMockTablet(grpcCleanup, "cell", 10, "c", keyspace, shard, portMap, Topodata.TabletType.REPLICA);
        hc.addTablet(mockTablet2.getTablet());

        sleepMillisSeconds(200);
        Assert.assertEquals("Wrong Tablet data", 3, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 0, hc.getHealthyCopy().size());

        printComment("c. Modify the status of Tablet to serving");
        sendOnNextMessage(mockTablet0, Topodata.TabletType.REPLICA, true, 0, 0.5, 0);
        sendOnNextMessage(mockTablet1, Topodata.TabletType.REPLICA, true, 0, 0.5, 0);
        sendOnNextMessage(mockTablet2, Topodata.TabletType.REPLICA, true, 0, 0.5, 0);

        sleepMillisSeconds(200);

        Assert.assertEquals("Wrong Tablet data", 3, hc.getHealthByAliasCopy().size());
        Assert.assertEquals("Wrong Healthy Tablet data", 1, hc.getHealthyCopy().size());
        List<TabletHealthCheck> healthyTabletStats = hc.getHealthyTabletStats(createTarget(Topodata.TabletType.REPLICA));
        Assert.assertNotNull(healthyTabletStats);
        Assert.assertEquals("Wrong Tablet data", 3, healthyTabletStats.size());

        int count = 1000000;
        Map<String, List<TabletHealthCheck>> healthy = hc.getHealthyCopy();
        executorService.execute(() -> {
            for (int i = 0; i < count; i++) {
                HealthyCollector.stateHealthyChecksum(healthy);
            }
        });
        for (int i = 0; i < count; i++) {
            healthyTabletStats = hc.getHealthyTabletStats(createTarget(Topodata.TabletType.REPLICA));
            Assert.assertNotNull(healthyTabletStats);
            Assert.assertEquals("Wrong Tablet data", 3, healthyTabletStats.size());
        }

        sleepMillisSeconds(2000);
        MockTablet.closeQueryService(mockTablet0, mockTablet1, mockTablet2);
    }

    @Test
    public void ipReUse() {
        @AllArgsConstructor
        @ToString
        class TestCase {
            final Topodata.TabletType oldType;

            final Topodata.TabletType newType;
        }
        TestCase testCase1 = new TestCase(Topodata.TabletType.REPLICA, Topodata.TabletType.MASTER);
        TestCase testCase2 = new TestCase(Topodata.TabletType.REPLICA, Topodata.TabletType.REPLICA);
        TestCase testCase3 = new TestCase(Topodata.TabletType.REPLICA, Topodata.TabletType.RDONLY);
        TestCase testCase4 = new TestCase(Topodata.TabletType.MASTER, Topodata.TabletType.MASTER);
        TestCase testCase5 = new TestCase(Topodata.TabletType.MASTER, Topodata.TabletType.REPLICA);
        TestCase testCase6 = new TestCase(Topodata.TabletType.MASTER, Topodata.TabletType.RDONLY);
        TestCase testCase7 = new TestCase(Topodata.TabletType.RDONLY, Topodata.TabletType.MASTER);
        TestCase testCase8 = new TestCase(Topodata.TabletType.RDONLY, Topodata.TabletType.REPLICA);
        TestCase testCase9 = new TestCase(Topodata.TabletType.RDONLY, Topodata.TabletType.RDONLY);
        ArrayList<TestCase> testCases = Lists.newArrayList(testCase1, testCase2, testCase3, testCase4, testCase5, testCase6, testCase7, testCase8, testCase9);

        for (TestCase testCase : testCases) {
            printComment("HealthCheck Test when Tablet ip reuse");
            HealthCheck hc = getHealthCheck();

            printComment("1. Add a no-serving Tablet");
            String ip = "127.0.0.1";
            MockTablet mockTablet = MockTablet.buildMockTablet(grpcCleanup, "cell", 0, ip, "k", "s", portMap, testCase.oldType);
            hc.addTablet(mockTablet.getTablet());

            Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
            Assert.assertEquals("Wrong Healthy Tablet data", 0, getHealthySize(hc.getHealthyCopy()));
            sleepMillisSeconds(200);

            printComment("2. Modify the status of Tablet to serving");
            sendOnNextMessage(mockTablet, testCase.oldType, true, 0, 0.5, 0);

            sleepMillisSeconds(200);

            Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
            Assert.assertEquals("Wrong Healthy Tablet data", 1, getHealthySize(hc.getHealthyCopy()));

            printComment("3. Modify the status of Tablet to no serving");
            sendOnNextMessage(mockTablet, testCase.oldType, false, 0, 0.5, 0);

            sleepMillisSeconds(200);

            printComment("4. Add another no-serving new Tablet");
            MockTablet mockTablet2 = MockTablet.buildMockTablet(grpcCleanup, "cell", 1, ip, "k2", "s", portMap, testCase.newType);
            hc.addTablet(mockTablet2.getTablet());

            Assert.assertEquals("Wrong Tablet data", 2, hc.getHealthByAliasCopy().size());
            Assert.assertEquals("Wrong Healthy Tablet data", 0, getHealthySize(hc.getHealthyCopy()));
            sleepMillisSeconds(200);

            printComment("5. Modify the status of new Tablet to serving");
            sendOnNextMessage(mockTablet2, testCase.newType, true, 0, 0.5, 1);

            sleepMillisSeconds(200);

            Assert.assertEquals("Wrong Tablet data", 2, hc.getHealthByAliasCopy().size());
            Assert.assertEquals("Wrong Healthy Tablet data", 1, getHealthySize(hc.getHealthyCopy()));

            printComment("4. removeTablet old Tablet");
            hc.removeTablet(mockTablet.getTablet());

            sleepMillisSeconds(2000);

            Assert.assertEquals("Wrong Tablet data", 1, hc.getHealthByAliasCopy().size());
            Assert.assertEquals("Wrong Healthy Tablet data", 1, getHealthySize(hc.getHealthyCopy()));

            MockTablet.closeQueryService(mockTablet, mockTablet2);
            printOk("test ipReUse success,testCase :" + testCase);
            HealthCheck.resetHealthCheck();
        }
    }

    private int getHealthySize(Map<String, List<TabletHealthCheck>> map) {
        int size = 0;
        for (Map.Entry<String, List<TabletHealthCheck>> entry : map.entrySet()) {
            size += entry.getValue().size();
        }
        return size;
    }

    private List<TabletHealthCheck> mockGetHealthyTabletStats(Map<String, List<TabletHealthCheck>> healthy, Query.Target target) {
        List<TabletHealthCheck> list = healthy.get(HealthCheck.keyFromTarget(target));
        if (null == list || list.isEmpty()) {
            return null;
        }
        if (target.getTabletType() == Topodata.TabletType.MASTER) {
            return list;
        }
        List<TabletHealthCheck> servlist = new ArrayList<>(list.size());
        list.forEach(entry -> {
            if (entry.getServing().get()) {
                servlist.add(entry);
            }
        });
        return servlist;
    }

    private void startWatchTopo(String keyspaceName, TopoServer topoServer, String... cells) {
        for (String cell : cells) {
            TopologyWatcherManager.INSTANCE.startWatch(globalContext, topoServer, cell, keyspaceName, TimeUnit.SECONDS);
        }
    }

    protected HealthCheck getHealthCheck() {
        HealthCheck hc = HealthCheck.INSTANCE;
        Assert.assertEquals(0, hc.getHealthByAliasCopy().size());
        Assert.assertEquals(0, hc.getHealthyCopy().size());
        return hc;
    }

    private void assertTabletHealthCheck(TabletHealthCheck actualTabletHealthCheck, Topodata.Tablet expectTablet, Query.Target expectTarget, AtomicBoolean expectServing, AtomicBoolean expectRetrying,
                                         Query.RealtimeStats expectStats) {
        Assert.assertNotNull(actualTabletHealthCheck);

        Assert.assertEquals("Wrong tablet", expectTablet, actualTabletHealthCheck.getTablet());
        Assert.assertEquals("Wrong target", expectTarget, actualTabletHealthCheck.getTarget());
        Assert.assertEquals("Wrong serving status", expectServing.get(), actualTabletHealthCheck.getServing().get());
        Assert.assertEquals("Wrong retrying status", expectRetrying.get(), actualTabletHealthCheck.getRetrying().get());
        Assert.assertEquals("Wrong realtime stats", expectStats, actualTabletHealthCheck.getStats());
    }

    private Topodata.Tablet buildTablet(String cell, Integer uid, String hostName, String keyspaceName, String shard, Map<String, Integer> portMap, Topodata.TabletType type) {
        Topodata.TabletAlias tabletAlias = Topodata.TabletAlias.newBuilder().setCell(cell).setUid(uid).build();

        Topodata.Tablet.Builder tabletBuilder = Topodata.Tablet.newBuilder()
            .setHostname(hostName).setAlias(tabletAlias).setKeyspace(keyspaceName).setShard(shard).setType(type).setMysqlHostname(hostName).setMysqlPort(defaultMysqlPort);
        for (Map.Entry<String, Integer> portEntry : portMap.entrySet()) {
            tabletBuilder.putPortMap(portEntry.getKey(), portEntry.getValue());
        }
        Topodata.Tablet tablet = tabletBuilder.build();
        return tablet;
    }

    private Query.StreamHealthResponse createStreamHealthResponse(Topodata.TabletAlias tabletAlias, Query.Target target, boolean isServing, int reparentedTimestamp, double cpuUsage,
                                                                  int secondsBehindMaster) {
        return Query.StreamHealthResponse.newBuilder()
            .setTabletAlias(tabletAlias)
            .setTarget(target)
            .setServing(isServing)
            .setTabletExternallyReparentedTimestamp(reparentedTimestamp)
            .setRealtimeStats(Query.RealtimeStats.newBuilder().setCpuUsage(cpuUsage).setSecondsBehindMaster(secondsBehindMaster).build())
            .build();
    }

    private Query.Target createTarget(Topodata.TabletType type) {
        return Query.Target.newBuilder().setKeyspace("k").setShard("s").setTabletType(type).build();
    }

    protected void sendOnNextMessage(MockTablet mockTablet, Topodata.TabletType type, boolean isServing, int reparentedTimestamp, double cpuUsage, int secondsBehindMaster) {
        Query.Target target = createTarget(type);
        Query.StreamHealthResponse streamHealthResponse = createStreamHealthResponse(mockTablet.getTablet().getAlias(), target, isServing, reparentedTimestamp, cpuUsage, secondsBehindMaster);
        MockQueryServer.HealthCheckMessage message = new MockQueryServer.HealthCheckMessage(MockQueryServer.MessageType.Next, streamHealthResponse);
        try {
            mockTablet.getHealthMessage().put(message);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected void sendOnErrorMessage(MockTablet mockTablet) {
        MockQueryServer.HealthCheckMessage error = new MockQueryServer.HealthCheckMessage(MockQueryServer.MessageType.Error, null);
        try {
            mockTablet.getHealthMessage().put(error);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
