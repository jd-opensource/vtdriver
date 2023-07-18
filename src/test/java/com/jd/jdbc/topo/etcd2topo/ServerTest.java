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

package com.jd.jdbc.topo.etcd2topo;

import com.jd.jdbc.context.VtContext;
import com.jd.jdbc.srvtopo.ResilientServer;
import com.jd.jdbc.srvtopo.SrvTopo;
import com.jd.jdbc.srvtopo.SrvTopoException;
import com.jd.jdbc.topo.Topo;
import com.jd.jdbc.topo.TopoException;
import com.jd.jdbc.topo.TopoExceptionCode;
import com.jd.jdbc.topo.TopoServer;
import com.jd.jdbc.util.threadpool.impl.VtDaemonExecutorService;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import io.netty.util.internal.StringUtil;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import vschema.Vschema;

public class ServerTest {
    private static final String TOPO_IMPLEMENTATION_K8S = "k8s";

    //The ip address needs to be modified to the ip address of the machine where etcd is located
    private static final String TOPO_ZONE1_PROXY_ADDRESS = "http://127.0.0.1:2379";

    private static final String TOPO_ZONE1_ROOT = "/vitess/zone1/tablets";

    private static final String TOPO_ZONE1_ROOT_END = "/vitess/zone1/tablets1";

    //The ip address needs to be modified to the ip address of the machine where etcd is located
    private static final String TOPO_GLOBAL_PROXY_ADDRESS = "http://127.0.0.1:2379";

    private static final String TOPO_GLOBAL_ROOT = "/vitess/global";

    private static final String TOPO_CELL = "zone1";

    private static final String TOPO_KEYSPACE = "customer";

    public static Topo.TopoServerImplementType TOPO_IMPLEMENTATION = Topo.TopoServerImplementType.TOPO_IMPLEMENTATION_ETCD2;

    //The ip address needs to be modified to the ip address of the machine where etcd is located
    public static String TOPO_GLOBAL_SERVER_ADDRESS = "http://127.0.0.1:2379";

    /**
     * @return
     * @throws TopoException
     */
    public static TopoServer open() throws TopoException {
        if (StringUtil.isNullOrEmpty(TOPO_GLOBAL_SERVER_ADDRESS) && !TOPO_IMPLEMENTATION_K8S.equalsIgnoreCase(TOPO_IMPLEMENTATION.name())) {
            throw TopoException.wrap("topo_global_server_address must be configured");
        }
        if (StringUtil.isNullOrEmpty(TOPO_GLOBAL_ROOT)) {
            throw TopoException.wrap("topo_global_root must be non-empty");
        }
        return Topo.getTopoServer(TOPO_IMPLEMENTATION, TOPO_GLOBAL_SERVER_ADDRESS);
    }

    @BeforeClass
    public static void registerFactory() {
        try {
            Topo.registerFactory(Topo.TopoServerImplementType.TOPO_IMPLEMENTATION_ETCD2);
        } catch (TopoException e) {
            System.err.println(e.getMessage());
        }
    }

    @Test
    public void case01_testClient() throws ExecutionException, InterruptedException {
        Client client = Client.builder().endpoints(TOPO_GLOBAL_PROXY_ADDRESS).build();
        Assert.assertNotNull(client);

        ByteSequence sequence = ByteSequence.from(TOPO_GLOBAL_ROOT, Charset.defaultCharset());
        GetOption option = GetOption.newBuilder()
            .withKeysOnly(true)
            .withPrefix(ByteSequence.from(sequence.getBytes()))
            .build();
        CompletableFuture<GetResponse> future = client.getKVClient().get(sequence, option);
        GetResponse resp = future.get();
        for (KeyValue kv : resp.getKvs()) {
            String key = kv.getKey().toString(Charset.defaultCharset());
            Assert.assertNotNull(key);
        }
    }

    @Test
    public void case02_testServer() {
        try {
            TopoServer topoServer = open();
            Assert.assertNotNull(topoServer);
            this.commonTestServer(topoServer);
        } catch (TopoException e) {
            Assert.assertNull(e);
        }
    }

    @Test
    public void case03_testServer() {
        try {
            TopoServer topoServer = Topo.getTopoServer(Topo.TopoServerImplementType.TOPO_IMPLEMENTATION_ETCD2, TOPO_GLOBAL_PROXY_ADDRESS);
            Assert.assertNotNull(topoServer);
            this.commonTestServer(topoServer);
        } catch (TopoException e) {
            Assert.assertNull(e);
        }
    }

    @Test
    public void case04_testWatch() throws TopoException {
        TopoServer topoServer = open();
        Assert.assertNotNull(topoServer);

        VtDaemonExecutorService.initialize(null, null, null);
        Topo.WatchSrvKeyspaceResponse watchSrvKeyspaceResponse = topoServer.watchSrvKeyspace(VtContext.withCancel(VtContext.background()), TOPO_CELL, TOPO_KEYSPACE);
        while (true) {
            BlockingQueue<Topo.WatchSrvKeyspaceData> change = watchSrvKeyspaceResponse.getChange();
            try {
                Topo.WatchSrvKeyspaceData watchSrvKeyspaceData = change.take();
                Topodata.SrvKeyspace srvKeyspace = watchSrvKeyspaceData.getValue();
                Assert.assertNotNull(srvKeyspace);
            } catch (InterruptedException e) {
                Assert.assertNull(e);
                break;
            }
        }
    }

    @Test
    public void case05_testGetSrvKeyspace() throws SrvTopoException, TopoException {
        TopoServer topoServer = open();
        ResilientServer resilientServer = SrvTopo.newResilientServer(topoServer, "");
        VtDaemonExecutorService.initialize(null, null, null);

        // Ask for a not-yet-created keyspace
        ResilientServer.GetSrvKeyspaceResponse srvKeyspace = resilientServer.getSrvKeyspace(VtContext.withCancel(VtContext.background()), "test_cell", "test_ks");
        Assert.assertNull(srvKeyspace.getSrvKeyspace());
        Assert.assertEquals(((TopoException) srvKeyspace.getException()).getCode(), TopoExceptionCode.NO_NODE);

        srvKeyspace = resilientServer.getSrvKeyspace(VtContext.withCancel(VtContext.background()), TOPO_CELL, TOPO_KEYSPACE);
        Assert.assertNotNull(srvKeyspace);
    }

    @Test
    public void case051_testGetSrvKeyspaces() throws Exception {
        TopoServer topoServer = open();
        ResilientServer resilientServer = SrvTopo.newResilientServer(topoServer, "");
        VtDaemonExecutorService.initialize(null, null, null);

        for (int i = 0; i < 5; i++) {
            VtDaemonExecutorService.execute(() -> {
                ResilientServer.GetSrvKeyspaceResponse srvKeyspace = resilientServer.getSrvKeyspace(VtContext.withCancel(VtContext.background()), TOPO_CELL, TOPO_KEYSPACE);
                Assert.assertNotNull(srvKeyspace);
                System.out.println(new Date() + "---" + srvKeyspace.getSrvKeyspace());
            });
        }
        TimeUnit.SECONDS.sleep(20);
    }

    @Test
    public void case052_testGetSrvKeyspaces() throws Exception {
        TopoServer topoServer = open();
        ResilientServer resilientServer = SrvTopo.newResilientServer(topoServer, "");
        VtDaemonExecutorService.initialize(null, null, null);

        String[] keyspaces = new String[] {TOPO_KEYSPACE, "vtdriver1", "jtm_tr"};

        for (String keyspace : keyspaces) {
            for (int i = 0; i < 2; i++) {
                VtDaemonExecutorService.execute(() -> {
                    ResilientServer.GetSrvKeyspaceResponse srvKeyspace = resilientServer.getSrvKeyspace(VtContext.withCancel(VtContext.background()), TOPO_CELL, keyspace);
                    Assert.assertNotNull(srvKeyspace);
                    System.out.println(new Date() + "---" + srvKeyspace.getSrvKeyspace());
                });
            }
        }
        TimeUnit.SECONDS.sleep(20);
    }

    @Test
    public void case06_testGetSrvKeyspaceNames() throws TopoException, SrvTopoException {
        TopoServer topoServer = open();
        ResilientServer resilientServer = SrvTopo.newResilientServer(topoServer, "");
        VtDaemonExecutorService.initialize(null, null, null);

        ResilientServer.GetSrvKeyspaceNamesResponse response = resilientServer.getSrvKeyspaceNames(VtContext.withCancel(VtContext.background()), TOPO_CELL, false);
        Assert.assertNotNull(response.getKeyspaceNameList());
        Assert.assertNull(response.getException());
    }

    @Test
    public void case07_testFindAllTargets() throws Exception {
        TopoServer topoServer = open();
        ResilientServer resilientServer = SrvTopo.newResilientServer(topoServer, "");
        VtDaemonExecutorService.initialize(null, null, null);

        List<Query.Target> targetList = SrvTopo.findAllTargets(VtContext.withCancel(VtContext.background()), resilientServer, TOPO_CELL, TOPO_KEYSPACE, new ArrayList<Topodata.TabletType>() {{
            add(Topodata.TabletType.MASTER);
            add(Topodata.TabletType.REPLICA);
            add(Topodata.TabletType.RDONLY);
        }});
        Assert.assertEquals(6, targetList.size());
    }

    @Test
    public void case08_testTimer() throws Exception {
        TopoServer topoServer = open();
        ResilientServer resilientServer = SrvTopo.newResilientServer(topoServer, "");
        TimeUnit.SECONDS.sleep(10);
    }

    private void commonTestServer(TopoServer topoServer) throws TopoException {
        List<Topodata.TabletAlias> tabletAliasList = topoServer.getTabletAliasByCell(VtContext.withCancel(VtContext.background()), TOPO_CELL);
        for (Topodata.TabletAlias tabletAlias : tabletAliasList) {
            Topodata.Tablet tablet = topoServer.getTablet(VtContext.withCancel(VtContext.background()), tabletAlias);
            Assert.assertNotNull(tablet);
        }

        Topodata.SrvKeyspace srvKeyspace = topoServer.getSrvKeyspace(VtContext.withCancel(VtContext.background()), TOPO_CELL, TOPO_KEYSPACE);
        Assert.assertNotNull(srvKeyspace);
        Assert.assertEquals(3, srvKeyspace.getPartitionsCount());

        Vschema.Keyspace keyspace = topoServer.getVschema(VtContext.withCancel(VtContext.background()), TOPO_KEYSPACE);
        Assert.assertTrue(keyspace.getSharded());
    }

    @Test
    public void case09_getTabletsByRange() throws ExecutionException, InterruptedException {

        Client client = Client.builder()
            .connectTimeout(Duration.ofSeconds(5))
            .waitForReady(false)
            .endpoints(TOPO_ZONE1_PROXY_ADDRESS)
            .build();
        Assert.assertNotNull(client);

        ByteSequence startSequence = ByteSequence.from(TOPO_ZONE1_ROOT.getBytes());
        ByteSequence endSequence = ByteSequence.from(TOPO_ZONE1_ROOT_END.getBytes());

        GetOption option = GetOption.newBuilder()
            .withRange(endSequence)
            .build();
        CompletableFuture<GetResponse> future = client.getKVClient().get(startSequence, option);
        GetResponse resp = future.get();
        for (KeyValue kv : resp.getKvs()) {
            String key = kv.getKey().toString(Charset.defaultCharset());
            Assert.assertNotNull(key);
        }
        System.out.println(resp.getCount());
    }
}
