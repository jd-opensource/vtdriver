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

import com.jd.jdbc.common.Constant;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.context.VtContext;
import com.jd.jdbc.monitor.SrvKeyspaceCollector;
import com.jd.jdbc.topo.Topo;
import com.jd.jdbc.topo.TopoConnection;
import com.jd.jdbc.topo.TopoException;
import com.jd.jdbc.topo.TopoServer;
import com.jd.jdbc.topo.etcd2topo.Etcd2TopoServer;
import com.jd.jdbc.topo.etcd2topo.EtcdWatcher;
import com.jd.jdbc.vitess.VitessJdbcUrlParser;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.PutOption;
import io.netty.util.CharsetUtil;
import io.prometheus.client.Collector;
import io.vitess.proto.Topodata;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class EtcdTopoServerTest extends TestSuite {

    private final IContext globalContext = VtContext.withCancel(VtContext.background());

    private static final ExecutorService executorService = getThreadPool(10, 10);

    private TopoServer topoServer;

    private Client client;

    private String keyspace;

    private List<String> cells;

    private final int size = 100;

    private String cell;

    private String keyspacePrefix;

    private final String counter = "watch_SrvKeyspace_counter_total";

    private final String errorCounter = "watch_SrvKeyspace_error_counter_total";

    @AfterClass
    public static void afterClass() {
        executorService.shutdownNow();
    }

    @Before
    public void init() throws TopoException, SQLException {
        String connectionUrl = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        Properties prop = VitessJdbcUrlParser.parse(connectionUrl, null);
        keyspace = prop.getProperty(Constant.DRIVER_PROPERTY_SCHEMA);

        String topoServerAddress = "http://" + prop.getProperty("host") + ":" + prop.getProperty("port");
        topoServer = Topo.getTopoServer(globalContext, Topo.TopoServerImplementType.TOPO_IMPLEMENTATION_ETCD2, topoServerAddress);
        cells = topoServer.getAllCells(VtContext.withCancel(VtContext.background()));
        cell = cells.get(0);
        keyspacePrefix = "testkeyspace";

        // build jetcd Client
        Topodata.CellInfo cellInfo = topoServer.getCellInfo(null, cell, false);
        String serverAddress = cellInfo.getServerAddress();
        client = Client.builder().endpoints(serverAddress).build();
    }

    @After
    public void close() throws ExecutionException, InterruptedException {
        clearData(client);
    }

    @Test
    public void test01sameKeyspace() throws NoSuchFieldException, IllegalAccessException, InterruptedException {
        AtomicBoolean failFlag = new AtomicBoolean(false);
        for (int i = 0; i < size; i++) {
            executorService.execute(() -> {
                try {
                    TopoConnection topoConnection = topoServer.connForCell(null, cell);
                    topoConnection.watchSrvKeyspace(null, cell, keyspace);
                } catch (TopoException e) {
                    failFlag.set(true);
                }
            });
        }
        Assert.assertFalse(failFlag.get());
        TimeUnit.SECONDS.sleep(1);

        Field field = Etcd2TopoServer.class.getDeclaredField("WATCHER_MAP");
        field.setAccessible(true);
        ConcurrentMap<String, EtcdWatcher> watcherMap = (ConcurrentMap<String, EtcdWatcher>) field.get(null);
        Assert.assertNotNull(watcherMap);
        Assert.assertEquals(1, watcherMap.size());
    }

    @Test
    public void test02SrvKeyspaceExist() throws TopoException {
        for (String cell : cells) {
            Topodata.SrvKeyspace srvKeyspace = topoServer.getAndWatchSrvKeyspace(null, cell, keyspace);
            if (Objects.equals(srvKeyspace, Topodata.SrvKeyspace.getDefaultInstance())) {
                Assert.fail();
            }
        }
    }

    @Test
    public void test03watchKeyspaces() throws TopoException, InterruptedException {
        // 1.init data
        Topodata.SrvKeyspace srvKeyspace = initData(client);

        // 2.start watch
        for (int i = 0; i < size; i++) {
            TopoConnection topoConnection = topoServer.connForCell(null, cell);
            topoConnection.watchSrvKeyspace(null, cell, keyspacePrefix + i);
        }

        // 3.change data
        for (int i = 0; i < size; i++) {
            String path = buildPath(i);
            ByteSequence key = ByteSequence.from(path, CharsetUtil.UTF_8);

            Topodata.ShardReference shardReference = Topodata.ShardReference.newBuilder().buildPartial();
            Topodata.SrvKeyspace.KeyspacePartition keyspacePartition = Topodata.SrvKeyspace.KeyspacePartition.newBuilder().addShardReferences(shardReference).build();
            Topodata.SrvKeyspace newSrvKeyspace = srvKeyspace.toBuilder().addPartitions(keyspacePartition).build();

            ByteSequence value = ByteSequence.from(newSrvKeyspace.toByteArray());
            client.getKVClient().put(key, value, PutOption.DEFAULT);
        }
        TimeUnit.SECONDS.sleep(2);

        // 可能会watch到1次或2次变化
        checkCounter(counter, 2.0D, 1.0D);
    }

    @Test
    public void test04watchKeyspacesError() throws TopoException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        // 1.init data
        Topodata.SrvKeyspace srvKeyspace = initData(client);

        // 2.start watch
        for (int i = 0; i < size; i++) {
            TopoConnection topoConnection = topoServer.connForCell(null, cell);
            topoConnection.watchSrvKeyspace(null, cell, keyspacePrefix + i);
        }

        // 3.change error data
        for (int i = 0; i < size; i++) {
            String path = buildPath(i);
            ByteSequence key = ByteSequence.from(path, CharsetUtil.UTF_8);
            ByteSequence value = ByteSequence.from("xxx".getBytes(StandardCharsets.UTF_8));
            client.getKVClient().put(key, value, PutOption.DEFAULT);
        }
        TimeUnit.SECONDS.sleep(2);
        checkCounter(errorCounter, 1.0D);

        // 4.change error data
        for (int i = 0; i < size; i++) {
            String path = buildPath(i);
            ByteSequence key = ByteSequence.from(path, CharsetUtil.UTF_8);
            ByteSequence value = ByteSequence.EMPTY;
            client.getKVClient().put(key, value, PutOption.DEFAULT);
        }
        TimeUnit.SECONDS.sleep(2);
        checkCounter(errorCounter, 2.0D);

        // 5.resume data
        for (int i = 0; i < size; i++) {
            String path = buildPath(i);
            ByteSequence key = ByteSequence.from(path, CharsetUtil.UTF_8);
            ByteSequence value = ByteSequence.from(srvKeyspace.toByteArray());
            client.getKVClient().put(key, value, PutOption.DEFAULT);
        }
        TimeUnit.SECONDS.sleep(2);
        checkCounter(counter, 3.0D, 4.0D);

        Field field = TopoServer.class.getDeclaredField("SRVKEYSPACE_MAP");
        field.setAccessible(true);
        Map<String, Topodata.SrvKeyspace> srvKeyspaceMap = (Map<String, Topodata.SrvKeyspace>) field.get(null);
        for (Topodata.SrvKeyspace value : srvKeyspaceMap.values()) {
            Assert.assertEquals(srvKeyspace, value);
        }
    }

    private void clearData(Client client) throws InterruptedException, ExecutionException {
        for (int i = 0; i < size; i++) {
            String path = buildPath(i);
            ByteSequence key = ByteSequence.from(path, CharsetUtil.UTF_8);
            DeleteOption deleteOption = DeleteOption.newBuilder().build();
            client.getKVClient().delete(key, deleteOption).get();
        }
        TimeUnit.SECONDS.sleep(2);
    }

    private String buildPath(int i) {
        return "/vt/" + cell + "/keyspaces/" + keyspacePrefix + i + "/SrvKeyspace";
    }

    private Topodata.SrvKeyspace initData(Client client) throws TopoException, InterruptedException {
        Topodata.SrvKeyspace srvKeyspace = topoServer.getSrvKeyspace(null, cell, keyspace);

        for (int i = 0; i < size; i++) {
            String path = buildPath(i);
            ByteSequence key = ByteSequence.from(path, CharsetUtil.UTF_8);
            ByteSequence value = ByteSequence.from(srvKeyspace.toByteArray());
            client.getKVClient().put(key, value, PutOption.DEFAULT);
        }
        TimeUnit.SECONDS.sleep(2);
        return srvKeyspace;
    }

    private void checkCounter(String sampleName, double expected) {
        checkCounter(sampleName, expected, expected);
    }

    private void checkCounter(String sampleName, double expected1, double expected2) {
        List<Collector.MetricFamilySamples> metricFamilySamples = SrvKeyspaceCollector.getCounter().collect();
        for (Collector.MetricFamilySamples metricFamilySample : metricFamilySamples) {
            for (Collector.MetricFamilySamples.Sample sample : metricFamilySample.samples) {
                if (Objects.equals(sampleName, sample.name)) {
                    if (!Objects.equals(sample.value, expected1) && !Objects.equals(sample.value, expected2)) {
                        Assert.fail();
                    }
                }
            }
        }
    }
}
