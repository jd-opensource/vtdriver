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
import com.jd.jdbc.topo.Topo;
import com.jd.jdbc.topo.TopoServer;
import com.jd.jdbc.vitess.VitessJdbcUrlParser;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

public class Etcd2TopoServerTest extends TestSuite {

    @Test
    @Ignore
    public void testTopoExecuteTimeout() throws Exception {
        long timeout = 50000;
        System.setProperty("vtdriver.topoExecuteTimeout", timeout + "");

        String connectionUrl = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        Properties prop = VitessJdbcUrlParser.parse(connectionUrl, null);
        String topoServerAddress = "http://" + prop.getProperty("host") + ":" + prop.getProperty("port");
        TopoServer topoServer = Topo.getTopoServer(Topo.TopoServerImplementType.TOPO_IMPLEMENTATION_ETCD2, topoServerAddress);
        List<String> cells = topoServer.getAllCells(VtContext.withCancel(VtContext.background()));
        String cell = cells.get(0);
        topoServer.connForCell(null, cell);

        Field field = Etcd2TopoServer.class.getDeclaredField("DEFALUT_TIMEOUT");
        field.setAccessible(true);
        long defalutTimeout = (long) field.get(null);
        Assert.assertEquals(timeout, defalutTimeout);
    }

    @Test
    public void testTopoExecuteTimeout2() throws Exception {
        String connectionUrl = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        Properties prop = VitessJdbcUrlParser.parse(connectionUrl, null);
        String topoServerAddress = "http://" + prop.getProperty("host") + ":" + prop.getProperty("port");
        TopoServer topoServer = Topo.getTopoServer(Topo.TopoServerImplementType.TOPO_IMPLEMENTATION_ETCD2, topoServerAddress);
        List<String> cells = topoServer.getAllCells(VtContext.withCancel(VtContext.background()));
        String cell = cells.get(0);
        topoServer.connForCell(null, cell);

        Field field = Etcd2TopoServer.class.getDeclaredField("DEFALUT_TIMEOUT");
        field.setAccessible(true);
        long defalutTimeout = (long) field.get(null);
        Assert.assertEquals(10000L, defalutTimeout);
    }
}