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

package com.jd.jdbc.vitess;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class VitessDriverUnshardedTest extends TestSuite {

    Connection driverConnection;

    @Before
    public void init() throws SQLException {
        driverConnection = getConnection(Driver.of(TestSuiteShardSpec.NO_SHARDS));
    }

    @After
    public void close() throws SQLException {
        if (driverConnection != null) {
            driverConnection.close();
        }
    }

    @Test
    public void test01SubQuery() throws Exception {

        Statement statement = driverConnection.createStatement();
        statement.executeUpdate("delete from unsharded; insert into unsharded (`predef1`,`predef2`) values(1,1),(2,2);");
        statement.executeUpdate("delete from unsharded_auto; insert into unsharded_auto (`id`,`val`) values(1,'a'),(2,'b');");

        ResultSet resultSet = statement.executeQuery("select predef1 from unsharded where predef2 in (select id from unsharded_auto) order by predef1;");
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(1, resultSet.getInt(1));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(2, resultSet.getInt(1));
        Assert.assertFalse(resultSet.next());

    }

    @Test
    public void test02Join() throws Exception {

        Statement statement = driverConnection.createStatement();

        ResultSet resultSet = statement.executeQuery("select unsharded.predef1,unsharded_auto.val from unsharded join unsharded_auto on unsharded.predef1=unsharded_auto.id;");
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(1, resultSet.getInt(1));
        Assert.assertEquals("a", resultSet.getString(2));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(2, resultSet.getInt(1));
        Assert.assertEquals("b", resultSet.getString(2));
        Assert.assertFalse(resultSet.next());

    }
}

