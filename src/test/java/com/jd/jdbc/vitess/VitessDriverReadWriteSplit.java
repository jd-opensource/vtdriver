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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runners.MethodSorters;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class VitessDriverReadWriteSplit extends TestSuite {
    protected static String baseUrl;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    protected Connection rrConnection;

    protected Connection rwConnection;

    @Before
    public void init() throws Exception {
        baseUrl = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS));

        rrConnection = DriverManager.getConnection(baseUrl + "&role=rr");
        rwConnection = DriverManager.getConnection(baseUrl + "&role=rw");
    }

    @After
    public void close() throws SQLException {
        if (rrConnection != null) {
            rrConnection.close();
        }
        if (rwConnection != null) {
            rwConnection.close();
        }
    }

    @Test
    public void test01() throws SQLException {
        thrown.expect(SQLException.class);
        thrown.expectMessage("is not allowed for read only connection");
        try (Statement stmt = rrConnection.createStatement()) {
            stmt.executeUpdate("delete from test");
        }
    }

    @Test
    public void test02() throws SQLException {
        try (Statement stmt = rwConnection.createStatement()) {
            stmt.executeUpdate("delete from test");
        }
    }

    @Test
    public void test03() throws SQLException {
        thrown.expect(SQLException.class);
        thrown.expectMessage("is not allowed for read only connection");
        try (Statement stmt = rrConnection.createStatement()) {
            stmt.executeUpdate("insert into test (f_tinyint,f_int) values(1,2)");
        }
    }

    @Test
    public void test04() throws SQLException {
        try (Statement stmt = rwConnection.createStatement()) {
            int affectedRows = stmt.executeUpdate("insert into test (f_tinyint,f_int) values(1,2)");
            Assert.assertEquals(1, affectedRows);
        }
    }

    @Test
    public void test05() throws SQLException {
        thrown.expect(SQLException.class);
        thrown.expectMessage("is not allowed for read only connection");
        try (Statement stmt = rrConnection.createStatement()) {
            stmt.executeUpdate("update test  set f_int = 100 where f_tinyint = 1");
        }
    }

    @Test
    public void test06() throws SQLException {
        try (Statement stmt = rwConnection.createStatement()) {
            int affectedRows = stmt.executeUpdate("update test  set f_int = 100 where f_tinyint = 1");
            Assert.assertEquals(1, affectedRows);
        }
    }

    @Test
    public void test07() throws SQLException {
        try (Statement stmt = rwConnection.createStatement()) {
            ResultSet resultSet = stmt.executeQuery("select f_tinyint,f_int from test where f_tinyint = 1");
            while (resultSet.next()) {
                Assert.assertEquals(100, resultSet.getInt(2));
            }
        }
    }

    @Test
    public void test08() throws SQLException {
        sleep(10);
        try (Statement stmt = rrConnection.createStatement()) {
            ResultSet resultSet;
            resultSet = stmt.executeQuery("select f_tinyint,f_int from test where f_tinyint = 1");
            while (resultSet.next()) {
                Assert.assertEquals(100, resultSet.getInt(2));
            }
        }
    }
}
