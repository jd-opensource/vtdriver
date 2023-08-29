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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
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

    protected Connection roConnection;

    protected Connection rrmConnection;

    @Before
    public void init() throws Exception {
        baseUrl = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS));

        rrConnection = DriverManager.getConnection(baseUrl + "&role=rr");
        rwConnection = DriverManager.getConnection(baseUrl + "&role=rw");
        roConnection = DriverManager.getConnection(baseUrl + "&role=ro");
        rrmConnection = DriverManager.getConnection(baseUrl + "&role=rrm");
    }

    @After
    public void close() throws SQLException {
        closeConnection(rrConnection, rwConnection, roConnection, rrmConnection);
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

    @Test
    public void test09() throws SQLException {
        thrown.expect(SQLException.class);
        thrown.expectMessage("is not allowed for read only connection");
        try (Statement stmt = roConnection.createStatement()) {
            stmt.executeUpdate("delete from test");
        }
    }

    @Test
    public void test10() throws SQLException {
        thrown.expect(SQLException.class);
        thrown.expectMessage("is not allowed for read only connection");
        try (Statement stmt = roConnection.createStatement()) {
            stmt.executeUpdate("insert into test (f_tinyint,f_int) values(1,2)");
        }
    }

    @Test
    public void test11() throws SQLException {
        thrown.expect(SQLException.class);
        thrown.expectMessage("is not allowed for read only connection");
        try (Statement stmt = roConnection.createStatement()) {
            stmt.executeUpdate("update test  set f_int = 100 where f_tinyint = 1");
        }
    }

    @Test
    public void test12() throws SQLException {
        sleep(10);
        try (Statement stmt = roConnection.createStatement()) {
            ResultSet resultSet;
            resultSet = stmt.executeQuery("select f_tinyint,f_int from test where f_tinyint = 1");
            while (resultSet.next()) {
                Assert.assertEquals(100, resultSet.getInt(2));
            }
        }
    }

    @Test
    public void test13() throws SQLException {
        thrown.expect(SQLException.class);
        thrown.expectMessage("error in jdbc url");
        DriverManager.getConnection(baseUrl + "&role=rt");
    }

    @Test
    public void test14() throws SQLException {
        thrown.expect(SQLException.class);
        thrown.expectMessage("is not allowed for read only connection");
        try (Statement stmt = rrmConnection.createStatement()) {
            stmt.executeUpdate("delete from test");
        }
    }

    @Test
    public void test15() throws SQLException {
        thrown.expect(SQLException.class);
        thrown.expectMessage("is not allowed for read only connection");
        try (Statement stmt = rrmConnection.createStatement()) {
            stmt.executeUpdate("insert into test (f_tinyint,f_int) values(1,2)");
        }
    }

    @Test
    public void test16() throws SQLException {
        thrown.expect(SQLException.class);
        thrown.expectMessage("is not allowed for read only connection");
        try (Statement stmt = rrmConnection.createStatement()) {
            stmt.executeUpdate("update test  set f_int = 100 where f_tinyint = 1");
        }
    }

    @Test
    public void test17() throws SQLException {
        sleep(10);
        try (Statement stmt = rrmConnection.createStatement()) {
            ResultSet resultSet;
            resultSet = stmt.executeQuery("select f_tinyint,f_int from test where f_tinyint = 1");
            while (resultSet.next()) {
                Assert.assertEquals(100, resultSet.getInt(2));
            }
        }
    }

    @Test
    public void concurrencyTest() throws InterruptedException {
        // 交替使用不同账户执行sql
        final Connection[] conns = new Connection[] {rwConnection, rrConnection, roConnection, rrmConnection};
        final String[] sqls = new String[] {
            "insert into user (id, name) values (null, '%s')",
            "select id, name from user limit 1",
            "select id, name from user limit 2",
            "select id, name from user limit 3",
        };
        final int numThreads = 4;
        final int count = 200;

        ExecutorService service = new ThreadPoolExecutor(numThreads, numThreads,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>());

        AtomicBoolean flag = new AtomicBoolean(true);
        CountDownLatch latch = new CountDownLatch(numThreads);
        for (int i = 0; i < numThreads; i++) {
            final int fi = i;
            service.execute(() -> {
                for (int k = 0; k < count; k++) {
                    int randInteger = RandomUtils.nextInt(0, conns.length);
                    String randString = RandomStringUtils.random(10, true, true);
                    Connection conn = conns[randInteger];
                    String sql = sqls[randInteger];
                    if (randInteger == 0) {
                        sql = String.format(sql, randString);
                    }

                    try (Statement stmt = conn.createStatement()) {
                        printInfo("thread" + fi + ", sql" + k + ": " + sql);
                        stmt.execute(sql);
                    } catch (SQLException e) {
                        flag.set(false);
                        System.out.println(printFail("concurrencyTest: error"));
                        e.printStackTrace();
                    }
                }
                latch.countDown();
            });
        }

        latch.await(180, TimeUnit.SECONDS);
        Assert.assertTrue(flag.get());
        printOk("[OK]");
    }

    @Test
    @Ignore
    public void multiKeyspaceTest() throws SQLException, InterruptedException {
        String sql11 = "select * from plan_test limit 10";
        PreparedStatement ps11 = rrmConnection.prepareStatement(sql11);

        for (int i = 0; i < 100000000; i++) {
            try {
                ResultSet resultSet = ps11.executeQuery();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        TimeUnit.SECONDS.sleep(4000);
    }
}
