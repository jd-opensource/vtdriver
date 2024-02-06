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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

public class VitessDriverExecuteBatchTest extends TestSuite {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private Connection driverConnection;

    @Before
    public void init() throws SQLException {
        String url = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS)) + "&rewriteBatchedStatements=true";
        driverConnection = DriverManager.getConnection(url);
        driverConnection.createStatement().executeUpdate("delete from user ");
    }

    @After
    public void clean() throws SQLException {
        if (driverConnection != null) {
            driverConnection.close();
        }
    }

    @Test
    public void testConcurrency() throws InterruptedException, SQLException {
        int threadCount = 10;
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService service = getThreadPool(10, 10);
        AtomicInteger inserted = new AtomicInteger(0);
        for (int i = 0; i < 10; i++) {
            final int finalI = i;
            service.execute(() -> {
                try {
                    Statement statement = driverConnection.createStatement();
                    String sql = "insert into `user`(id,`name`) values(%d,'%d')";
                    int count = 3000;
                    // 0 1 2 3 4 5 6 7 8 9
                    // 10 11 12 13 14 15 16 17 18 19
                    // ...
                    // 90 91 92 93 94 95 96 97 98 99
                    // 100 101 102 103 104 105 106 107 108 109
                    // ...
                    // 2990 2991 ... 2999
                    for (int j = 0; j < count; j++) {
                        if (j % 10 != finalI) {
                            continue;
                        }
                        if ((j - finalI) % 100 == 0) {
                            statement.addBatch("select * from user");
                            continue;
                        }
                        inserted.incrementAndGet();
                        String format = String.format(sql, j, j);
                        statement.addBatch(format);
                    }
                    int[] counts = statement.executeBatch();

                    Assert.assertEquals(counts.length, 300);
                    for (int j = 0; j < counts.length; j++) {
                        if (j % 10 == 0) {
                            Assert.assertEquals(counts[j], -1);
                            continue;
                        }
                        Assert.assertEquals(counts[j], 1);
                    }
                    latch.countDown();
                } catch (SQLException e) {
                    e.printStackTrace();
                    Assert.fail();
                }
            });
        }
        latch.await();

        Statement stmt = driverConnection.createStatement();
        ResultSet rs = stmt.executeQuery("select id, name from user");
        int rows = 0;
        while (rs.next()) {
            rows++;
            Assert.assertEquals(String.valueOf(rs.getInt(1)), rs.getString(2));
        }
        Assert.assertEquals(inserted.intValue(), rows);
    }

    @Test
    public void testRewriteBatchedStatementsFalse() throws SQLException {
        clean();
        thrown.expect(java.sql.SQLException.class);
        thrown.expectMessage("Can not issue select statements with executeUpdate()");
        String url = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS)) + "&rewriteBatchedStatements=false";
        driverConnection = DriverManager.getConnection(url);
        statementBatchTest();
    }

    @Test
    public void statementBatchTest() throws SQLException {
        Statement statement = driverConnection.createStatement();
        String sql = "insert into `user`(id,`name`) values(%d,'zhangSan')";
        int count = 300;
        for (int i = 1; i <= count; i++) {
            if (i % 10 == 0) {
                statement.addBatch("select * from user");
                continue;
            }
            String format = String.format(sql, i);
            statement.addBatch(format);
        }
        int[] counts = statement.executeBatch();

        Assert.assertEquals(counts.length, count);
        int inserted = 0;
        for (int i = 0; i < counts.length; i++) {
            if ((i + 1) % 10 == 0) {
                Assert.assertEquals(counts[i], -1);
                continue;
            }
            inserted++;
            Assert.assertEquals(counts[i], 1);
        }

        ResultSet resultSet = statement.executeQuery("select * from user");
        int rows = 0;
        while (resultSet.next()) {
            rows++;
            Assert.assertEquals(resultSet.getString("name"), "zhangSan");
        }
        Assert.assertEquals(inserted, rows);

        String updateSql = "update user set predef1=100 where id = %d";
        for (int i = 1; i <= count; i++) {
            if (i % 10 == 0) {
                statement.addBatch("select * from user");
                continue;
            }
            String format = String.format(updateSql, i);
            statement.addBatch(format);
        }
        int[] updateCounts = statement.executeBatch();

        Assert.assertEquals(updateCounts.length, count);
        for (int i = 0; i < updateCounts.length; i++) {
            if ((i + 1) % 10 == 0) {
                Assert.assertEquals(updateCounts[i], -1);
                continue;
            }
            Assert.assertEquals(updateCounts[i], 1);
        }

        ResultSet resultSet1 = statement.executeQuery("select * from user");
        rows = 0;
        while (resultSet1.next()) {
            rows++;
            Assert.assertEquals(resultSet1.getInt("predef1"), 100);
        }
        Assert.assertEquals(inserted, rows);

    }


    @Test
    public void prepareStatementBatchTest() throws SQLException {
        PreparedStatement statement = driverConnection.prepareStatement("insert into `user`(id,`name`) values(?,?)");
        int count = 300;
        for (int i = 1; i <= count; i++) {
            statement.setInt(1, i);
            statement.setString(2, "zhangSan");
            statement.addBatch();
        }
        int[] counts = statement.executeBatch();
        Assert.assertEquals(counts.length, count);
        for (int i = 0; i < counts.length; i++) {
            Assert.assertEquals(counts[i], 1);
        }

        ResultSet resultSet = statement.executeQuery("select * from user");
        int rows = 0;
        while (resultSet.next()) {
            rows++;
            Assert.assertEquals(resultSet.getString("name"), "zhangSan");
        }
        Assert.assertEquals(count, rows);

        statement.close();

        PreparedStatement updateStmt = driverConnection.prepareStatement("update user set predef1=100 where id = ?");
        for (int i = 1; i <= count; i++) {
            updateStmt.setInt(1, i);
            updateStmt.addBatch();
        }
        int[] updateCounts = updateStmt.executeBatch();

        Assert.assertEquals(updateCounts.length, count);
        for (int i = 0; i < updateCounts.length; i++) {
            Assert.assertEquals(updateCounts[i], 1);
        }

        ResultSet resultSet1 = updateStmt.executeQuery("select * from user");
        rows = 0;
        while (resultSet1.next()) {
            rows++;
            Assert.assertEquals(resultSet1.getInt("predef1"), 100);
        }
        Assert.assertEquals(count, rows);
    }
}
