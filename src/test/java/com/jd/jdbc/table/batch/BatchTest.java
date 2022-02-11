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

package com.jd.jdbc.table.batch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

public class BatchTest extends TestSuite {
    private Connection driverConnection;

    @Before
    public void init() throws SQLException {
        String url = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        driverConnection = DriverManager.getConnection(url);
        driverConnection.createStatement().executeUpdate("delete from table_engine_test");
    }

    @After
    public void clean() throws SQLException {
        driverConnection.createStatement().executeUpdate("delete from table_engine_test");
        closeConnection(driverConnection);
    }

    @Test
    public void testConcurrency() throws InterruptedException, SQLException {
        int threadCount = 10;
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        AtomicInteger inserted = new AtomicInteger(0);
        for (int i = 0; i < 10; i++) {
            final int finalI = i;
            service.execute(() -> {
                try {
                    Statement statement = driverConnection.createStatement();
                    String sql = "insert into table_engine_test (id,f_key,f_tinyint,f_bit) values(%d, 'zhangSan', 1, true)";
                    int count = 3000;
                    for (int j = 0; j < count; j++) {
                        if (j % 10 != finalI) {
                            continue;
                        }
                        inserted.incrementAndGet();
                        String format = String.format(sql, j);
                        statement.addBatch(format);
                    }
                    int[] counts = statement.executeBatch();

                    Assert.assertEquals(counts.length, 300);
                    for (int k : counts) {
                        Assert.assertEquals(k, 1);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail();
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();

        Statement stmt = driverConnection.createStatement();
        ResultSet resultSet = stmt.executeQuery("select * from table_engine_test");
        int rows = 0;
        while (resultSet.next()) {
            rows++;
            Assert.assertEquals(resultSet.getString("f_key"), "zhangSan");
        }
        Assert.assertEquals(inserted.get(), rows);
    }

    @Test
    public void statementBatchTest() throws SQLException {
        Statement statement = driverConnection.createStatement();
        String sql = "insert into table_engine_test (id,f_key,f_tinyint,f_bit) values(%d, 'zhangSan', 1, true)";
        int count = 300;
        for (int i = 1; i <= count; i++) {
            String format = String.format(sql, i);
            statement.addBatch(format);
        }
        int[] counts = statement.executeBatch();

        Assert.assertEquals(counts.length, count);
        int inserted = 0;
        for (int j : counts) {
            inserted++;
            Assert.assertEquals(j, 1);
        }

        ResultSet resultSet = statement.executeQuery("select * from table_engine_test");
        int rows = 0;
        while (resultSet.next()) {
            rows++;
            Assert.assertEquals(resultSet.getString("f_key"), "zhangSan");
            Assert.assertEquals(resultSet.getString(2), "zhangSan");
            Assert.assertEquals(resultSet.getInt("f_tinyint"), 1);
            Assert.assertEquals(resultSet.getInt(3), 1);
            Assert.assertTrue(resultSet.getBoolean(4));
            Assert.assertTrue(resultSet.getBoolean("f_bit"));
        }
        Assert.assertEquals(inserted, rows);

        String updateSql = "update table_engine_test set f_tinyint = 2 where id = %d";
        for (int i = 1; i <= count; i++) {
            String format = String.format(updateSql, i);
            statement.addBatch(format);
        }
        int[] updateCounts = statement.executeBatch();

        Assert.assertEquals(updateCounts.length, count);
        for (int updateCount : updateCounts) {
            Assert.assertEquals(updateCount, 1);
        }

        ResultSet resultSet1 = statement.executeQuery("select * from table_engine_test");
        rows = 0;
        while (resultSet1.next()) {
            rows++;
            Assert.assertEquals(resultSet1.getString("f_key"), "zhangSan");
            Assert.assertEquals(resultSet1.getString(2), "zhangSan");
            Assert.assertEquals(resultSet1.getInt("f_tinyint"), 2);
            Assert.assertEquals(resultSet1.getInt(3), 2);
            Assert.assertTrue(resultSet1.getBoolean(4));
            Assert.assertTrue(resultSet1.getBoolean("f_bit"));
        }
        Assert.assertEquals(inserted, rows);
    }

    @Test
    public void prepareStatementBatchTest() throws SQLException {
        PreparedStatement statement = driverConnection.prepareStatement("insert into table_engine_test (id,f_key,f_tinyint,f_bit) values(?, ?, ?, true)");
        int count = 300;
        for (int i = 1; i <= count; i++) {
            statement.setInt(1, i);
            statement.setString(2, "zhangSan");
            statement.setInt(3, 1);
            statement.addBatch();
        }
        int[] counts = statement.executeBatch();
        Assert.assertEquals(counts.length, count);
        for (int j : counts) {
            Assert.assertEquals(j, 1);
        }

        ResultSet resultSet = statement.executeQuery("select * from table_engine_test");
        int rows = 0;
        while (resultSet.next()) {
            rows++;
            Assert.assertEquals(resultSet.getString("f_key"), "zhangSan");
            Assert.assertEquals(resultSet.getString(2), "zhangSan");
            Assert.assertEquals(resultSet.getInt("f_tinyint"), 1);
            Assert.assertEquals(resultSet.getInt(3), 1);
            Assert.assertTrue(resultSet.getBoolean(4));
            Assert.assertTrue(resultSet.getBoolean("f_bit"));
        }
        Assert.assertEquals(count, rows);

        statement.close();

        PreparedStatement updateStmt = driverConnection.prepareStatement("update table_engine_test set f_tinyint = 2 where id = ?");
        for (int i = 1; i <= count; i++) {
            updateStmt.setInt(1, i);
            updateStmt.addBatch();
        }
        int[] updateCounts = updateStmt.executeBatch();

        Assert.assertEquals(updateCounts.length, count);
        for (int updateCount : updateCounts) {
            Assert.assertEquals(updateCount, 1);
        }

        ResultSet resultSet1 = updateStmt.executeQuery("select * from table_engine_test");
        rows = 0;
        while (resultSet1.next()) {
            rows++;
            Assert.assertEquals(resultSet1.getString("f_key"), "zhangSan");
            Assert.assertEquals(resultSet1.getString(2), "zhangSan");
            Assert.assertEquals(resultSet1.getInt("f_tinyint"), 2);
            Assert.assertEquals(resultSet1.getInt(3), 2);
            Assert.assertTrue(resultSet1.getBoolean(4));
            Assert.assertTrue(resultSet1.getBoolean("f_bit"));
        }
        Assert.assertEquals(count, rows);
    }
}
