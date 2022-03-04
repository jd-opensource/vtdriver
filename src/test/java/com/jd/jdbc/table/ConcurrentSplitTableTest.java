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

package com.jd.jdbc.table;

import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.tindexes.LogicTable;
import com.jd.jdbc.vitess.VitessDataSource;
import com.jd.jdbc.vitess.VitessPreparedStatement;
import io.vitess.proto.Query;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

public class ConcurrentSplitTableTest extends TestSuite {
    private static ExecutorService pool;

    private Connection conn;

    private CountDownLatch countDownLatch;

    private Statement statement;

    private int currentCount;

    private int dataCount;

    private static String getRandomKey() {
        int length = RandomUtils.nextInt(1, 10);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int i1 = RandomUtils.nextInt(33, 127);
            sb.append((char) i1);
        }
        return sb.toString();
    }

    @BeforeClass
    public static void initClass() {
        pool = Executors.newFixedThreadPool(10, new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(final Runnable r) {
                return new Thread(r, threadNumber.getAndIncrement() + "");
            }
        });
    }

    @AfterClass
    public static void afterClass() {
        pool.shutdownNow();
    }

    @Before
    public void init() throws Exception {
        conn = getTaskConnection();
        TableTestUtil.setSplitTableConfig("table/shardTable.yml");
        statement = conn.createStatement();
        currentCount = 10;
        dataCount = 100;
        countDownLatch = new CountDownLatch(currentCount);
        printInfo("currentThreadCount:" + currentCount);
        printInfo("dataCount:" + dataCount);
    }

    @After
    public void after() throws Exception {
        conn.close();
        TableTestUtil.setDefaultTableConfig();
    }

    @Test
    public void testShardTableByString() throws Exception {
        statement.execute("delete from table_engine_test");

        AtomicBoolean success = new AtomicBoolean(true);
        for (int i = 0; i < currentCount; i++) {
            pool.execute(new Task("table_engine_test", success, getTaskConnection()));
        }

        countDownLatch.await();
        Assert.assertTrue(success.get());
    }

    @Test
    public void testshardTableByMurmur() throws Exception {
        statement.execute("delete from shard_by_murmur_test");
        AtomicBoolean success = new AtomicBoolean(true);
        for (int i = 0; i < currentCount; i++) {
            pool.execute(new Task("shard_by_murmur_test", success, getTaskConnection()));
        }

        countDownLatch.await();
        Assert.assertTrue(success.get());
    }


    @Test
    public void testShardTableByLong() throws Exception {
        statement.execute("delete from shard_by_long_test");
        AtomicBoolean success = new AtomicBoolean(true);
        for (int i = 0; i < currentCount; i++) {
            pool.execute(new Task("shard_by_long_test", success, getTaskConnection()));
        }

        countDownLatch.await();
        Assert.assertTrue(success.get());
    }

    @Test
    public void testTableRuleMod() throws Exception {
        statement.execute("delete from shard_rule_mod");
        AtomicBoolean success = new AtomicBoolean(true);
        for (int i = 0; i < currentCount; i++) {
            pool.execute(new Task("shard_rule_mod", success, getTaskConnection()));
        }
        countDownLatch.await();
        Assert.assertTrue(success.get());
    }

    protected Connection getTaskConnection() throws SQLException {
        return getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
    }

    private class Task implements Runnable {
        private final Connection taskConn;

        private final String logicTable;

        private final LogicTable logicTables;

        private final AtomicBoolean success;

        Task(final String logicTable, final AtomicBoolean success, final Connection taskConn) throws SQLException {
            this.logicTable = logicTable;
            this.success = success;
            this.logicTables = VitessDataSource.getLogicTable(taskConn.getCatalog(), logicTable);
            this.taskConn = taskConn;
        }

        @Override
        public void run() {
            int index = Integer.parseInt(Thread.currentThread().getName());
            Query.Type type = logicTables.getTindexCol().getType();
            try {
                for (int i = (index % currentCount) * dataCount; i < (index % currentCount + 1) * dataCount; i++) {
                    String sql = "insert into " + logicTable + "(f_key,f_varchar,id) values (?,?,?)";
                    PreparedStatement statement = taskConn.prepareStatement(sql);
                    int id = i + 1;
                    switch (type) {
                        case INT32:
                            statement.setObject(1, RandomUtils.nextInt(0, Integer.MAX_VALUE));
                            break;
                        case VARCHAR:
                            statement.setObject(1, getRandomKey());
                            break;
                    }
                    BindVariable bindVariable = ((VitessPreparedStatement) statement).getClientPreparedQueryBindings().get(0).getBindVariableMap().get("0");
                    VtValue vtValue = VtValue.newVtValue(bindVariable);
                    String actualTableName = logicTables.map(vtValue).getActualTableName();

                    statement.setString(2, actualTableName);
                    statement.setInt(3, id);
                    statement.executeUpdate();

                    //验证是否成功且准确插入
                    Statement selectStatement = taskConn.createStatement();
                    ResultSet selectResultSet = selectStatement.executeQuery("select * from " + actualTableName + " where id=" + id);

                    Assert.assertTrue(selectResultSet.next());

                    //验证是否成功并准确修改
                    Statement updateStatement = taskConn.createStatement();
                    updateStatement.executeUpdate("update " + logicTable + " set f_midint =" + id + " where id =" + id);
                    ResultSet updateResultSet = updateStatement.executeQuery("select f_midint from " + logicTable + " where id =" + id);
                    while (updateResultSet.next()) {
                        Assert.assertEquals(updateResultSet.getInt(1), id);
                    }

                    //验证是否成功且准确删除
                    Statement deleteStatement = taskConn.createStatement();
                    deleteStatement.execute("delete from " + logicTable + " where id =" + id);
                    ResultSet deleteResultSet = deleteStatement.executeQuery("select * from " + logicTable + " where  id =" + id);
                    Assert.assertFalse(deleteResultSet.next());

                }

                taskConn.close();
            } catch (Exception throwables) {
                Assert.fail(throwables.getMessage());
                success.set(false);
            } finally {
                countDownLatch.countDown();
            }

        }
    }
}
