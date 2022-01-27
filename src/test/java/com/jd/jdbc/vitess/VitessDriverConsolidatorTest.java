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

import com.google.common.collect.Lists;
import com.jd.jdbc.util.JsonUtil;
import com.jd.jdbc.util.consolidator.Consolidator;
import com.jd.jdbc.util.consolidator.ConsolidatorResult;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

public class VitessDriverConsolidatorTest extends TestSuite {

    private static final String DRIVER_NAME = "com.jd.jdbc.vitess.VitessDriver";

    private static final String SLOW_SQL = "select count(*) from sbtest1 where k > 0 ";

    private static ExecutorService executorService;

    private final int threadCount = 10;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private AtomicInteger thCounter = null;

    private AtomicInteger thCounter2 = null;

    private HikariDataSource dataSource1;

    private HikariDataSource dataSource2;

    @BeforeClass
    public static void initClass() {
        executorService = Executors.newFixedThreadPool(10);
    }

    @AfterClass
    public static void afterClass() {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    @Before
    public void init() throws Exception {
        String baseUrl = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        String baseUrl2;
        baseUrl2 = getConnectionUrl(Driver.of(TestSuiteShardSpec.NO_SHARDS));
        baseUrl = baseUrl + "&role=rr" + "&queryConsolidator=true";
        baseUrl2 = baseUrl2 + "&role=rr" + "&queryConsolidator=true";
        dataSource1 = buildDataSource(baseUrl);
        dataSource2 = buildDataSource(baseUrl2);
        thCounter = new AtomicInteger(0);
        thCounter2 = new AtomicInteger(0);
    }

    @After
    public void close() throws SQLException {
        if (dataSource1 != null) {
            dataSource1.close();
        }
        if (dataSource2 != null) {
            dataSource2.close();
        }
    }

    @Test
    public void test01() throws InterruptedException {
        int[] count = new int[threadCount];
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        AtomicBoolean atomicBoolean = new AtomicBoolean(true);
        for (int i = 0; i < threadCount; i++) {
            int andIncrement = thCounter.getAndIncrement();
            executorService.execute(new ConsolidatorTask(count, countDownLatch, atomicBoolean, andIncrement, dataSource1, false));
        }
        countDownLatch.await();
        int result = count[0];
        for (int i = 1; i < threadCount; i++) {
            if (count[i] != result) {
                Assert.fail();
            }
        }
        Assert.assertTrue(atomicBoolean.get());
        checkConsolidatorMap();
    }

    @Test
    public void testSingle() throws InterruptedException {
        int[] count = new int[1];
        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicBoolean atomicBoolean = new AtomicBoolean(true);
        executorService.execute(new ConsolidatorTask(count, countDownLatch, atomicBoolean, 0, dataSource1, false));
        countDownLatch.await();
        Assert.assertTrue(atomicBoolean.get());
        checkConsolidatorMap();
    }

    @Test
    public void testExcecpion() throws Exception {
        thrown.expect(SQLSyntaxErrorException.class);
        thrown.expectMessage("Unknown column 'a' in 'where clause'");
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        List<Exception> exceptionList = new CopyOnWriteArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            executorService.execute(() -> {
                try (Connection connection = dataSource1.getConnection();
                     Statement stmt = connection.createStatement()) {
                    stmt.executeQuery("select count(*) from sbtest1 where k > 0 and a<1;");
                } catch (Exception e) {
                    exceptionList.add(e);
                } finally {
                    countDownLatch.countDown();
                }
            });
        }
        countDownLatch.await();
        Exception exception = exceptionList.get(0);
        for (Exception exception1 : exceptionList) {
            if (!exception1.getClass().equals(exception.getClass()) || !exception1.getMessage().equals(exception.getMessage())) {
                Assert.fail();
            }
        }
        checkConsolidatorMap();
        throw exception;
    }

    @Test
    public void testTwoKeyspace() throws InterruptedException {
        int[] count = new int[threadCount];
        CountDownLatch countDownLatch = new CountDownLatch(threadCount * 2);
        AtomicBoolean atomicBoolean = new AtomicBoolean(true);
        for (int i = 0; i < threadCount; i++) {
            int andIncrement = thCounter.getAndIncrement();
            executorService.execute(new ConsolidatorTask(count, countDownLatch, atomicBoolean, andIncrement, dataSource1, false));
        }

        // dataSource2
        int[] count2 = new int[threadCount];
        for (int i = 0; i < threadCount; i++) {
            int andIncrement = thCounter2.getAndIncrement();
            executorService.execute(new ConsolidatorTask(count2, countDownLatch, atomicBoolean, andIncrement, dataSource2, false));
        }
        countDownLatch.await();
        int result = count[0];
        int result2 = count2[0];
        for (int i = 1; i < threadCount; i++) {
            if (count[i] != result) {
                Assert.fail();
            }
            if (count2[i] != result2) {
                Assert.fail();
            }
        }
        Assert.assertTrue(atomicBoolean.get());
        checkConsolidatorMap();
    }

    @Test
    public void testSqls() throws InterruptedException {
        List<String> slowSqls = Lists.newArrayList(SLOW_SQL, SLOW_SQL + "limit 10000000", SLOW_SQL + "limit 10000001", SLOW_SQL + "limit 10000002", SLOW_SQL + "limit 10000003");

        int[] count = new int[threadCount * slowSqls.size()];
        CountDownLatch countDownLatch = new CountDownLatch(threadCount * slowSqls.size());
        AtomicBoolean atomicBoolean = new AtomicBoolean(true);
        for (String slowSql : slowSqls) {
            for (int i = 0; i < threadCount; i++) {
                int andIncrement = thCounter.getAndIncrement();
                executorService.execute(new ConsolidatorTask(count, countDownLatch, atomicBoolean, andIncrement, dataSource1, false, slowSql));
            }
        }
        countDownLatch.await();
        int result = count[0];
        for (int i = 1; i < threadCount; i++) {
            if (count[i] != result) {
                Assert.fail();
            }
        }
        Assert.assertTrue(atomicBoolean.get());
        checkConsolidatorMap();
    }

    @Test
    public void testSkipTransaction() throws InterruptedException {
        int[] count = new int[threadCount];
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        AtomicBoolean atomicBoolean = new AtomicBoolean(true);
        for (int i = 0; i < threadCount; i++) {
            int andIncrement = thCounter.getAndIncrement();
            executorService.execute(new ConsolidatorTask(count, countDownLatch, atomicBoolean, andIncrement, dataSource1, true));
        }
        countDownLatch.await();
        int result = count[0];
        for (int i = 1; i < threadCount; i++) {
            if (count[i] != result) {
                Assert.fail();
            }
        }
        Assert.assertTrue(atomicBoolean.get());
        checkConsolidatorMap();
    }

    private void checkConsolidatorMap() {
        Consolidator consolidator = Consolidator.getInstance();
        Field[] fields = consolidator.getClass().getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            if (!"consolidatorMap".equals(field.getName())) {
                continue;
            }
            Map<String, ConsolidatorResult> map = null;
            try {
                map = (Map<String, ConsolidatorResult>) field.get(consolidator);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            Assert.assertTrue("consolidatorMap should empty,but" + JsonUtil.toJSONString(map), map.isEmpty());
        }
    }

    private HikariDataSource buildDataSource(String baseUrl) {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(DRIVER_NAME);
        config.setJdbcUrl(baseUrl);
        config.setMinimumIdle(10);
        config.setMaximumPoolSize(50);
        return new HikariDataSource(config);
    }

    class ConsolidatorTask implements Runnable {
        private final int[] count;

        private final CountDownLatch countDownLatch;

        private final AtomicBoolean atomicBoolean;

        private final int andIncrement;

        private final HikariDataSource dataSource;

        private String slowSql = SLOW_SQL;

        private final boolean isTransaction;

        public ConsolidatorTask(int[] count, CountDownLatch countDownLatch, AtomicBoolean atomicBoolean, int andIncrement, HikariDataSource dataSource, boolean isTransaction) {
            this.count = count;
            this.countDownLatch = countDownLatch;
            this.atomicBoolean = atomicBoolean;
            this.andIncrement = andIncrement;
            this.dataSource = dataSource;
            this.isTransaction = isTransaction;
        }

        public ConsolidatorTask(int[] count, CountDownLatch countDownLatch, AtomicBoolean atomicBoolean, int andIncrement, HikariDataSource dataSource, boolean isTransaction, String slowSql) {
            this(count, countDownLatch, atomicBoolean, andIncrement, dataSource, isTransaction);
            this.slowSql = slowSql;
        }

        @Override
        public void run() {
            long start = System.currentTimeMillis();
            try (Connection connection = dataSource.getConnection();
                 Statement stmt = connection.createStatement()) {
                connection.setAutoCommit(!isTransaction);
                ResultSet resultSet = stmt.executeQuery(slowSql);
                while (resultSet.next()) {
                    count[andIncrement] = resultSet.getInt(1);
                }
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                atomicBoolean.set(false);
            } finally {
                printInfo(String.format("ConsolidatorTest-ThreadName=%s, cost %d ms", Thread.currentThread().getName(), (System.currentTimeMillis() - start)));
                if (countDownLatch != null) {
                    countDownLatch.countDown();
                }
            }
        }
    }
}
