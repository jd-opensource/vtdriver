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

package com.jd.jdbc.vitess.allowMultiQueries;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class VitessDriverAllowMultiQuerieTest extends TestSuite {
    protected static ExecutorService executor = null;

    Connection conn;

    @BeforeClass
    public static void init() {
        executor = getThreadPool(10, 10);
    }

    @AfterClass
    public static void shutdown() {
        executor.shutdown();
    }

    @Before
    public void testLoadDriver() throws Exception {
        conn = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        cleanData();
        prepareData();
    }

    @After
    public void after() throws SQLException {
        closeConnection(conn);
    }

    protected void cleanData() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("delete from auto;");
        }
    }

    protected void prepareData() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            for (int i = 0; i < 100; i++) {
                stmt.addBatch(String.format("insert into auto (id,ai,email) values(%d,%d,'%s')", i, i, "xxx" + i));
            }
            stmt.executeBatch();
            stmt.execute("delete from user");
        }
    }

    private void verifyResult(long expect, int offect, ResultSet resultSet) throws SQLException {
        long idx = offect;
        int size = 0;
        while (resultSet.next()) {
            assertEquals(idx, resultSet.getLong(1));
            idx++;
            size++;
        }
        assertEquals(expect, size);
    }


    @Test
    public void simpleSelect() throws SQLException {
        String sql = "select id from auto where id > 50 order by id;select id from auto where id <= 50 order by id;select id from auto order by id;";
        try (Statement stmt = conn.createStatement()) {
            boolean hasResult = stmt.execute(sql);
            int sqlIdx = 0;
            ResultSet resultSet = null;
            do {
                if (hasResult) {
                    resultSet = stmt.getResultSet();
                    if (sqlIdx == 0) {
                        verifyResult(49, 51, resultSet);
                    } else if (sqlIdx == 1) {
                        verifyResult(51, 0, resultSet);
                    } else if (sqlIdx == 2) {
                        verifyResult(100, 0, resultSet);
                    } else {
                        fail();
                    }
                } else {
                    break;
                }
                sqlIdx++;
                hasResult = stmt.getMoreResults();
                assertTrue(resultSet.isClosed());
            } while (true);
        }
    }

    @Test
    public void groupBy() throws SQLException {
        String sql = "select id from auto where id > 50 group by id order by id;select id from auto where id <= 50 group by id order by id;select id from auto group by id order by id;";
        try (Statement stmt = conn.createStatement()) {
            boolean hasResult = stmt.execute(sql);
            int sqlIdx = 0;
            ResultSet resultSet = null;
            do {
                if (hasResult) {
                    resultSet = stmt.getResultSet();
                    if (sqlIdx == 0) {
                        verifyResult(49, 51, resultSet);
                    } else if (sqlIdx == 1) {
                        verifyResult(51, 0, resultSet);
                    } else if (sqlIdx == 2) {
                        verifyResult(100, 0, resultSet);
                    } else {
                        fail();
                    }
                } else {
                    break;
                }
                sqlIdx++;
                hasResult = stmt.getMoreResults();
                assertTrue(resultSet.isClosed());
            } while (true);
        }
    }

    @Test
    public void groupBy2() throws SQLException {
        String sql = "insert into auto (id,ai,email) values(5,100,'abc'),(6,101,'abc');insert into auto (id,ai,email) values(6,102,'abc'); " +
            "select COUNT(*) from auto where id =5 group by id order by id; " +
            "select COUNT(*) from auto where id =6 group by id order by id; " +
            "select COUNT(*) from auto where id =7 group by id order by id;" +
            "select COUNT(*) from auto group by id having id =5 order by id; " +
            "select COUNT(*) from auto group by id having id =6 order by id; " +
            "select COUNT(*) from auto group by id having id =7 order by id;";
        try (Statement stmt = conn.createStatement()) {
            boolean hasResult = stmt.execute(sql);
            int sqlIdx = 0;
            ResultSet resultSet = null;
            do {
                if (hasResult) {
                    resultSet = stmt.getResultSet();
                    if (sqlIdx == 2) {
                        verifyResult(1, 2, resultSet);
                    } else if (sqlIdx == 3) {
                        verifyResult(1, 3, resultSet);
                    } else if (sqlIdx == 4) {
                        verifyResult(1, 1, resultSet);
                    } else if (sqlIdx == 5) {
                        verifyResult(1, 2, resultSet);
                    } else if (sqlIdx == 6) {
                        verifyResult(1, 3, resultSet);
                    } else if (sqlIdx == 7) {
                        verifyResult(1, 1, resultSet);
                    } else {
                        fail();
                    }
                } else {
                    if (sqlIdx == 0) {
                        assertEquals(2, stmt.getUpdateCount());
                    } else if (sqlIdx == 1) {
                        assertEquals(1, stmt.getUpdateCount());
                    } else if (sqlIdx == 8) {
                        assertEquals(-1, stmt.getUpdateCount());
                        break;
                    } else {
                        fail();
                    }
                }
                sqlIdx++;
                hasResult = stmt.getMoreResults();
                if (resultSet != null) {
                    assertTrue(resultSet.isClosed());
                }
            } while (true);
        }
    }

    @Test
    public void groupBy3() throws SQLException {
        String sql = "select id,email,count(*) from auto group by id,email order by id;" +
            "delete from auto;" +
            "insert into auto (id,ai,email) values(100,100,'abc'),(100,101,'abc');insert into auto (id,ai,email) values(100,102,'abc');" +
            "select id,email,count(*) from auto group by id,email order by id;";
        try (Statement stmt = conn.createStatement()) {
            boolean hasResult = stmt.execute(sql);
            int sqlIdx = 0;
            ResultSet resultSet = null;
            do {
                if (hasResult) {
                    resultSet = stmt.getResultSet();
                    if (sqlIdx == 0) {
                        verifyResult(100, 0, resultSet);
                    } else if (sqlIdx == 4) {
                        verifyResult(1, 100, resultSet);
                    } else {
                        fail();
                    }
                } else {
                    if (sqlIdx == 1) {
                        assertEquals(100, stmt.getUpdateCount());
                    } else if (sqlIdx == 2) {
                        assertEquals(2, stmt.getUpdateCount());
                    } else if (sqlIdx == 3) {
                        assertEquals(1, stmt.getUpdateCount());
                    } else if (sqlIdx == 5) {
                        assertEquals(-1, stmt.getUpdateCount());
                        break;
                    } else {
                        fail();
                    }
                }
                sqlIdx++;
                hasResult = stmt.getMoreResults();
                if (resultSet != null) {
                    assertTrue(resultSet.isClosed());
                }
            } while (true);
        }
    }

    @Test
    public void groupBy4() throws SQLException {
        String sql = "delete from plan_test;" +
            "insert into plan_test(f_tinyint, f_int, f_smallint) VALUES (1, 1, 2),(2, 1, 2),(3, 1, 2),(4, 1, 2),(5, 1, 2),(6, 1, 2),(7, 1, 2),(8, 1, 2),(9, 1, 2),(10, 1, 2);" +
            "select f_int, f_smallint, count(*) from plan_test group by f_int order by f_smallint;" +
            "delete from plan_test where f_tinyint >=1 and f_tinyint <=10;";
        try (Statement stmt = conn.createStatement()) {
            boolean hasResult = stmt.execute(sql);
            int sqlIdx = 0;
            ResultSet resultSet = null;
            do {
                if (hasResult) {
                    resultSet = stmt.getResultSet();
                    if (sqlIdx == 2) {
                        verifyResult(1, 1, resultSet);
                    } else {
                        fail();
                    }
                } else {
                    if (sqlIdx == 0) {

                    } else if (sqlIdx == 1) {
                        assertEquals(10, stmt.getUpdateCount());
                    } else if (sqlIdx == 3) {
                        assertEquals(10, stmt.getUpdateCount());
                    } else if (sqlIdx == 4) {
                        assertEquals(-1, stmt.getUpdateCount());
                        break;
                    } else {
                        fail();
                    }
                }
                sqlIdx++;
                hasResult = stmt.getMoreResults();
                if (resultSet != null) {
                    assertTrue(resultSet.isClosed());
                }
            } while (true);
        }
    }

    @Test
    public void limit() throws SQLException {
        String sql = "select id from auto where id > 50 order by id limit 10;" +
            "select id from auto where id > 50 order by id limit 20,10;" +
            "select id from auto where id > 50 order by id limit 10 offset 20;";
        try (Statement stmt = conn.createStatement()) {
            boolean hasResult = stmt.execute(sql);
            int sqlIdx = 0;
            ResultSet resultSet = null;
            do {
                if (hasResult) {
                    resultSet = stmt.getResultSet();
                    if (sqlIdx == 0) {
                        verifyResult(10, 51, resultSet);
                    } else if (sqlIdx == 1) {
                        verifyResult(10, 71, resultSet);
                    } else if (sqlIdx == 2) {
                        verifyResult(10, 71, resultSet);
                    } else {
                        fail();
                    }
                } else {
                    break;
                }
                sqlIdx++;
                hasResult = stmt.getMoreResults();
                assertTrue(resultSet.isClosed());
            } while (true);
        }
    }

    @Test
    public void selectNull() throws SQLException {
        String sql = "select id from auto where id in(1,2,3,4,5) order by id limit 10;select id from auto where id is null;select id from auto where id is not null order by id;";
        try (Statement stmt = conn.createStatement()) {
            boolean hasResult = stmt.execute(sql);
            int sqlIdx = 0;
            ResultSet resultSet = null;
            do {
                if (hasResult) {
                    resultSet = stmt.getResultSet();
                    if (sqlIdx == 0) {
                        verifyResult(5, 1, resultSet);
                    } else if (sqlIdx == 1) {
                        verifyResult(0, 0, resultSet);
                    } else if (sqlIdx == 2) {
                        verifyResult(100, 0, resultSet);
                    } else {
                        fail();
                    }
                } else {
                    break;
                }
                sqlIdx++;
                hasResult = stmt.getMoreResults();
                assertTrue(resultSet.isClosed());
            } while (true);
        }
    }

    @Test
    public void aggregate() throws SQLException {
        String sql = "select count(id) from auto;select max(id) from auto;select min(id) from auto;select sum(id) from auto;" +
            "insert into auto (id,ai,email) values(100,100,'abc'),(101,101,'abc');insert into auto (id,ai,email) values(102,102,'abc'); " +
            "select count(id) from auto;select max(id) from auto;select min(id) from auto;select sum(id) from auto";
        try (Statement stmt = conn.createStatement()) {
            boolean hasResult = stmt.execute(sql);
            int sqlIdx = 0;
            ResultSet resultSet = null;
            do {
                if (hasResult) {
                    resultSet = stmt.getResultSet();
                    if (sqlIdx == 0) {
                        verifyResult(1, 100, resultSet);
                    } else if (sqlIdx == 1) {
                        verifyResult(1, 99, resultSet);
                    } else if (sqlIdx == 2) {
                        verifyResult(1, 0, resultSet);
                    } else if (sqlIdx == 3) {
                        verifyResult(1, 5050 - 100, resultSet);
                    } else if (sqlIdx == 6) {
                        verifyResult(1, 103, resultSet);
                    } else if (sqlIdx == 7) {
                        verifyResult(1, 102, resultSet);
                    } else if (sqlIdx == 8) {
                        verifyResult(1, 0, resultSet);
                    } else if (sqlIdx == 9) {
                        verifyResult(1, 5050 + 101 + 102, resultSet);
                    } else {
                        fail();
                    }
                } else {
                    if (sqlIdx == 4) {
                        assertEquals(2, stmt.getUpdateCount());
                    } else if (sqlIdx == 5) {
                        assertEquals(1, stmt.getUpdateCount());
                    } else if (sqlIdx == 10) {
                        assertEquals(-1, stmt.getUpdateCount());
                        break;
                    } else {
                        fail();
                    }
                }
                sqlIdx++;
                hasResult = stmt.getMoreResults();
                if (resultSet != null) {
                    assertTrue(resultSet.isClosed());
                }
            } while (true);
        }
    }

    @Test
    public void distinct() throws SQLException {
        String sql = "select distinct id from auto order by id;" +
            "insert into auto (id,ai,email) values(1,100,'abc'),(2,101,'abc');insert into auto (id,ai,email) values(3,102,'abc'); " +
            "select distinct id from auto order by id;select count(1) from auto where id in(1,2,3)";
        try (Statement stmt = conn.createStatement()) {
            boolean hasResult = stmt.execute(sql);
            int sqlIdx = 0;
            ResultSet resultSet = null;
            do {
                if (hasResult) {
                    resultSet = stmt.getResultSet();
                    if (sqlIdx == 0) {
                        verifyResult(100, 0, resultSet);
                    } else if (sqlIdx == 3) {
                        verifyResult(100, 0, resultSet);
                    } else if (sqlIdx == 4) {
                        verifyResult(1, 6, resultSet);
                    } else {
                        fail();
                    }
                } else {
                    if (sqlIdx == 1) {
                        assertEquals(2, stmt.getUpdateCount());
                    } else if (sqlIdx == 2) {
                        assertEquals(1, stmt.getUpdateCount());
                    } else if (sqlIdx == 5) {
                        assertEquals(-1, stmt.getUpdateCount());
                        break;
                    } else {
                        fail();
                    }
                }
                sqlIdx++;
                hasResult = stmt.getMoreResults();
                if (resultSet != null) {
                    assertTrue(resultSet.isClosed());
                }
            } while (true);
        }
    }

    @Test
    public void updateCompare() throws Exception {
        List<Exception> exceptions = new CopyOnWriteArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(2);
        executor.execute(() -> {
            try {
                long start = System.currentTimeMillis();
                String sql = "update auto set email = ? where id = ?";
                for (int i = 0; i < 50; i++) {
                    PreparedStatement preparedStatement = null;
                    try {
                        preparedStatement = conn.prepareStatement(sql);
                        preparedStatement.setString(1, i + "normal-update.com");
                        preparedStatement.setInt(2, i);
                        preparedStatement.execute();
                    } catch (Exception e) {
                        exceptions.add(e);
                    } finally {
                        try {
                            preparedStatement.close();
                        } catch (Exception e) {

                        }
                    }
                }
                printOk("normal-update use:" + (System.currentTimeMillis() - start));
            } finally {
                countDownLatch.countDown();
            }
        });

        executor.execute(() -> {
            try {
                long start = System.currentTimeMillis();
                StringBuilder stringBuilder = new StringBuilder();
                String formatSql = "update auto set email = '%s' where id = %s;";
                for (int i = 50; i < 100; i++) {
                    String oneSql = String.format(formatSql, i + "multiQuery.com", i);
                    stringBuilder.append(oneSql);
                }
                try (PreparedStatement preparedStatement = conn.prepareStatement(stringBuilder.toString())) {
                    boolean hasResult = preparedStatement.execute();
                    int sqlIdx = 0;
                    do {
                        if (hasResult) {
                            fail();
                        } else {
                            if (sqlIdx < 50) {
                                assertEquals(1, preparedStatement.getUpdateCount());
                            } else if (sqlIdx == 50) {
                                break;
                            } else {
                                fail();
                            }
                        }
                        sqlIdx++;
                    } while (true);
                } catch (Exception e) {
                    exceptions.add(e);
                }
                printOk("multiQuery-update use:" + (System.currentTimeMillis() - start));
            } finally {
                countDownLatch.countDown();
            }
        });
        countDownLatch.await();
        if (!exceptions.isEmpty()) {
            fail();
        }
    }

    @Test
    public void insertCompare() throws Exception {
        List<Exception> exceptions = new CopyOnWriteArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(2);
        executor.execute(() -> {
            try {
                long start = System.currentTimeMillis();
                String sql = "insert into auto (id,email) values(?,?)";
                for (int i = 0; i < 50; i++) {
                    PreparedStatement preparedStatement = null;
                    try {
                        preparedStatement = conn.prepareStatement(sql);
                        preparedStatement.setInt(1, i + 1000);
                        preparedStatement.setString(2, i + "normal-insert.com");
                        preparedStatement.execute();
                    } catch (Exception e) {
                        exceptions.add(e);
                    } finally {
                        try {
                            preparedStatement.close();
                        } catch (Exception e) {

                        }
                    }
                }
                printOk("normal-insert use:" + (System.currentTimeMillis() - start));
            } finally {
                countDownLatch.countDown();
            }
        });

        executor.execute(() -> {
            try {
                long start = System.currentTimeMillis();
                StringBuilder stringBuilder = new StringBuilder();
                String formatSql = "insert into auto (id,email) values(%s,'%s');";
                for (int i = 0; i < 50; i++) {
                    String oneSql = String.format(formatSql, i + 2000, i + "multiQuery.com");
                    stringBuilder.append(oneSql);
                }
                try (PreparedStatement preparedStatement = conn.prepareStatement(stringBuilder.toString())) {
                    boolean hasResult = preparedStatement.execute();
                    int sqlIdx = 0;
                    do {
                        if (hasResult) {
                            fail();
                        } else {
                            if (sqlIdx < 500) {
                                assertEquals(1, preparedStatement.getUpdateCount());
                            } else if (sqlIdx == 500) {
                                break;
                            } else {
                                fail();
                            }
                        }
                        sqlIdx++;
                    } while (true);
                } catch (Exception e) {
                    exceptions.add(e);
                }
                printOk("multiQuery-insert use:" + (System.currentTimeMillis() - start));
            } finally {
                countDownLatch.countDown();
            }
        });
        countDownLatch.await();
        if (!exceptions.isEmpty()) {
            fail();
        }
    }

    @Test
    public void updateSelect() throws SQLException {
        StringBuilder stringBuilder = new StringBuilder();
        String formatSql = "update auto set email = '%s' where id = %s;";
        for (int i = 0; i < 100; i++) {
            String oneSql = String.format(formatSql, "1000", i);
            stringBuilder.append(oneSql);
        }
        stringBuilder.append("select count(*) from auto where email = '1000';");
        try (PreparedStatement preparedStatement = conn.prepareStatement(stringBuilder.toString())) {
            boolean hasResult = preparedStatement.execute();
            int sqlIdx = 0;
            ResultSet resultSet = null;
            do {
                if (hasResult) {
                    resultSet = preparedStatement.getResultSet();
                    if (sqlIdx == 100) {
                        verifyResult(1, 100, resultSet);
                    } else {
                        fail();
                    }
                } else {
                    if (sqlIdx < 100) {
                        assertEquals(1, preparedStatement.getUpdateCount());
                    } else if (sqlIdx == 101) {
                        break;
                    } else {
                        fail();
                    }
                }
                sqlIdx++;
                hasResult = preparedStatement.getMoreResults();
                if (resultSet != null) {
                    assertTrue(resultSet.isClosed());
                }
            } while (true);
        }
    }

    @Test
    public void updateSelectTx() throws SQLException {
        List<Exception> exceptions = new CopyOnWriteArrayList<>();
        StringBuilder stringBuilder = new StringBuilder();
        String formatSql = "update auto set email = '%s' where id = %s;";
        for (int i = 0; i < 100; i++) {
            String oneSql = String.format(formatSql, "10000", i);
            stringBuilder.append(oneSql);
        }
        stringBuilder.append("select count(*) from auto where email = '10000';");
        conn.setAutoCommit(false);
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(stringBuilder.toString());
            boolean hasResult = preparedStatement.execute();
//			int b = 1/0;
            conn.commit();
            int sqlIdx = 0;
            ResultSet resultSet = null;
            do {
                if (hasResult) {
                    resultSet = preparedStatement.getResultSet();
                    if (sqlIdx == 100) {
                        verifyResult(1, 100, resultSet);
                    } else {
                        fail();
                    }
                } else {
                    if (sqlIdx < 100) {
                        assertEquals(1, preparedStatement.getUpdateCount());
                    } else if (sqlIdx == 101) {
                        break;
                    } else {
                        fail();
                    }
                }
                sqlIdx++;
                hasResult = preparedStatement.getMoreResults();
                if (resultSet != null) {
                    assertTrue(resultSet.isClosed());
                }
            } while (true);

        } catch (Exception e) {
            exceptions.add(e);
            conn.rollback();
        }
        if (!exceptions.isEmpty()) {
            fail();
        }
    }

    @Test
    public void rollback() throws SQLException, InterruptedException {
        StringBuilder stringBuilder = new StringBuilder();
        String formatSql = "update auto set email = '%s' where id = %s;";
        for (int i = 0; i < 2; i++) {
            String oneSql = String.format(formatSql, "10000", i);
            stringBuilder.append(oneSql);
        }
        // error sql
//		stringBuilder.append("abc;");
        stringBuilder.append("select a from auto;");
//		stringBuilder.append("select a from a;");
        conn.setAutoCommit(false);
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(stringBuilder.toString());
            boolean hasResult = preparedStatement.execute();
            conn.commit();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Unknown column 'a' in 'field list'"));
            conn.rollback();
        }


        executor.execute(() -> {
            for (int i = 0; i < 10; i++) {
                try (Statement statement = conn.createStatement()) {
                    ResultSet resultSet = statement.executeQuery("select count(*) from auto where email = '10000';");
                    while (resultSet.next()) {
                        assertEquals(0, resultSet.getLong(1));
                    }
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (Exception e) {

                }
            }
        });

        TimeUnit.SECONDS.sleep(10);
    }

    @Test
    public void rollback2() throws SQLException {
        conn.setAutoCommit(false);
        try {
            String sql = "update auto set email = ? where id = ?";
            for (int i = 0; i < 2; i++) {
                PreparedStatement preparedStatement = conn.prepareStatement(sql);
                preparedStatement.setString(1, "10000");
                preparedStatement.setInt(2, i);
                preparedStatement.execute();
            }
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery("select a from auto;");
            conn.commit();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("Unknown column 'a' in 'field list'"));
            try {
                conn.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }

        try (Statement statement = conn.createStatement()) {
            ResultSet resultSet = null;
            try {
                resultSet = statement.executeQuery("select count(*) from auto where email = '10000';");
                conn.commit();
            } catch (Exception e) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }

            while (resultSet.next()) {
                assertEquals(0, resultSet.getLong(1));
            }
        }
    }

    @Test
    public void tx() throws SQLException {
        conn.setAutoCommit(false);
        try {
            String sql = "update auto set email = ? where id = ?";
            for (int i = 0; i < 100; i++) {
                PreparedStatement preparedStatement = null;
                try {
                    preparedStatement = conn.prepareStatement(sql);
                    preparedStatement.setString(1, "normal-update.com");
                    preparedStatement.setInt(2, i);
                    preparedStatement.execute();
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                } finally {
                    try {
                        preparedStatement.close();
                    } catch (Exception e) {

                    }
                }
            }
            Statement statement = conn.createStatement();
            conn.commit();
        } catch (Exception e) {
            conn.rollback();
        }

        try (Statement statement = conn.createStatement()) {
            ResultSet resultSet = statement.executeQuery("select count(*) from auto where email = 'normal-update.com';");
            while (resultSet.next()) {
                assertEquals(100, resultSet.getLong(1));
            }
        }
    }

    @Test
    public void doubleInsert() throws Exception {
        long start = System.currentTimeMillis();
        StringBuilder stringBuilder = new StringBuilder();
        String formatSql = "insert into auto (id,email) values(%s,'%s');";
        for (int i = 0; i < 50; i++) {
            String oneSql = String.format(formatSql, i + 2000, i + "multiQuery.com");
            stringBuilder.append(oneSql);
        }
        conn.setAutoCommit(false);
        try (PreparedStatement preparedStatement = conn.prepareStatement(stringBuilder.toString())) {
            boolean hasResult = preparedStatement.execute();
            int sqlIdx = 0;
            do {
                if (hasResult) {
                    fail();
                } else {
                    if (sqlIdx < 500) {
                        assertEquals(1, preparedStatement.getUpdateCount());
                    } else if (sqlIdx == 500) {
                        break;
                    } else {
                        fail();
                    }
                }
                sqlIdx++;
            } while (true);
        }

        stringBuilder = new StringBuilder();
        for (int i = 50; i < 100; i++) {
            String oneSql = String.format(formatSql, i + 2000, i + "multiQuery.com");
            stringBuilder.append(oneSql);
        }
        try (PreparedStatement preparedStatement = conn.prepareStatement(stringBuilder.toString())) {
            boolean hasResult = preparedStatement.execute();
            int sqlIdx = 0;
            do {
                if (hasResult) {
                    fail();
                } else {
                    if (sqlIdx < 500) {
                        assertEquals(1, preparedStatement.getUpdateCount());
                    } else if (sqlIdx == 500) {
                        break;
                    } else {
                        fail();
                    }
                }
                sqlIdx++;
            } while (true);
        }
        printOk("double-insert use:" + (System.currentTimeMillis() - start));
        conn.commit();
        conn.setAutoCommit(true);

        try (Statement statement = conn.createStatement()) {
            ResultSet resultSet = statement.executeQuery("select count(*) from auto where id >=2000 ;");
            resultSet.next();
            assertEquals(100, resultSet.getLong(1));
        }
    }

    @Test
    public void doubleInsert0() throws Exception {
        long start = System.currentTimeMillis();
        StringBuilder stringBuilder = new StringBuilder();
        String formatSql = "insert into auto (id,email) values(%s,'%s');";
        for (int i = 0; i < 50; i++) {
            String oneSql = String.format(formatSql, i + 2000, i + "multiQuery.com");
            stringBuilder.append(oneSql);
        }
        conn.setAutoCommit(false);
        try (PreparedStatement preparedStatement = conn.prepareStatement(stringBuilder.toString())) {
            boolean hasResult = preparedStatement.execute();
            int sqlIdx = 0;
            do {
                if (hasResult) {
                    fail();
                } else {
                    if (sqlIdx < 500) {
                        assertEquals(1, preparedStatement.getUpdateCount());
                    } else if (sqlIdx == 500) {
                        break;
                    } else {
                        fail();
                    }
                }
                sqlIdx++;
            } while (true);
        }

        try (Statement statement = conn.createStatement()) {
            ResultSet resultSet = statement.executeQuery("select 1;select a from auto;");
            conn.commit();
            conn.setAutoCommit(true);
        } catch (Exception e) {
            e.printStackTrace();
            conn.rollback();
            conn.setAutoCommit(true);
        }

        try (Statement statement = conn.createStatement()) {
            ResultSet resultSet = statement.executeQuery("select count(*) from auto where id >=2000 ;");
            resultSet.next();
            assertEquals(0, resultSet.getLong(1));
        }
    }

    @Test
    public void testTransactionWithRollback() throws SQLException, IOException {
        conn.setAutoCommit(false);
        try (Statement stmt = conn.createStatement()) {
            String insertSQL = "select 1;INSERT INTO user (name,textcol1,textcol2) VALUES ('%s', '%s', '%s') ";
            String insertErrorSQL = "select 1;INSERT INTO user (name,textcol1,textcol2_) VALUES('1','1','1')";
            for (int i = 1; i <= 3; i++) {
                try {
                    if (i == 2) {
                        stmt.executeUpdate(insertErrorSQL);
                    } else {
                        stmt.executeUpdate(String.format(insertSQL, "1", "2", "3"));
                    }
                } catch (Exception e) {
                    System.out.println("one sql failed");
                    conn.rollback();
                }
            }
            conn.commit();

            conn.setAutoCommit(true);
            String selectSQL = "Select id,`name`,textcol1,textcol2 from user";
            ResultSet rs = stmt.executeQuery(selectSQL);
            int rowNum = 0;
            while (rs.next()) {
                String result = "result: %d, %s, %s, %s";
                System.out.println(
                    String.format(
                        result,
                        rs.getInt(1),
                        rs.getString(2),
                        rs.getString(3),
                        rs.getString(4)));
                rowNum++;
            }
            if (rowNum == 1) {
                printOk();
            } else {
                printFail("Failed");
                throw new SQLException();
            }
        }
    }

    @Test
    public void testTransactionWithCatch() throws SQLException, IOException {
        conn.setAutoCommit(false);
        try (Statement stmt = conn.createStatement()) {
            String insertSQL = "select 1;INSERT INTO user (name,textcol1,textcol2) VALUES ('%s', '%s', '%s') ";
            for (int i = 1; i <= 10; i++) {
                String sql = String.format(insertSQL, "1", "2", "3");
                stmt.executeUpdate(sql);
            }

            String errorSql = String.format("select 1;INSERT INTO user (id, name, textcol1_) VALUES ('1', '2', '3')");
            try {
                stmt.executeUpdate(errorSql);
            } catch (Exception e) {
                System.out.println("one sql failed: " + errorSql);
            }
            String sql = String.format(insertSQL, "1", "2", "3");
            stmt.executeUpdate(sql);
            conn.commit();
            String selectSQL = "Select id,`name`,textcol1,textcol2 from user";
            conn.setAutoCommit(true);
            ResultSet rs = stmt.executeQuery(selectSQL);
            int rowNum = 0;
            while (rs.next()) {
                String result = "result: %d, %s, %s, %s";
                System.out.println(
                    String.format(
                        result,
                        rs.getInt(1),
                        rs.getString(2),
                        rs.getString(3),
                        rs.getString(4)));
                rowNum++;
            }
            if (rowNum == 11) {
                printOk();
            } else {
                printFail("Failed");
                throw new SQLException();
            }
        }
    }
}