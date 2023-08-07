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

package com.jd.jdbc.engine;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.Random;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.Assert;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;
import static testsuite.internal.TestSuiteShardSpec.TWO_SHARDS;
import testsuite.internal.testcase.TestSuiteCase;

public class InsertEngineTest extends TestSuite {

    protected Connection shardedDriverConnection;

    protected Connection unshardedDriverConnection;

    private List<InsertEngineTestCase> testCaseList;

    @Test
    public void testUnsharded() throws SQLException, IOException {
        beforeUnsharded();
        testInsert(unshardedDriverConnection);
        afterUnsharded();
    }

    @Test
    public void testSharded() throws SQLException, IOException {
        beforeSharded();
        testInsert(shardedDriverConnection);
        afterSharded();
    }

    protected Connection getDriverConnection() throws SQLException {
        return getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
    }

    @Test
    public void testTransactionWithRollback() throws SQLException, IOException {
        Connection shardedDriverConnection = getDriverConnection();

        try (Statement stmt = shardedDriverConnection.createStatement()) {
            stmt.executeUpdate("delete from user_metadata");
        }

        beforeSharded();

        shardedDriverConnection.setAutoCommit(false);
        try (Statement stmt = shardedDriverConnection.createStatement()) {
            String insertSQL = "INSERT INTO user_metadata (user_id,email,address) VALUES ('%d', '%s', '%s') ";
            String insertErrorSQL = "INSERT INTO user_metadata (user_id,email,address_) VALUES(10, 'xxx@xxx', '3')";
            for (int i = 1; i <= 3; i++) {
                try {
                    if (i == 2) {
                        stmt.execute(insertErrorSQL);
                    } else {
                        stmt.execute(String.format(insertSQL, 1, "2", "3"));
                    }
                } catch (Exception e) {
                    System.out.println("one sql failed");
                    shardedDriverConnection.rollback();
                }
            }
            shardedDriverConnection.commit();

            shardedDriverConnection.setAutoCommit(true);
            String selectSQL = "Select id,`email`,address from user_metadata";
            boolean hasSelectResult = stmt.execute(selectSQL);
            if (!hasSelectResult) {
                throw new SQLException("no result set for sql: " + selectSQL);
            }
            ResultSet rs = stmt.getResultSet();
            int rowNum = 0;
            while (rs.next()) {
                String result = "result: %d, %s, %s";
                System.out.println(
                    String.format(
                        result,
                        rs.getInt(1),
                        rs.getString(2),
                        rs.getString(3)));
                rowNum++;
            }
            if (rowNum == 1) {
                printOk();
            } else {
                printFail("Failed: " + selectSQL);
                throw new SQLException();
            }
        } finally {
            shardedDriverConnection.close();
        }

    }

    @Test
    public void testReplaceWithKeyWords() throws SQLException {
        String sql1 =
                "replace INTO `all_type_test`  (`tinyint`, `u_tinyint`, `tinyint_1`, `u_tinyint_1`, `smallint`, `u_smallint`, `mediumint`, `u_mediumint`,`int`, `u_int`, `bigint`, `u_bigint`, `bit_1`, " +
                "`bit_64`, `float`, `u_float`, `double`, `u_double`, `char`,`varchar`,`tinytext`,`text`,`mediumtext`,`longtext`,`json`,`binary`," +
                "`varbinary`,`tinyblob`,`blob`,`mediumblob`,`longblob`,`date`,`time_1`,`time_3`, `year`,`datetime_3`,`datetime_6`,`timestamp_3`,`timestamp_6`) values " +
                "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        try (Connection conn = getConnection(Driver.of(TWO_SHARDS));
             PreparedStatement prepareStatement1 = conn.prepareStatement(sql1)) {
            for (int i = 0; i < 100; i++) {
                prepareStatement1.setInt(1, i);
                prepareStatement1.setInt(2, i);
                prepareStatement1.setInt(3, 0);
                prepareStatement1.setInt(4, 0);
                prepareStatement1.setInt(5, i + 100);
                prepareStatement1.setInt(6, i + 200);
                prepareStatement1.setInt(7, i + 100);
                prepareStatement1.setInt(8, i + 200);
                prepareStatement1.setInt(9, i + 100);
                prepareStatement1.setInt(10, i + 200);

                prepareStatement1.setLong(11, -10000L + i);
                prepareStatement1.setLong(12, 10000L + i);
                prepareStatement1.setBoolean(13, true);
                prepareStatement1.setBoolean(14, false);
                prepareStatement1.setFloat(15, -(float) i);
                prepareStatement1.setFloat(16, (float) i);
                prepareStatement1.setDouble(17, -(double) i);
                prepareStatement1.setDouble(18, i);

                prepareStatement1.setString(19, "a" + i);
                prepareStatement1.setString(20, "String" + i);

                prepareStatement1.setString(21, "tinytext" + i);
                prepareStatement1.setString(22, "text" + i);
                prepareStatement1.setString(23, "mediumtext" + i);
                prepareStatement1.setString(24, "longtext" + i);

                prepareStatement1.setString(25, "{\n" +
                    "    \"BigIntSupported\": 995815895020119800000,\n" +
                    "    \"date\": \"20180322\",\n" +
                    "    \"message\": \"Success !\",\n" +
                    "    \"status\": 200,\n" +
                    "    \"city\": \"北京\",\n" +
                    "    \"count\": 632\n" +
                    "}");

                byte[] bytes1 = new byte[10];
                for (int j = 0; j < bytes1.length; j++) {
                    bytes1[j] = 1;
                }
                prepareStatement1.setBytes(26, bytes1);
                prepareStatement1.setBytes(27, bytes1);
                prepareStatement1.setBytes(28, bytes1);
                prepareStatement1.setBytes(29, bytes1);
                prepareStatement1.setBytes(30, bytes1);
                prepareStatement1.setBytes(31, bytes1);

                prepareStatement1.setDate(32, new Date(System.currentTimeMillis()));
                prepareStatement1.setTimestamp(33, new Timestamp(System.currentTimeMillis()));
                prepareStatement1.setTime(34, new Time(System.currentTimeMillis()));
                prepareStatement1.setInt(35, 2022);
                prepareStatement1.setDate(36, new Date(System.currentTimeMillis()));
                prepareStatement1.setTimestamp(37, new Timestamp(System.currentTimeMillis()));
                prepareStatement1.setTimestamp(38, new Timestamp(System.currentTimeMillis()));
                prepareStatement1.setTimestamp(39, new Timestamp(System.currentTimeMillis()));
                int sql1InsertCount = prepareStatement1.executeUpdate();
                if (sql1InsertCount != 1 && sql1InsertCount != 2) {
                    Assert.fail();
                }
            }
        }
    }

    @Test
    public void testTransactionWithCatch() throws SQLException {
        Connection shardedDriverConnection = getDriverConnection();

        try (Statement stmt = shardedDriverConnection.createStatement()) {
            stmt.execute("delete from user_metadata");
        }

        shardedDriverConnection.setAutoCommit(false);
        try (Statement stmt = shardedDriverConnection.createStatement()) {
            String insertSQL = "INSERT INTO user_metadata (user_id,email,address) VALUES ('%d', '%s', '%s') ";
            for (int i = 1; i <= 10; i++) {
                String sql = String.format(insertSQL, 1, "2", "3");
                stmt.execute(sql);
            }

            String errorSql = "INSERT INTO user_metadata (user_id, email, address_) VALUES (1, 'xxx@xxx', '3') ";
            try {
                stmt.execute(errorSql);
            } catch (Exception e) {
                System.out.println("one sql failed: " + errorSql);
            }
            String sql = String.format(insertSQL, 1, "2", "3");
            stmt.execute(sql);
            shardedDriverConnection.commit();
            String selectSQL = "Select id,`email`,address from user_metadata";
            shardedDriverConnection.setAutoCommit(true);
            boolean hasSelectResult = stmt.execute(selectSQL);
            if (!hasSelectResult) {
                throw new SQLException("no result set for sql: " + selectSQL);
            }
            ResultSet rs = stmt.getResultSet();
            int rowNum = 0;
            while (rs.next()) {
                String result = "result: %d, %s, %s";
                System.out.printf(
                    result + "%n",
                    rs.getInt(1),
                    rs.getString(2),
                    rs.getString(3));
                rowNum++;
            }
            if (rowNum == 11) {
                printOk();
            } else {
                printFail("Failed: " + selectSQL);
                throw new SQLException();
            }
        } finally {
            shardedDriverConnection.close();
        }
    }

    protected String getUrl(boolean isShard) {
        if (isShard) {
            return getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS)) + "&useAffectedRows=false";
        }
        return getConnectionUrl(Driver.of(TestSuiteShardSpec.NO_SHARDS)) + "&useAffectedRows=false";
    }

    protected void beforeUnsharded() throws SQLException, IOException {
        unshardedDriverConnection = DriverManager.getConnection(getUrl(false));
        clearUnshardedTestData();
        initTestCase("src/test/resources/engine/dml_insert_unsharded.json");
    }

    protected void beforeSharded() throws SQLException, IOException {
        shardedDriverConnection = DriverManager.getConnection(getUrl(true));
        clearShardedTestData();
        initTestCase("src/test/resources/engine/dml_insert_sharded.json");
    }

    protected void clearUnshardedTestData() throws SQLException {
        try (Statement stmt = unshardedDriverConnection.createStatement()) {
            stmt.execute("delete from unsharded");
            stmt.execute("delete from unsharded_authoritative");
            stmt.execute("delete from unsharded_auto");
        }
    }

    protected void clearShardedTestData() throws SQLException {
        try (Statement stmt = shardedDriverConnection.createStatement()) {
            stmt.execute("delete from authoritative");
            stmt.execute("delete from user");
        }
    }

    protected void initTestCase(String filename) throws IOException {
        this.testCaseList = iterateExecFile(filename, InsertEngineTestCase.class);
    }

    protected void testInsert(Connection driverConnection) throws SQLException {
        testInsert(driverConnection, false);
    }

    protected void testInsert(Connection driverConnection, boolean useAffectedRows) throws SQLException {
        int num = 0;
        for (InsertEngineTestCase testCase : testCaseList) {
            num++;

            System.out.print("No." + num + " ");

            printComment(testCase.getComment());

            ResultSet rs = null;
            try (Statement stmt = driverConnection.createStatement();
                 PreparedStatement psmt = driverConnection.prepareStatement(testCase.insertSql)) {

                String insertSql = this.replaceNextLong(testCase.insertSql);

                printNormal(insertSql);

                int affectedRows;
                if (testCase.insertVar.isEmpty()) {
                    stmt.execute(insertSql);
                    affectedRows = stmt.getUpdateCount();
                } else {
                    List<Object> insertVar = testCase.insertVar;
                    for (int i = 0; i < insertVar.size(); i++) {
                        Object var = insertVar.get(i);
                        if (var == null) {
                            psmt.setNull(i + 1, Types.VARCHAR);
                        } else {
                            Class<?> clazz = var.getClass();
                            if (clazz == Integer.class) {
                                psmt.setInt(i + 1, (Integer) var);
                            } else if (clazz == String.class) {
                                psmt.setString(i + 1, (String) var);
                            }
                        }
                    }
                    psmt.execute();
                    affectedRows = psmt.getUpdateCount();
                }
                Long expectedRs = useAffectedRows ? testCase.getAffectedRows() : testCase.getUpdateCount();
                printNormal("\t\tupdateCount expected: " + expectedRs + " actual: " + affectedRows);
                Assert.assertEquals(printFail("[Failed]"), expectedRs, Long.valueOf(affectedRows));

                printNormal(testCase.verifySql);
                rs = stmt.executeQuery(testCase.verifySql);
                rs.next();

                for (int i = 0; i < testCase.verifyResult.length; i++) {
                    Object[] expectArray = testCase.verifyResult[i];
                    for (int j = 0; j < expectArray.length; j++) {
                        if (expectArray[j] instanceof Integer) {
                            Assert.assertEquals(printFail("Failed: expected is: " + expectArray[j] + ". but was: " + rs.getInt(j + 1)), expectArray[j], rs.getInt(j + 1));
                        }
                    }
                    rs.next();
                }
                printOk();
                System.out.println();
            } catch (Exception e) {
                e.printStackTrace();
                printNormal("exception:\n\texpected: [" + testCase.getException() + "] " + testCase.getErrorMessage());
                printNormal("\tactual: [" + e.getClass().getName() + "] " + e.getMessage());

                Assert.assertEquals(testCase.getException(), e.getClass().getName());
                Assert.assertTrue(e.getMessage().contains(testCase.getErrorMessage()));
                printOk();
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        }
    }

    protected void afterUnsharded() throws SQLException {
        closeConnection(unshardedDriverConnection);
    }

    protected void afterSharded() throws SQLException {
        closeConnection(shardedDriverConnection);
    }

    private String replaceNextLong(String sql) {
        if (sql.contains(":nextLong")) {
            sql = sql.replace(":nextLong", String.valueOf(new Random().nextLong()));
            if (sql.contains(":nextLong")) {
                replaceNextLong(sql);
            }
        }
        return sql;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class InsertEngineTestCase extends TestSuiteCase {
        private String insertSql;

        private List<Object> insertVar;

        private Long updateCount;

        private Long affectedRows;

        private String verifySql;

        private Object[][] verifyResult;

        private String exception;

        private String errorMessage;
    }
}
