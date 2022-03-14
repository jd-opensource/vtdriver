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

import com.jd.jdbc.table.TableTestUtil;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;
import testsuite.internal.testcase.TestSuiteCase;

public class TransactionTest extends TestSuite {
    Connection conn;

    private List<TransactionTestCase> transactionTestCaseList;

    @Before
    public void testLoadDriver() throws Exception {
        conn = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
    }

    public List<Connection> getExecuteBatchConnection() throws SQLException {
        List<Connection> connectionList = new ArrayList<>();
        connectionList.add(this.conn);
        connectionList.add(DriverManager.getConnection(getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS)) + "&rewriteBatchedStatements=true"));
        return connectionList;
    }

    @After
    public void after() throws Exception {
        closeConnection(conn);
        TableTestUtil.setDefaultTableConfig();
    }

    @Test
    public void test() throws SQLException, IOException {
        this.transactionTestCaseList = iterateExecFile("src/test/resources/transaction/transaction.json", TransactionTestCase.class);
        testTx();
    }

    public List<List<String>> ErrorInTransactionTestCase() {
        List<List<String>> testCase = new ArrayList<List<String>>() {{
            add(new ArrayList<String>() {{
                add("DELETE FROM user;");
                add("INSERT INTO user (name, textcol1, textcol2) VALUES ('%s', '1', '1');");
                add("INSERT INTO user (name, textcol1, textcol2_) VALUES ('%s', '1', '1');");
                add("SELECT COUNT(*) FROM user;");
            }});
            add(new ArrayList<String>() {{
                add("DELETE FROM plan_test;");
                add("INSERT INTO plan_test (f_tinyint, f_int, f_midint) VALUES (%s, 1, 1);");
                add("INSERT INTO plan_test (f_tinyint, f_int, f_midint_) VALUES (%s, 1, 1);");
                add("SELECT COUNT(*) FROM plan_test;");
            }});
        }};
        return testCase;
    }

    @Test
    public void testErrorInTransaction() throws SQLException {
        for (List<String> sqls : ErrorInTransactionTestCase()) {
            testErrorInTransactionNormal(sqls.get(0), sqls.get(1), sqls.get(2), sqls.get(3));
            testErrorInTransactionExecuteBatch(getExecuteBatchConnection(), sqls.get(0), sqls.get(1), sqls.get(2), sqls.get(3));
            testErrorInTransactionMultiQuery(sqls.get(0), sqls.get(1), sqls.get(2), sqls.get(3));
        }
    }

    public void testErrorInTransactionNormal(String initSQL, String sql, String errorSql, String checkSql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(initSQL);
            conn.setAutoCommit(false);
            for (int i = 0; i < 10; i++) {
                stmt.executeUpdate(String.format(sql, i));
            }
            // error sql
            try {
                stmt.executeUpdate(String.format(errorSql, 1));
            } catch (SQLException ignored) {
            }
            ResultSet resultSet = stmt.executeQuery(checkSql);
            resultSet.next();
            Assert.assertEquals(10, resultSet.getInt(1));
            conn.rollback();

            stmt.executeUpdate(String.format(sql, 1));
            conn.commit();
            conn.setAutoCommit(true);

            resultSet = stmt.executeQuery(checkSql);
            resultSet.next();
            Assert.assertEquals(1, resultSet.getInt(1));
        }
    }

    public void testErrorInTransactionMultiQuery(String initSql, String sql, String errorSql, String checkSql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(initSql);

            conn.setAutoCommit(false);
            for (int i = 0; i < 10; i++) {
                stmt.executeUpdate(String.format(sql + sql, i, i * 10 + 100));
            }
            try {
                stmt.executeUpdate(String.format(errorSql + errorSql, 21, 22));
            } catch (SQLException ignored) {
            }
            ResultSet resultSet = stmt.executeQuery(checkSql);
            resultSet.next();
            Assert.assertEquals(20, resultSet.getInt(1));
            conn.rollback();

            stmt.executeUpdate(String.format(sql + sql, 21, 22));
            conn.commit();
            conn.setAutoCommit(true);

            resultSet = stmt.executeQuery(checkSql);
            resultSet.next();
            Assert.assertEquals(2, resultSet.getInt(1));
        }
    }

    public void testErrorInTransactionExecuteBatch(List<Connection> connList, String initSql, String sql, String errorSql, String checkSql) throws SQLException {
        for (Connection conn : connList) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(initSql);
                conn.setAutoCommit(false);

                for (int i = 0; i < 10; i++) {
                    stmt.addBatch(String.format(sql, i));
                }
                stmt.executeBatch();
                try {
                    stmt.addBatch(String.format(sql, 11));
                    stmt.addBatch(String.format(errorSql, 12));
                    stmt.executeBatch();
                } catch (SQLException ignored) {
                }
                ResultSet resultSet = stmt.executeQuery(checkSql);
                resultSet.next();
                Assert.assertEquals(11, resultSet.getInt(1));
                conn.rollback();

                stmt.addBatch(String.format(sql, 13));
                stmt.addBatch(String.format(sql, 14));
                stmt.executeBatch();
                conn.commit();
                conn.setAutoCommit(true);

                resultSet = stmt.executeQuery(checkSql);
                resultSet.next();
                Assert.assertEquals(2, resultSet.getInt(1));
            }
        }
    }

    private void testTx() throws SQLException {
        for (int index = 0; index < transactionTestCaseList.size(); index++) {
            TransactionTestCase testCase = transactionTestCaseList.get(index);
            try (Statement stmt = conn.createStatement()) {
                for (String init : testCase.initSql) {
                    stmt.executeUpdate(init);
                }
            }
            printComment(testCase.getComment());
            if (testCase.getNeedTransaction()) {
                conn.setAutoCommit(false);
            }
            try (Statement stmt = conn.createStatement()) {
                for (String executeSql : testCase.executeSqls) {
                    printNormal("No." + (index + 1) + " " + executeSql);
                    stmt.executeUpdate(executeSql);
                }
                if (testCase.getNeedTransaction()) {
                    conn.commit();
                    conn.setAutoCommit(true);
                }
            } catch (Exception e) {
                Assert.assertTrue(printFail("[Failed]"), e.getMessage().contains(testCase.errorMsg));
                if (testCase.getNeedTransaction()) {
                    conn.rollback();
                    conn.setAutoCommit(true);
                }
            }
            try (Statement stmt = conn.createStatement()) {
                for (String sql : testCase.verfiySql) {
                    ResultSet rs = stmt.executeQuery(sql);
                    while (rs.next()) {
                        for (int i = 0; i < testCase.verfiyResult.length; i++) {
                            Object[] expectArray = testCase.verfiyResult[i];
                            for (int j = 0; j < expectArray.length; j++) {
                                if (expectArray[j] instanceof Integer) {
                                    Assert.assertEquals(printFail("Failed"), expectArray[j], rs.getInt(j + 1));
                                }
                            }
                        }
                    }
                }
                printOk("No." + (index + 1) + " [Successed] \n");
            }
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TransactionTestCase extends TestSuiteCase {
        private List<String> initSql;

        private Boolean needTransaction;

        private List<String> executeSqls;

        private Object[][] verfiyResult;

        private List<String> verfiySql;

        private String errorMsg;
    }
}