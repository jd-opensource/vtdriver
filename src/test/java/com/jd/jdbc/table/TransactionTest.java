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

import com.jd.jdbc.session.SafeSession;
import com.jd.jdbc.vitess.VitessConnection;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;
import testsuite.internal.testcase.TestSuiteCase;

public class TransactionTest extends TestSuite {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    protected String baseUrl;

    protected List<Connection> connectionList;

    protected List<TransactionTestCase> transactionTestCaseList;

    @Before
    public void testLoadDriver() throws Exception {
        getConn();
    }

    protected void getConn() throws SQLException {
        baseUrl = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS));

        Connection conn_0 = DriverManager.getConnection(baseUrl + "&queryParallelNum=0");
        Connection conn_1 = DriverManager.getConnection(baseUrl + "&queryParallelNum=1");
        Connection conn_8 = DriverManager.getConnection(baseUrl + "&queryParallelNum=2");
        this.connectionList = new ArrayList<>();
        this.connectionList.add(conn_0);
        this.connectionList.add(conn_1);
        this.connectionList.add(conn_8);
    }

    @After
    public void after() throws Exception {
        if (this.connectionList != null) {
            for (Connection conn : this.connectionList) {
                if (conn != null) {
                    conn.close();
                }
            }
        }
        TableTestUtil.setDefaultTableConfig();
    }

    @Test
    public void test01() throws Exception {
        TableTestUtil.setSplitTableConfig("engine/tableengine/split-table_1.yml");
        this.transactionTestCaseList = iterateExecFile("src/test/resources/transaction/transaction/transaction_case.json", TransactionTestCase.class);
        testTx();
    }

    @Test
    public void test02() throws Exception {
        TableTestUtil.setSplitTableConfig("engine/tableengine/split-table_2.yml");
        this.transactionTestCaseList = iterateExecFile("src/test/resources/transaction/transaction/transaction_case.json", TransactionTestCase.class);
        testTx();
    }

    @Test
    public void testCommitWhenAutocommitTrue() throws SQLException {
        for (Connection conn : this.connectionList) {
            conn.setAutoCommit(true);
            expectedEx.expectMessage("Can't call commit when autocommit=true");
            conn.commit();
        }
    }

    @Test
    public void testRollbackWhenAutocommitTrue() throws SQLException {
        for (Connection conn : this.connectionList) {
            conn.setAutoCommit(true);
            expectedEx.expectMessage("Can't call rollback when autocommit=true");
            conn.rollback();
        }
    }

    @Test
    public void testErrorInTransaction() throws Exception {
        TableTestUtil.setSplitTableConfig("engine/tableengine/split-table_1.yml");
        List<List<String>> testCase = new ArrayList<List<String>>() {{
            add(new ArrayList<String>() {{
                add("DELETE FROM table_engine_test;");
                add("INSERT INTO table_engine_test(f_key, f_tinyint, f_int) VALUES ('%s', 1, 1)");
                add("INSERT INTO table_engine_test(f_key, f_tinyint, f_int_) VALUES ('%s', 1, 1)");
                add("SELECT COUNT(*) FROM table_engine_test;");
            }});
        }};
        for (List<String> sqls : testCase) {
            testErrorInTransactionNormal(sqls.get(0), sqls.get(1), sqls.get(2), sqls.get(3));
            testErrorInTransactionExecuteBatch(sqls.get(0), sqls.get(1), sqls.get(2), sqls.get(3));
        }
    }

    public void testErrorInTransactionNormal(String initSql, String sql, String errorSql, String checkSql) throws SQLException {
        for (Connection conn : connectionList) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(initSql);
                conn.setAutoCommit(false);
                for (int i = 0; i < 10; i++) {
                    stmt.executeUpdate(String.format(sql, i));
                }

                try {
                    stmt.executeUpdate(String.format(errorSql, 11));
                } catch (SQLException ignored) {
                }

                ResultSet resultSet = stmt.executeQuery(checkSql);
                resultSet.next();
                Assert.assertEquals(10, resultSet.getInt(1));
                conn.rollback();

                stmt.executeUpdate(String.format(sql, 11));
                conn.commit();
                conn.setAutoCommit(true);

                resultSet = stmt.executeQuery(checkSql);
                resultSet.next();
                Assert.assertEquals(1, resultSet.getInt(1));
            }
        }
    }

    public void testErrorInTransactionExecuteBatch(String initSql, String sql, String errorSql, String checkSql) throws SQLException {
        for (Connection conn : connectionList) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(initSql);

                conn.setAutoCommit(false);
                for (int i = 0; i < 10; i++) {
                    stmt.addBatch(String.format(sql, i));
                }
                stmt.executeBatch();

                try {
                    stmt.addBatch(String.format(sql, 11));
                    stmt.addBatch(String.format(errorSql, 1));
                    stmt.executeBatch();
                } catch (SQLException ignored) {
                }

                ResultSet resultSet = stmt.executeQuery(checkSql);
                resultSet.next();
                Assert.assertEquals(11, resultSet.getInt(1));
                conn.rollback();

                stmt.addBatch(String.format(sql, 1));
                stmt.addBatch(String.format(sql, 2));
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
            for (Connection conn : this.connectionList) {
                int maxParallelNum = SafeSession.newSafeSession((VitessConnection) conn).getMaxParallelNum();
                System.out.println("parallel num:" + maxParallelNum);
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
                    if (testCase.getNeedTransaction()) {
                        conn.rollback();
                        conn.setAutoCommit(true);
                    }
                    Assert.assertTrue(e.getMessage().contains(testCase.getErrorMsg()));
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
