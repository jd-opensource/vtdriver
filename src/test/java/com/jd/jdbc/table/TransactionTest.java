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
