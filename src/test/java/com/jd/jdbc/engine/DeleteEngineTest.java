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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.Assert;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;
import testsuite.internal.testcase.TestSuiteCase;

public class DeleteEngineTest extends TestSuite {

    Connection shardedConnection;

    Connection unshardedConnection;

    List<TestCase> testCaseList = new ArrayList<>();

    Integer number;

    @Test
    public void deleteEngineShardedTest() throws SQLException, IOException {
        shardedInit();
        testEngine(shardedConnection);
    }

    @Test
    public void deleteEngineUnshardedTest() throws SQLException, IOException {
        unshardedInit();
        testEngine(unshardedConnection);
    }

    public void shardedInit() throws IOException, SQLException {
        shardedConnection = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));

        testCaseList.clear();
        testCaseList.addAll(iterateExecFile("src/test/resources/engine/dml_delete_sharded.json", TestCase.class));
    }

    public void unshardedInit() throws SQLException, IOException {
        unshardedConnection = getConnection(Driver.of(TestSuiteShardSpec.NO_SHARDS));

        testCaseList.clear();
        testCaseList.addAll(iterateExecFile("src/test/resources/engine/dml_delete_unsharded.json", TestCase.class));
    }

    public void testEngine(Connection conn) throws SQLException {

        replaceDefaultKeyspace(conn, testCaseList);
        number = 0;
        for (TestCase testCase : testCaseList) {
            number++;
            printCaseInfo(testCase);

            try (Statement stmt = conn.createStatement()) {
                for (String initSql : testCase.getInitSql()) {
                    stmt.execute(initSql);
                }

                int affectedRows = stmt.executeUpdate(testCase.getDeleteSql());
                printNormal("\t\tdeleteUpdateCount expected: " + testCase.getDeleteUpdateCount() + " actual: " + affectedRows);
                Assert.assertEquals(printFail("[Failed]"), testCase.getDeleteUpdateCount(), Long.valueOf(affectedRows));

                printNormal("verifySql: ");
                for (int i = 0; i < testCase.getVerifySql().length; i++) {
                    String verifySql = testCase.getVerifySql()[i];
                    printNormal("\t" + verifySql);
                    boolean flag = stmt.execute(verifySql);
                    Assert.assertTrue(flag);
                    ResultSet rs = stmt.getResultSet();
                    Assert.assertTrue(rs.next());

                    Long expected = testCase.getVerifyResult()[i];
                    long actual = rs.getLong("count(*)");
                    printNormal("\t\tverifyResult expected: " + expected + " actual: " + actual);

                    Assert.assertEquals(printFail("[Failed]"), testCase.getVerifyResult()[i], Long.valueOf(actual));
                }
                printOk();

            } catch (Exception e) {
                printException(e, testCase);
                Assert.assertEquals(printFail("[Failed]"), testCase.getException(), e.getClass().getName());
                Assert.assertTrue(printFail("[Failed]"), e.getMessage().contains(testCase.getErrorMessage()));
                printOk();
            }
        }
    }

    public void printException(Exception e, TestCase testCase) {
        printNormal("exception:\n\texpected: [" + testCase.getException() + "] " + testCase.getErrorMessage());
        printNormal("\tactual: [" + e.getClass().getName() + "] " + e.getMessage());
    }

    public void replaceDefaultKeyspace(Connection conn, List<TestCase> testCaseList) throws SQLException {
        String defaultKeyspace = conn.getCatalog();
        for (TestCase testCase : testCaseList) {
            testCase.setDeleteSql(testCase.getDeleteSql().replaceAll(":ks", defaultKeyspace));
        }
    }

    public void printCaseInfo(TestCase testCase) {
        printInfo("\nNo." + number + " - From File:   " + testCase.getFile());
        printComment("comment: " + testCase.getComment());
        printNormal("initSql: ");
        for (String initSql : testCase.getInitSql()) {
            printNormal("\t" + initSql);
        }
        printNormal("deleteSql: \n\t" + testCase.deleteSql);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    protected static class TestCase extends TestSuiteCase {
        private String[] initSql;

        private String deleteSql;

        private Long deleteUpdateCount;

        private String[] verifySql;

        private Long[] verifyResult;

        private String exception;

        private String errorMessage;
    }
}
