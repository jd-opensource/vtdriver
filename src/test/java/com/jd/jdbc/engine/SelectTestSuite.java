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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.Assert;
import testsuite.TestSuite;
import testsuite.internal.testcase.TestSuiteCase;

public abstract class SelectTestSuite extends TestSuite {

    protected static Connection conn;

    protected static int number;

    public <T extends TestCase> void execute(List<T> testCaseList, boolean isStream) throws SQLException, NoSuchFieldException, IllegalAccessException, NoSuchMethodException {
        number = 0;
        for (T testCase : testCaseList) {
            if (isStream && testCase.getNoStream()) {
                continue;
            }
            number++;
            printCaseInfo(testCase);

            try (Statement stmt = conn.createStatement()) {
                if (testCase.getInitSql() != null) {
                    for (String initSql : testCase.getInitSql()) {
                        stmt.execute(initSql);
                    }
                }
                if (isStream) {
                    stmt.setFetchSize(Integer.MIN_VALUE);
                }

                List<ResultRow> resultRowList = new ArrayList<>();
                boolean hasResult = stmt.execute(testCase.getQuery());
                if (hasResult) {
                    ResultSet rs = stmt.getResultSet();
                    resultRowList.addAll(rsToResultList(rs, testCase));
                }

                if (testCase.getSkipResultCheck()) {
                    continue;
                }

                ResultRow[] expectedResult = testCase.getVerifyResult();
                ResultRow[] driverResult = resultRowList.toArray(new ResultRow[0]);

                if (testCase.getNeedSort()) {
                    Arrays.sort(expectedResult);
                    Arrays.sort(driverResult);
                }

                printResultRow(expectedResult, "expected result:");
                printResultRow(driverResult, "actual result:");
                Assert.assertArrayEquals(printFail("[FAIL]"), expectedResult, driverResult);
                printOk("[OK]");
                System.out.println();
            } catch (Exception e) {
                printException(e, testCase);
                Assert.assertEquals(printFail("[FAIL]"), testCase.getException(), e.getClass().getName());
                Assert.assertTrue(printFail("[FAIL]"), e.getMessage().contains(testCase.getErrorMessage()));
                printOk();
            }
        }
    }

    protected abstract List<? extends ResultRow> rsToResultList(ResultSet rs, Object tclass) throws SQLException, NoSuchFieldException, IllegalAccessException;

    protected void printCaseInfo(TestCase testCase) {
        printInfo("\nNo." + number + " - From File:   " + testCase.getFile());
        printComment("comment: " + testCase.getComment());
        if (testCase.getInitSql() != null) {
            printNormal("initSql: ");
            for (String initSql : testCase.getInitSql()) {
                printNormal("\t" + initSql);
            }
        }
        printNormal("query: \n\t" + testCase.getQuery());
    }

    protected void printException(Exception e, TestCase testCase) {
        printNormal("exception:\n\texpected: [" + testCase.getException() + "] " + testCase.getErrorMessage());
        printNormal("\tactual: [" + e.getClass().getName() + "] " + e.getMessage());
    }

    protected void printResultRow(ResultRow[] rows, String message) {
        printNormal(message);
        for (ResultRow row : rows) {
            printNormal("\t" + row.toString());
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    protected static class TestCase extends TestSuiteCase {
        private String[] initSql;

        private ResultRow[] verifyResult;

        private String exception;

        private String errorMessage;
    }

    @Data
    protected abstract static class ResultRow {
    }
}
