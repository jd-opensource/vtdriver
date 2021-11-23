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

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;
import testsuite.internal.testcase.TestSuiteCase;

public class UpdateEngineTest extends TestSuite {

    Connection shardedConnection;

    Connection unshardedConnection;

    List<TestCase> testCaseList = new ArrayList<>();

    Integer number;

    @SneakyThrows
    @Test
    public void updateEngineShardedTest() {
        shardedConnection = DriverManager.getConnection(getUrl(true));

        testCaseList.clear();
        testCaseList.addAll(iterateExecFile("src/test/resources/engine/dml_update_sharded.json", TestCase.class));

        testEngine(shardedConnection);
    }

    @SneakyThrows
    @Test
    public void updateEngineUnshardedTest() {
        unshardedConnection = DriverManager.getConnection(getUrl(false));

        testCaseList.clear();
        testCaseList.addAll(iterateExecFile("src/test/resources/engine/dml_update_unsharded.json", TestCase.class));
        testEngine(unshardedConnection);
    }

    protected String getUrl(boolean isShard) {
        if (isShard) {
            return getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS)) + "&useAffectedRows=false";
        }
        return getConnectionUrl(Driver.of(TestSuiteShardSpec.NO_SHARDS)) + "&useAffectedRows=false";
    }

    protected void testEngine(Connection conn) throws NoSuchFieldException, IllegalAccessException, SQLException {
        testEngine(conn, false);
    }

    public void testEngine(Connection conn, boolean useAffectedRows) throws SQLException, NoSuchFieldException, IllegalAccessException {
        replaceDefaultKeyspace(conn, testCaseList);
        number = 0;
        for (TestCase testCase : testCaseList) {
            number++;
            printCaseInfo(testCase);

            try (Statement stmt = conn.createStatement()) {
                for (String initSql : testCase.getInitSql()) {
                    stmt.execute(initSql);
                }
                int affectedRows = stmt.executeUpdate(testCase.getUpdateSql());

                if (testCase.getSkipResultCheck()) {
                    printOk();
                    continue;
                }
                Long expectedRs = useAffectedRows ? testCase.getAffectedRows() : testCase.getUpdateCount();
                printNormal("\t\tupdateCount expected: " + expectedRs + " actual: " + affectedRows);
                Assert.assertEquals(printFail("[Failed]"), expectedRs, Long.valueOf(affectedRows));

                printNormal("verifySql: ");
                for (int i = 0; i < testCase.getVerifySql().length; i++) {
                    String verifySql = testCase.getVerifySql()[i];
                    printNormal("\t" + verifySql);
                    boolean flag = stmt.execute(verifySql);
                    Assert.assertTrue(flag);
                    ResultSet rs = stmt.getResultSet();

                    ResultRow[] expected = testCase.getVerifyResult()[i];
                    ResultRow[] actual = getResultList(rs).toArray(new ResultRow[0]);

                    if (testCase.getNeedSort()) {
                        Arrays.sort(expected);
                        Arrays.sort(actual);
                    }

                    printResultRow(expected, "\t\texpected:");
                    printResultRow(actual, "\t\tactual:");

                    Assert.assertArrayEquals(expected, actual);
                }
                printOk();
            } catch (Exception e) {
                printNormal("exception:\n\texpected: [" + testCase.getException() + "] " + testCase.getErrorMessage());
                printNormal("\tactual: [" + e.getClass().getName() + "] " + e.getMessage());

                Assert.assertEquals(testCase.getException(), e.getClass().getName());
                Assert.assertTrue(e.getMessage().contains(testCase.getErrorMessage()));
                printOk();
            }
        }
    }

    @After
    public void close() throws SQLException {
        if (shardedConnection != null) {
            shardedConnection.close();
        }

        if (unshardedConnection != null) {
            unshardedConnection.close();
        }
    }

    protected List<ResultRow> getResultList(ResultSet rs) throws SQLException, NoSuchFieldException, IllegalAccessException {
        List<ResultRow> queryResult = new ArrayList<>();
        while (rs.next()) {
            ResultRow row = new ResultRow();
            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
                String fieldName = rs.getMetaData().getColumnName(i + 1);
                row.getClass().getField(fieldName).set(row, rs.getObject(fieldName));
            }
            queryResult.add(row);
        }
        return queryResult;
    }

    public void replaceDefaultKeyspace(Connection conn, List<TestCase> testCaseList) throws SQLException {
        String defaultKeyspace = conn.getCatalog();
        for (TestCase testCase : testCaseList) {
            testCase.setUpdateSql(testCase.getUpdateSql().replaceAll(":ks", defaultKeyspace));
        }
    }

    public void printCaseInfo(TestCase testCase) {
        printInfo("\nNo." + number + " - From File:   " + testCase.getFile());
        printComment("comment: " + testCase.getComment());
        printNormal("initSql: ");
        for (String initSql : testCase.getInitSql()) {
            printNormal("\t" + initSql);
        }
        printNormal("updateSql: \n\t" + testCase.getUpdateSql());
    }

    protected void printResultRow(ResultRow[] rows, String message) {
        printNormal(message);
        for (ResultRow row : rows) {
            printNormal("\t\t\t" + row.toString());
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    protected static class TestCase extends TestSuiteCase {
        private String[] initSql;

        private String updateSql;

        private Long updateCount;

        private String[] verifySql;

        private ResultRow[][] verifyResult;

        private String exception;

        private String errorMessage;

        private Long affectedRows;
    }

    @Setter
    protected static class ResultRow implements Comparable<ResultRow> {
        public Integer f_tinyint;

        public String f_varchar;

        public Integer predef2;

        public Integer col1;

        public Integer col2;

        public Integer id;

        @Override
        public int compareTo(ResultRow o) {
            if (this.hashCode() < o.hashCode()) {
                return -1;
            } else if (this.hashCode() > o.hashCode()) {
                return 1;
            }
            return 0;
        }

        @SneakyThrows
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            Field[] fields = this.getClass().getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                Object res = field.get(this);
                if (res != null) {
                    sb.append(field.getName())
                        .append(":")
                        .append(res)
                        .append(" ");
                }
            }
            return sb.toString();
        }


        @Override
        public int hashCode() {
            return com.google.common.base.Objects
                .hashCode(f_tinyint, f_varchar, predef2, col1, col2, id);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ResultRow)) {
                return false;
            }

            ResultRow o = (ResultRow) other;
            return java.util.Objects.equals(f_tinyint, o.f_tinyint) &&
                java.util.Objects.equals(f_varchar, o.f_varchar) &&
                java.util.Objects.equals(predef2, o.predef2) &&
                java.util.Objects.equals(col1, o.col1) &&
                java.util.Objects.equals(col2, o.col2) &&
                java.util.Objects.equals(id, o.id);
        }
    }

}
