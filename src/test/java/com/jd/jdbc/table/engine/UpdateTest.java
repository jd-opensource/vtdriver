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

package com.jd.jdbc.table.engine;

import com.jd.jdbc.table.TableTestUtil;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;
import testsuite.internal.testcase.TestSuiteCase;

public class UpdateTest extends TestSuite {
    protected Connection conn;

    protected List<UpdateTest.TestCase> testCaseList;

    @Before
    public void init() throws SQLException, IOException {
        getConn();
        initCase();
    }

    protected void getConn() throws SQLException {
        conn = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
    }

    protected void initCase() throws IOException, SQLException {
        testCaseList = initCase("src/test/resources/engine/tableengine/update_case.json", TestCase.class, conn.getCatalog());
        testCaseList.addAll(initCase("src/test/resources/engine/tableengine/update_case_upperCase.json", TestCase.class, conn.getCatalog()));
    }

    @After
    public void clean() {
        closeConnection(conn);
    }

    protected void printResultRow(UpdateTest.ResultRow[] rows, String message) {
        printInfo(message);
        for (UpdateTest.ResultRow row : rows) {
            printInfo("\t" + row.toString());
        }
    }

    @Test
    public void test01() throws Exception {
        // 分片、分表键一致
        TableTestUtil.setSplitTableConfig("engine/tableengine/split-table_1.yml", conn.getMetaData().getURL());
        update(false);
    }

    @Test
    public void test02() throws Exception {
        // 分片、分表键不一致
        TableTestUtil.setSplitTableConfig("engine/tableengine/split-table_2.yml", conn.getMetaData().getURL());
        update(false);
    }

    public void update(boolean useAffectedRows) throws SQLException, NoSuchFieldException, IllegalAccessException {
        int count = 0;
        for (UpdateTest.TestCase testCase : testCaseList) {
            count++;
            printInfo("\nNo." + count + " - From File:   " + testCase.getFile());
            printComment("comment: " + testCase.getComment());
            try (Statement stmt = conn.createStatement()) {
                printNormal("initSql:");
                for (String initSql : testCase.getInitSql()) {
                    printNormal("\t" + initSql);
                    stmt.execute(initSql);
                }

                printNormal("query: \n\t" + testCase.getQuery());
                if (testCase.getException() != null) {
                    printNormal("expected exception: \n\t" + testCase.getException());
                }

                int updateCount;
                try {
                    if (testCase.getUpdateVar() != null && testCase.getUpdateVar().length > 0) {
                        try (PreparedStatement pstmt = conn.prepareStatement(testCase.getQuery())) {
                            for (int i = 1; i <= testCase.getUpdateVar().length; i++) {
                                pstmt.setObject(i, testCase.getUpdateVar()[i - 1]);
                            }
                            updateCount = pstmt.executeUpdate();
                        }
                    } else {
                        updateCount = stmt.executeUpdate(testCase.getQuery());
                    }
                } catch (Exception e) {
                    Assert.assertEquals(printFail("[FAIL]"), e.getClass().getName(), testCase.getException());
                    Assert.assertTrue(printFail("wrong errorMessage,error message: " + e.getMessage()), e.getMessage().contains(testCase.getErrorMessage()));
                    printOk();
                    continue;
                }
                Assert.assertFalse(printFail("no exception thrown: " + testCase.getException()), testCase.getException() != null && !"".equals(testCase.getException()));

                if (useAffectedRows) {
                    Assert.assertEquals(testCase.getAffectUpdateCount(), updateCount);
                } else {
                    Assert.assertEquals(testCase.getUpdateCount(), updateCount);
                }

                printNormal("verifySql: " + testCase.getVerifySql());
                List<UpdateTest.ResultRow> verifyResult = new ArrayList<>();
                ResultSet rs = stmt.executeQuery(testCase.getVerifySql());
                while (rs.next()) {
                    UpdateTest.ResultRow row = new UpdateTest.ResultRow();
                    for (TestSuiteCase.Field field : testCase.getFields()) {
                        String fieldName = field.getName();
                        String colName = "".equals(field.getAlias()) ? fieldName : field.getAlias();
                        Integer colId = field.getColumnIndex();
                        row.getClass().getField(fieldName)
                            .set(row, colId == null ? rs.getObject(colName) : rs.getObject(colId));
                    }
                    verifyResult.add(row);
                }

                UpdateTest.ResultRow[] expected = testCase.getVerifyResult();
                UpdateTest.ResultRow[] actual = verifyResult.toArray(new UpdateTest.ResultRow[0]);
                if (testCase.getNeedSort()) {
                    Arrays.sort(expected);
                    Arrays.sort(actual);
                }

                printResultRow(expected, "expected result:");
                printResultRow(actual, "actual result:");

                Assert.assertArrayEquals(printFail("[FAIL] sql: " + testCase.getQuery()), expected, actual);
                printOk("[OK]");
            }
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    protected static class TestCase extends TestSuiteCase {
        private String[] initSql;

        private Object[] updateVar;

        private int updateCount;

        private int affectUpdateCount;

        private String verifySql;

        private UpdateTest.ResultRow[] verifyResult;

        private String exception;

        private String errorMessage;
    }

    @Setter
    protected static class ResultRow implements Comparable<UpdateTest.ResultRow> {
        public BigInteger id; // bigint(20)

        public String f_key;

        public Integer f_tinyint;

        public Boolean f_bit; // bit(64)

        public Integer f_midint;

        public Integer f_int;

        @Override
        public int compareTo(UpdateTest.ResultRow o) {
            if (this.hashCode() < o.hashCode()) {
                return -1;
            } else if (this.hashCode() > o.hashCode()) {
                return 1;
            }
            return 0;
        }

        @Override
        public String toString() {
            return "id=" + id + ", f_key=" + f_key + ", f_tinyint=" + f_tinyint + ", f_bit=" + f_bit + ", f_midint=" + f_midint + ", f_int=" + f_int;
        }

        @Override
        public int hashCode() {
            return com.google.common.base.Objects
                .hashCode(id, f_key, f_tinyint, f_bit, f_midint, f_int);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof UpdateTest.ResultRow)) {
                return false;
            }

            UpdateTest.ResultRow o = (UpdateTest.ResultRow) other;
            return Objects.equals(id, o.id) &&
                Objects.equals(f_key, o.f_key) &&
                Objects.equals(f_tinyint, o.f_tinyint) &&
                Objects.equals(f_bit, o.f_bit) &&
                Objects.equals(f_midint, o.f_midint) &&
                Objects.equals(f_int, o.f_int);
        }
    }
}
