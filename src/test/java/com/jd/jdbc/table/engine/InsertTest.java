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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;
import testsuite.internal.testcase.TestSuiteCase;

public class InsertTest extends TestSuite {
    protected Connection conn;

    protected List<InsertTest.TestCase> testCaseList;

    @Before
    public void init() throws SQLException, IOException {
        conn = DriverManager.getConnection(getUrl());
        initCase();
    }

    protected String getUrl() {
        return getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS)) + "&useAffectedRows=false";
    }

    private void initCase() throws IOException, SQLException {
        testCaseList = initCase("src/test/resources/engine/tableengine/insert_case.json", TestCase.class, conn.getCatalog());
        testCaseList.addAll(initCase("src/test/resources/engine/tableengine/insert_case_upperCase.json", TestCase.class, conn.getCatalog()));
    }

    @After
    public void clean() throws Exception {
        closeConnection(conn);
        TableTestUtil.setDefaultTableConfig();
    }

    protected void printResultRow(InsertTest.ResultRow[] rows, String message) {
        printNormal(message);
        for (InsertTest.ResultRow row : rows) {
            printNormal("\t" + row.toString());
        }
    }

    @Test
    public void testSamekey() throws Exception {
        // 分片分表键一致
        TableTestUtil.setSplitTableConfig("engine/tableengine/split-table_1.yml");
        insert();

    }

    @Test
    public void testDifferentKey() throws Exception {
        // 分片分表键不一致
        TableTestUtil.setSplitTableConfig("engine/tableengine/split-table_2.yml");
        insert();
    }

    protected void insert() throws SQLException, NoSuchFieldException, IllegalAccessException {
        insert(false, true);
    }

    protected void insert(boolean useAffectedRows, boolean shardFlag) throws SQLException, NoSuchFieldException, IllegalAccessException {
        int count = 0;
        for (InsertTest.TestCase testCase : testCaseList) {
            count++;
            printInfo("\nNo." + count + " - From File:   " + testCase.getFile());
            printComment("comment: " + testCase.getComment());
            if (!shardFlag && testCase.getShardFlag()) {
                continue;
            }
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

                Integer rowCount;
                try {
                    if (testCase.getInsertVar() != null && testCase.getInsertVar().length > 0) {
                        try (PreparedStatement pstmt = conn.prepareStatement(testCase.getQuery())) {
                            for (int i = 1; i <= testCase.getInsertVar().length; i++) {
                                pstmt.setObject(i, testCase.getInsertVar()[i - 1]);
                            }
                            rowCount = pstmt.executeUpdate();
                        }
                    } else {
                        rowCount = stmt.executeUpdate(testCase.getQuery());
                    }
                } catch (Exception e) {
                    Assert.assertEquals(printFail("exception: " + testCase.getException()), testCase.getException(), e.getClass().getName());
                    Assert.assertTrue(printFail("wrong errorMessage,error message: " + e.getMessage()), e.getMessage().contains(testCase.getErrorMessage()));
                    printOk();
                    continue;
                }
                Assert.assertTrue(testCase.getException() == null || "".equals(testCase.getException()));
                if (testCase.getRowCount() != null) {
                    printNormal("expected rowCount: " + testCase.getRowCount());
                }

                printNormal("verifySql: \n\t" + testCase.getVerifySql());
                List<InsertTest.ResultRow> queryResult = new ArrayList<>();
                ResultSet rs = stmt.executeQuery(testCase.getVerifySql());
                while (rs.next()) {
                    InsertTest.ResultRow row = new InsertTest.ResultRow();
                    for (TestSuiteCase.Field field : testCase.getFields()) {
                        String fieldName = field.getName();
                        String colName = "".equals(field.getAlias()) ? fieldName : field.getAlias();
                        Integer colId = field.getColumnIndex();
                        row.getClass().getField(fieldName)
                            .set(row, colId == null ? rs.getObject(colName) : rs.getObject(colId));
                    }
                    queryResult.add(row);
                }

                InsertTest.ResultRow[] expected = testCase.getVerifyResult();
                InsertTest.ResultRow[] actual = queryResult.toArray(new InsertTest.ResultRow[0]);
                if (testCase.getNeedSort()) {
                    Arrays.sort(expected);
                    Arrays.sort(actual);
                }

                printResultRow(expected, "expected result:");
                printResultRow(actual, "actual result:");

                Assert.assertEquals(useAffectedRows ? testCase.getRowCount() : testCase.getFoundRows(), rowCount);
                Assert.assertArrayEquals(printFail("[FAIL] sql: " + testCase.getQuery()), expected, actual);
                printOk();
            }
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    protected static class TestCase extends TestSuiteCase {
        private String[] initSql;

        private Object[] insertVar;

        private String verifySql;

        private Integer rowCount;

        private Integer foundRows;

        private ResultRow[] verifyResult;

        private String exception;

        private String errorMessage;

        private Boolean shardFlag = false;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    protected static class ResultRow implements Comparable<InsertTest.ResultRow> {
        // column fields in ResultSet
        public BigInteger id; // bigint(20)

        public String f_key;

        public Integer f_tinyint;

        public Boolean f_bit; // bit(64)

        public Integer f_midint;

        public Integer f_int;

        // aggregation, alias
        public Long _long; // select count(*) ...

        public String _string; // select trim('...')

        public BigDecimal _bigDecimal; // select sum(...)

        @Override
        public int compareTo(InsertTest.ResultRow o) {
            if (this.hashCode() < o.hashCode()) {
                return -1;
            } else if (this.hashCode() > o.hashCode()) {
                return 1;
            }
            return 0;
        }

        @Override
        public String toString() {
            return "id=" + id + ", f_key=" + f_key + ", f_tinyint=" + f_tinyint + ", f_bit=" +
                f_bit + ", f_midint=" + f_midint + ", f_int=" + f_int + ", _long=" + _long +
                ", _string=" + _string + ", _bigDecimal=" + _bigDecimal;
        }

        @Override
        public int hashCode() {
            return com.google.common.base.Objects
                .hashCode(id, f_key, f_tinyint, f_bit, f_midint, f_int, _long, _string,
                    _bigDecimal);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof InsertTest.ResultRow)) {
                return false;
            }

            InsertTest.ResultRow o = (InsertTest.ResultRow) other;
            return Objects.equals(id, o.id) &&
                Objects.equals(f_key, o.f_key) &&
                Objects.equals(f_tinyint, o.f_tinyint) &&
                Objects.equals(f_bit, o.f_bit) &&
                Objects.equals(f_midint, o.f_midint) &&
                Objects.equals(f_int, o.f_int) &&
                Objects.equals(_long, o._long) &&
                Objects.equals(_string, o._string) &&
                Objects.equals(_bigDecimal, o._bigDecimal);
        }
    }
}
