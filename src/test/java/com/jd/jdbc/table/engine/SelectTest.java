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

import com.jd.jdbc.session.SafeSession;
import com.jd.jdbc.table.TableTestUtil;
import com.jd.jdbc.vitess.VitessConnection;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
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

public class SelectTest extends TestSuite {

    protected String baseUrl;

    protected List<TestCase> testCaseList;

    protected List<Connection> connectionList;

    @Before
    public void init() throws SQLException, IOException {
        getConn();
        testCaseList = new ArrayList<>();
        Connection conn = this.connectionList.get(0);
        testCaseList.addAll(initCase("src/test/resources/engine/tableengine/select_case.json", TestCase.class, conn.getCatalog()));
        testCaseList.addAll(initCase("src/test/resources/engine/tableengine/select_case_upperCase.json", TestCase.class, conn.getCatalog()));
        testCaseList.addAll(initCase("src/test/resources/engine/tableengine/select_aggr_case.json", TestCase.class, conn.getCatalog()));
        testCaseList.addAll(initCase("src/test/resources/engine/tableengine/select_aggr_case_upperCase.json", TestCase.class, conn.getCatalog()));
        testCaseList.addAll(initCase("src/test/resources/engine/tableengine/select_memsort_case.json", TestCase.class, conn.getCatalog()));
        testCaseList.addAll(initCase("src/test/resources/engine/tableengine/select_memsort_case_upperCase.json", TestCase.class, conn.getCatalog()));
        testCaseList.addAll(initCase("src/test/resources/engine/tableengine/select_postprocess_case.json", TestCase.class, conn.getCatalog()));
        testCaseList.addAll(initCase("src/test/resources/engine/tableengine/select_postprocess_case_upperCase.json", TestCase.class, conn.getCatalog()));
        testCaseList.addAll(initCase("src/test/resources/engine/tableengine/select_filter_case.json", TestCase.class, conn.getCatalog()));
        testCaseList.addAll(initCase("src/test/resources/engine/tableengine/select_filter_case_upperCase.json", TestCase.class, conn.getCatalog()));
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
    public void clean() throws SQLException {
        if (this.connectionList != null) {
            for (Connection conn : this.connectionList) {
                if (conn != null) {
                    conn.close();
                }
            }
        }
    }

    protected void printResultRow(ResultRow[] rows, String message) {
        printNormal(message);
        for (ResultRow row : rows) {
            printNormal("\t" + row.toString());
        }
    }

    @Test
    public void test01() throws Exception {
        // 分片分表键一致
        TableTestUtil.setSplitTableConfig("engine/tableengine/split-table_1.yml", baseUrl);
        select();
    }

    @Test
    public void test02() throws Exception {
        // 分片分表键不一致
        TableTestUtil.setSplitTableConfig("engine/tableengine/split-table_2.yml", baseUrl);
        select();
    }

    protected void select() throws NoSuchFieldException, IllegalAccessException, SQLException {
        select(true);
    }

    public void select(boolean shardFlag)
        throws SQLException, NoSuchFieldException, IllegalAccessException {
        int count = 0;
        for (TestCase testCase : testCaseList) {
            count++;
            printInfo("\nNo." + count + " - From File:   " + testCase.getFile());
            printComment("comment: " + testCase.getComment());
            if (!shardFlag && testCase.getShardFlag()) {
                continue;
            }
            for (Connection conn : this.connectionList) {
                int maxParallelNum = SafeSession.newSafeSession((VitessConnection) conn).getMaxParallelNum();
                System.out.println("parallel num:" + maxParallelNum);
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

                    List<ResultRow> queryResult = new ArrayList<>();
                    try {
                        ResultSet rs = stmt.executeQuery(testCase.getQuery());
                        while (rs.next()) {
                            ResultRow row = new ResultRow();
                            for (TestSuiteCase.Field field : testCase.getFields()) {
                                String fieldName = field.getName();
                                String colName =
                                    "".equals(field.getAlias()) ? fieldName : field.getAlias();
                                Integer colId = field.getColumnIndex();
                                row.getClass().getField(fieldName).set(row,
                                    colId == null ? rs.getObject(colName) : rs.getObject(colId));
                            }
                            queryResult.add(row);
                        }
                    } catch (Exception e) {
                        Assert.assertEquals(printFail("exception: " + testCase.getException()), testCase.getException(), e.getClass().getName());
                        Assert.assertTrue(printFail("wrong errorMessage,error message: " + e.getMessage()), e.getMessage().contains(testCase.getErrorMessage()));
                        printOk();
                        continue;
                    }
                    Assert.assertTrue(testCase.getException() == null || "".equals(testCase.getException()));

                    ResultRow[] expected = testCase.getVerifyResult();
                    ResultRow[] actual = queryResult.toArray(new ResultRow[0]);
                    if (testCase.getNeedSort()) {
                        Arrays.sort(expected);
                        Arrays.sort(actual);
                    }

                    printResultRow(expected, "expected result:");
                    printResultRow(actual, "actual result:");

                    Assert.assertArrayEquals(printFail("[Fail] sql: " + testCase.getQuery()), expected, actual);
                    printOk();
                }
            }
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

        private Boolean shardFlag = false;
    }

    @Setter
    protected static class ResultRow implements Comparable<ResultRow> {
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
        public int compareTo(ResultRow o) {
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
                f_bit + ", f_midint=" + f_midint + ", f_int=" + f_int +
                ", _long=" + _long + ", _string=" + _string + ", _bigDecimal=" + _bigDecimal;
        }

        @Override
        public int hashCode() {
            return com.google.common.base.Objects
                .hashCode(id, f_key, f_tinyint, f_bit, f_midint, f_int, _long, _string,
                    _bigDecimal);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ResultRow)) {
                return false;
            }

            ResultRow o = (ResultRow) other;
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
