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

package com.jd.jdbc.table.multiquery;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;
import testsuite.internal.testcase.TestSuiteCase;

public class MultiqueryTest extends TestSuite {
    @Rule
    public final ExpectedException expection = ExpectedException.none();

    protected Connection conn;

    protected List<MultiqueryTest.TestCase> testCaseList;

    @Before
    public void init() throws SQLException, IOException {
        conn = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        testCaseList = new ArrayList<>();
        testCaseList.addAll(iterateExecFile("src/test/resources/engine/tableengine/multiquery/select_case.json", MultiqueryTest.TestCase.class));
    }

    @After
    public void clean() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

    protected void printResultRow(MultiqueryTest.ResultRow[] rows, String message) {
        printNormal(message);
        for (MultiqueryTest.ResultRow row : rows) {
            printNormal("\t" + row.toString());
        }
    }

    @Test
    public void test_select()
        throws SQLException, NoSuchFieldException, IllegalAccessException {
        expection.equals(SQLFeatureNotSupportedException.class);
        expection.expectMessage("unsupported multiquery");
        int count = 0;
        for (MultiqueryTest.TestCase testCase : testCaseList) {
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

                List<MultiqueryTest.ResultRow> queryResult = new ArrayList<>();
                try {
                    ResultSet rs = stmt.executeQuery(testCase.getQuery());
                    while (rs.next()) {
                        MultiqueryTest.ResultRow row = new MultiqueryTest.ResultRow();
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
                    if (e.getClass().getName().equals(testCase.getException())) {
                        continue;
                    }
                    throw e;
                }
                if (testCase.getException() != null && !"".equals(testCase.getException())) {
                    throw new SQLException("no exception thrown: " + testCase.getException());
                }

                MultiqueryTest.ResultRow[] expected = testCase.getVerifyResult();
                MultiqueryTest.ResultRow[] actual = queryResult.toArray(new MultiqueryTest.ResultRow[0]);
                if (testCase.getNeedSort()) {
                    Arrays.sort(expected);
                    Arrays.sort(actual);
                }

                printResultRow(expected, "expected result:");
                printResultRow(actual, "actual result:");

                Assert.assertArrayEquals(expected, actual);
            }
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    protected static class TestCase extends TestSuiteCase {
        String[] initSql;

        MultiqueryTest.ResultRow[] verifyResult;

        String exception;
    }

    @Setter
    protected static class ResultRow implements Comparable<MultiqueryTest.ResultRow> {
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
        public int compareTo(MultiqueryTest.ResultRow o) {
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
            if (!(other instanceof MultiqueryTest.ResultRow)) {
                return false;
            }

            MultiqueryTest.ResultRow o = (MultiqueryTest.ResultRow) other;
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
