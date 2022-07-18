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

package com.jd.jdbc.table.engine.join;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;
import testsuite.internal.testcase.TestSuiteCase;

public class JoinTest extends TestSuite {

    private Connection conn;

    private List<TestCase> testCaseList;

    @Before
    public void init() throws SQLException, IOException {
        conn = getConnection(TestSuite.Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        testCaseList = initCase("src/test/resources/engine/tableengine/join_case.json", TestCase.class, conn.getCatalog());
    }

    @Test
    public void test_join() throws SQLException {
        int count = 0;
        for (TestCase testCase : testCaseList) {
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
                } else {
                    ResultRow[] expected = testCase.getVerifyResult();
                    printResultRow(expected, "expected result:");
                }

                try {
                    stmt.executeQuery(testCase.getQuery());
                } catch (Exception e) {
                    Assert.assertEquals(printFail("exception: " + testCase.getException()), testCase.getException(), e.getClass().getName());
                }
                printOk();
            }
        }
    }

    @Test
    @Ignore
    public void test() throws SQLException {
        String select0 = "select id, f_key from table_engine_test where id > 1";
        String select1 = "select id, f_key from table_engine_test where f_key = '1'";
        String join0 = "select a.id, b.id, a.name from user a join t_users b on a.name = b.id where a.id > 1";
        String join01 = "select a.id, b.id, a.name from user a join t_users b on a.name = b.id where a.name='a'";
        String join1 = "select a.id, b.id, a.name from user a join t_users b on a.name = b.name where a.id > 1";
        String join2 = "select a.id, b.id, a.name from user a join table_engine_test b on a.name = b.f_key where a.name='a'";
        String join3 = "select a.id, b.id, b.name from table_engine_test a join user b on a.id = b.id where a.f_key='key1'";
        String sql4 = "select f_key from table_engine_test where f_key='1'";

        Connection conn = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        Statement stmt = conn.createStatement();

        stmt.executeUpdate("delete from user");
        stmt.executeUpdate("delete from t_users");
        stmt.executeUpdate("delete from table_engine_test");

        stmt.executeUpdate("insert into user (id, name) values (1, 'name1')");
        stmt.executeUpdate("insert into user (id, name) values (1, 'a')");
        stmt.executeUpdate("insert into t_users (id, name, age) values (99, 'name2', 10)");
        stmt.executeUpdate("insert into table_engine_test (id, f_key) values (100, 'name1')");
        stmt.executeUpdate("insert into table_engine_test (id, f_key) values (101, 'a')");
        stmt.executeUpdate("insert into table_engine_test (id, f_key) values (102, 'b')");

        ResultSet rs = stmt.executeQuery(join2);

        while (rs.next()) {
            rs.getObject(1);
        }
        rs.close();
        stmt.close();
        conn.close();
    }

    protected void printResultRow(final ResultRow[] rows, final String message) {
        printNormal(message);
        for (ResultRow row : rows) {
            printNormal("\t" + row.toString());
        }
    }

    @After
    public void clean() throws SQLException {
        closeConnection(conn);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    protected static class TestCase extends TestSuiteCase {
        private String[] initSql;

        private Object[] insertVar;

        private String verifySql;

        private ResultRow[] verifyResult;

        private String exception;

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    protected static class ResultRow implements Comparable<JoinTest.ResultRow> {
        // column fields in ResultSet
        private BigInteger id; // bigint(20)

        private String f_key;

        private Integer f_tinyint;

        private Boolean f_bit; // bit(64)

        private Integer f_midint;

        private Integer f_int;

        // aggregation, alias
        private Long _long; // select count(*) ...

        private String _string; // select trim('...')

        private BigDecimal _bigDecimal; // select sum(...)

        @Override
        public int compareTo(final ResultRow o) {
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
        public boolean equals(final Object other) {
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
