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

package com.jd.jdbc.engine.union;

import com.jd.jdbc.engine.SelectTestSuite;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import testsuite.internal.TestSuiteShardSpec;
import testsuite.internal.testcase.TestSuiteCase;

public class UnShardedUnionTest extends SelectTestSuite {

    protected static List<TestCase> testCaseList;

    @BeforeClass
    public static void init() throws SQLException, IOException {
        conn = getConnection(Driver.of(TestSuiteShardSpec.NO_SHARDS));
        testCaseList = new ArrayList<>();
        initTestCase();
        initTestData();
    }

    public static void initTestCase() throws IOException {
        testCaseList.addAll(iterateExecFile("src/test/resources/engine/union/unsharded/union_cases.json", TestCase.class));
    }

    @AfterClass
    public static void cleanup() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("delete from user_unsharded;");
            stmt.execute("delete from user_unsharded_extra;");
            stmt.execute("delete from unsharded_auto;");
        }
        if (conn != null) {
            conn.close();
        }
    }

    protected static void initTestData() {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("delete from user_unsharded;");
            stmt.execute("delete from user_unsharded_extra;");
            stmt.execute("delete from unsharded_auto;");
            stmt.execute("insert into user_unsharded(id, name, costly, predef1, predef2, textcol1, textcol2) " +
                "VALUES (1, '101', 101, 101, 101, 'user_unsharded.textcol1_1', 'user_unsharded.textcol2_1')," +
                "(2, '102', 102, 102, 102, 'user_unsharded.textcol1_2', 'user_unsharded.textcol2_2')," +
                "(3, '103', 103, 103, 103, 'user_unsharded.textcol1_3', 'user_unsharded.textcol2_3')," +
                "(4, '104', 104, 104, 104, 'user_unsharded.textcol1_4', 'user_unsharded.textcol2_4')," +
                "(5, '105', 105, 105, 105, 'user_unsharded.textcol1_5', 'user_unsharded.textcol2_5')");
            stmt.execute("insert into user_unsharded_extra(id, user_id, extra_id) VALUES " +
                "(100, 101, 101),(200, 102, 102),(300, 103, 103),(400, 104, 104),(500, 105, 105)");
            stmt.execute("insert into unsharded_auto(id, val) VALUES " +
                "(100, 101),(200, 102),(300, 103),(400, 104),(500, 105)");
        } catch (SQLException e) {
            Assert.fail("Failed: error init sql");
        }
    }

    @Test
    public void testStreamUnShardedUnion() throws SQLException, IllegalAccessException, NoSuchFieldException, NoSuchMethodException {
        execute(testCaseList, true);
    }

    @Test
    public void testUnShardedUnion() throws SQLException, IllegalAccessException, NoSuchFieldException, NoSuchMethodException {
        execute(testCaseList, false);
    }

    @Override
    protected List<ResultRow> rsToResultList(ResultSet rs, Object object) throws SQLException {
        TestCase testCase = (TestCase) object;
        List<ResultRow> testResultList = new ArrayList<>();
        TestSuiteCase.Field[] fields = testCase.getFields();
        while (rs.next()) {
            ResultRow testResult = new ResultRow();
            for (int j = 0; j < fields.length; j++) {
                TestSuiteCase.Field field = fields[j];
                String fieldName = "".equals(field.getAlias()) ? field.getName() : field.getAlias();
                String javaType = field.getJavaType();
                switch (javaType) {
                    case "Integer":
                        int intValue = rs.getInt(j + 1);
                        if ("id".equalsIgnoreCase(fieldName)) {
                            testResult.setId(intValue);
                        } else {
                            Assert.fail("fieldName in actual output is not found: " + fieldName + ", javaType: " + javaType);
                        }
                        break;
                    case "String":
                        String strValue = rs.getString(j + 1);
                        if ("name".equalsIgnoreCase(fieldName)) {
                            testResult.setName(strValue);
                        } else {
                            Assert.fail("fieldName in actual output is not found: " + fieldName + ", javaType: " + javaType);
                        }
                        break;
                    default:
                        Assert.fail("javaType in actual output is not found: " + fieldName + ", javaType: " + javaType);
                        break;
                }
            }
            testResultList.add(testResult);
        }
        return testResultList;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    protected static class TestCase extends SelectTestSuite.TestCase {
        private ResultRow[] verifyResult;

        private String exception;

        private String errorMessage;
    }

    @Data
    protected static class ResultRow extends SelectTestSuite.ResultRow implements Comparable<ResultRow> {

        // column fields in ResultSet
        private Integer id;

        private String name;

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
        public int hashCode() {
            return com.google.common.base.Objects
                .hashCode(id, name);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (!(o instanceof ResultRow)) {
                return false;
            }

            ResultRow that = (ResultRow) o;
            return Objects.equals(that.id, id) &&
                Objects.equals(that.name, name);
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
    }
}
