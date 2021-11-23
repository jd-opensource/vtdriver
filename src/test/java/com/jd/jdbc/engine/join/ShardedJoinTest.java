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

package com.jd.jdbc.engine.join;

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

public class ShardedJoinTest extends SelectTestSuite {

    protected static List<TestCase> testCaseList;

    @BeforeClass
    public static void init() throws SQLException, IOException {
        conn = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        testCaseList = new ArrayList<>();
        initTestCase();
        initTestData();
    }

    public static void initTestCase() throws IOException {
        testCaseList.addAll(iterateExecFile("src/test/resources/engine/join/sharded/join_cases.json", TestCase.class));
    }

    @AfterClass
    public static void cleanup() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("delete from user;");
            stmt.execute("delete from user_extra;");
            stmt.execute("delete from music;");
            stmt.execute("delete from authoritative;");
        }
        if (conn != null) {
            conn.close();
        }
    }

    protected static void initTestData() {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("delete from user;");
            stmt.execute("delete from user_extra;");
            stmt.execute("delete from music;");
            stmt.execute("delete from authoritative;");
            stmt.execute("insert into user(id, name, costly, predef1, predef2, textcol1, textcol2) " +
                "VALUES (1, '101', 101, 101, 101, 'user.textcol1_1', 'user.textcol2_1')," +
                "(2, '102', 102, 102, 102, 'user.textcol1_2', 'user.textcol2_2')," +
                "(3, '103', 103, 103, 103, 'user.textcol1_3', 'user.textcol2_3')," +
                "(4, '104', 104, 104, 104, 'user.textcol1_4', 'user.textcol2_4')," +
                "(5, '105', 105, 105, 105, 'user.textcol1_5', 'user.textcol2_5')");
            stmt.execute("insert into user_extra(id, user_id, extra_id) VALUES " +
                "(100, 101, 101),(200, 102, 102),(300, 103, 103),(400, 104, 104),(500, 105, 105)");
            stmt.execute("insert into music(id, user_id, col) VALUES " +
                "(101, 101, 'col_1'),(102, 102, 'col_2'),(103, 103, 'col_3'),(104, 104, 'col_4'),(105, 105, 'col_5')");
            stmt.execute("insert into authoritative(user_id, col1, col2) VALUES " +
                "(101, '101', 101),(102, '102', 102),(103, '103', 103),(104, '104', 104),(105, '105', 105);");
        } catch (SQLException e) {
            Assert.fail("Failed: error init sql");
        }
    }

    @Test
    public void testStreamShardedJoin() throws SQLException, IllegalAccessException, NoSuchFieldException, NoSuchMethodException {
        execute(testCaseList, true);
    }

    @Test
    public void testShardedJoin() throws SQLException, IllegalAccessException, NoSuchFieldException, NoSuchMethodException {
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
                        } else if ("userid".equalsIgnoreCase(fieldName)) {
                            testResult.setUserId(intValue);
                        } else if ("predef1".equalsIgnoreCase(fieldName)) {
                            testResult.setPredef1(intValue);
                        } else if ("predef2".equalsIgnoreCase(fieldName)) {
                            testResult.setPredef2(intValue);
                        } else if ("costly".equalsIgnoreCase(fieldName)) {
                            testResult.setCostly(intValue);
                        } else if ("aliasint1".equalsIgnoreCase(fieldName)) {
                            testResult.setAliasInt1(intValue);
                        } else if ("aliasint2".equalsIgnoreCase(fieldName)) {
                            testResult.setAliasInt2(intValue);
                        } else {
                            Assert.fail("fieldName in actual output is not found: " + fieldName + ", javaType: " + javaType);
                        }
                        break;
                    case "String":
                        String strValue = rs.getString(j + 1);
                        if ("textcol1".equalsIgnoreCase(fieldName)) {
                            testResult.setTextCol1(strValue);
                        } else if ("textcol2".equalsIgnoreCase(fieldName)) {
                            testResult.setTextCol2(strValue);
                        } else if ("aliasStr1".equalsIgnoreCase(fieldName)) {
                            testResult.setAliasStr1(strValue);
                        } else if ("name".equalsIgnoreCase(fieldName)) {
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

        private Integer userId;

        private Integer costly;

        private Integer aliasInt1;

        private Integer aliasInt2;

        private Integer predef1;

        private Integer predef2;

        private Long f_tinyInt;

        private String name;

        private String email;

        private String aliasStr1;

        private String textCol1;

        private String textCol2;

        @Override
        public int compareTo(ResultRow o) {
            if (this.hashCode() == o.hashCode()) {
                return 0;
            } else if (this.hashCode() > o.hashCode()) {
                return 1;
            }
            return -1;
        }

        @Override
        public int hashCode() {
            return com.google.common.base.Objects
                .hashCode(id, userId, costly, aliasInt1, aliasInt2, predef1, predef2, f_tinyInt,
                    name, email, aliasStr1, textCol1, textCol2);
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
                Objects.equals(that.userId, userId) &&
                Objects.equals(that.costly, costly) &&
                Objects.equals(that.aliasInt1, aliasInt1) &&
                Objects.equals(that.aliasInt2, aliasInt2) &&
                Objects.equals(that.predef1, predef1) &&
                Objects.equals(that.predef2, predef2) &&
                Objects.equals(that.f_tinyInt, f_tinyInt) &&
                Objects.equals(that.name, name) &&
                Objects.equals(that.email, email) &&
                Objects.equals(that.aliasStr1, aliasStr1) &&
                Objects.equals(that.textCol1, textCol1) &&
                Objects.equals(that.textCol2, textCol2);
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
