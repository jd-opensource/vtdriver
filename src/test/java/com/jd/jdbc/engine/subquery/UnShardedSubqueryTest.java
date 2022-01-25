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

package com.jd.jdbc.engine.subquery;

import com.google.common.base.Objects;
import com.jd.jdbc.engine.SelectTestSuite;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
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

public class UnShardedSubqueryTest extends SelectTestSuite {

    protected static List<TestCase> testCaseList;

    @BeforeClass
    public static void init() throws SQLException, IOException {
        conn = getConnection(Driver.of(TestSuiteShardSpec.NO_SHARDS));
        testCaseList = new ArrayList<>();
        initTestCase();
        initTestData();
    }

    public static void initTestCase() throws IOException {
        testCaseList.addAll(iterateExecFile("src/test/resources/engine/subquery/unsharded/subquery_cases.json",
            TestCase.class));
    }

    public static void initTestData() {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("delete from user_unsharded;");
            stmt.executeUpdate("delete from user_unsharded_extra;");
            stmt.executeUpdate("delete from music;");
            stmt.executeUpdate("insert into user_unsharded(id, name, costly, predef1, predef2, textcol1, textcol2) " +
                "VALUES (1, '101', 101, 101, 101, 'user_unsharded.textcol1_1', 'user_unsharded.textcol2_1')," +
                "(2, '102', 102, 102, 102, 'user_unsharded.textcol1_2', 'user_unsharded.textcol2_2')," +
                "(3, '103', 103, 103, 103, 'user_unsharded.textcol1_3', 'user_unsharded.textcol2_3')," +
                "(4, '104', 104, 104, 104, 'user_unsharded.textcol1_4', 'user_unsharded.textcol2_4')," +
                "(5, '105', 105, 105, 105, 'user_unsharded.textcol1_5', 'user_unsharded.textcol2_5')");
            stmt.executeUpdate("insert into user_unsharded_extra(id, user_id, extra_id) VALUES " +
                "(100, 101, 101),(200, 102, 102),(300, 103, 103),(400, 104, 104),(500, 105, 105)");
            stmt.executeUpdate("insert into music(id, user_id, col) VALUES " +
                "(101, 101, 'col_1')");
        } catch (SQLException e) {
            Assert.fail("Failed: error init sql");
        }
    }

    @AfterClass
    public static void cleanup() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("delete from user_unsharded;");
            stmt.executeUpdate("delete from user_unsharded_extra;");
            stmt.executeUpdate("delete from music;");
        }
        if (conn != null) {
            conn.close();
        }
    }

    @Test
    public void testUnShardedSubquery() throws SQLException, NoSuchFieldException, IllegalAccessException, NoSuchMethodException {
        execute(testCaseList, false);
    }

    @Test
    public void testUnShardedStreamSubquery() throws SQLException, NoSuchFieldException, IllegalAccessException, NoSuchMethodException {
        execute(testCaseList, true);
    }

    @Override
    protected List<ResultRow> rsToResultList(ResultSet rs, Object tclass) throws SQLException {
        TestCase testCase = (TestCase) tclass;
        List<ResultRow> testResultList = new ArrayList<>();
        TestSuiteCase.Field[] fields = testCase.getFields();
        while (rs.next()) {
            ResultRow testResult = new ResultRow();
            for (int j = 0; j < fields.length; j++) {
                TestSuiteCase.Field field = fields[j];
                String fieldName = field.getName();
                String javaType = field.getJavaType();
                switch (javaType) {
                    case "Integer":
                        int intValue = rs.getInt(j + 1);
                        if ("cnt".equalsIgnoreCase(fieldName)) {
                            testResult.setCnt(intValue);
                        } else if ("u.id".equalsIgnoreCase(fieldName)
                            || "id".equalsIgnoreCase(fieldName)
                            || "t.id".equalsIgnoreCase(fieldName)) {
                            testResult.setUserId(intValue);
                        } else if ("user_unsharded_extra.id".equalsIgnoreCase(fieldName)) {
                            testResult.setUserExtraId(intValue);
                        } else if ("ue.user_id".equalsIgnoreCase(fieldName)) {
                            testResult.setUserExtraUserId(intValue);
                        } else if ("user_unsharded_extra.extra_id".equalsIgnoreCase(fieldName)
                            || "e.extra_id".equalsIgnoreCase(fieldName)) {
                            testResult.setUserExtraExtraId(intValue);
                        } else if ("user_unsharded.costly".equalsIgnoreCase(fieldName)
                            || "u.costly".equalsIgnoreCase(fieldName)
                            || "costly".equalsIgnoreCase(fieldName)) {
                            testResult.setUserCostly(intValue);
                        } else if ("user_unsharded.predef1".equalsIgnoreCase(fieldName)
                            || "u.predef1".equalsIgnoreCase(fieldName)
                            || "e.predef1".equalsIgnoreCase(fieldName)) {
                            testResult.setUserPredef1(intValue);
                        } else if ("user_unsharded.predef2".equalsIgnoreCase(fieldName)
                            || "u.predef2".equalsIgnoreCase(fieldName)) {
                            testResult.setUserPredef2(intValue);
                        } else if ("a.user_id".equalsIgnoreCase(fieldName)
                            || "b.user_id".equalsIgnoreCase(fieldName)) {
                            testResult.setAuthUserId(intValue);
                        } else if ("a.col2".equalsIgnoreCase(fieldName)
                            || "b.col2".equalsIgnoreCase(fieldName)) {
                            testResult.setAuthCol2(intValue);
                        } else {
                            Assert.fail("fieldName in output is not found: " + fieldName + ", javaType: " + javaType);
                        }
                        break;
                    case "String":
                        String strValue = rs.getString(j + 1);
                        if ("pluginName".equalsIgnoreCase(fieldName)) {
                            testResult.setPluginName(strValue);
                        } else if ("u.name".equalsIgnoreCase(fieldName)
                            || "t.name".equalsIgnoreCase(fieldName)
                            || "name".equalsIgnoreCase(fieldName)) {
                            testResult.setUserName(strValue);
                        } else if ("u.textcol1".equalsIgnoreCase(fieldName)
                            || "textcol1".equalsIgnoreCase(fieldName)) {
                            testResult.setUserTextcol1(strValue);
                        } else if ("u.textcol2".equalsIgnoreCase(fieldName)
                            || "textcol2".equalsIgnoreCase(fieldName)) {
                            testResult.setUserTextcol2(strValue);
                        } else if ("music.col".equalsIgnoreCase(fieldName)) {
                            testResult.setMusicCol(strValue);
                        } else if ("a.col1".equalsIgnoreCase(fieldName)
                            || "b.col1".equalsIgnoreCase(fieldName)) {
                            testResult.setAuthCol1(strValue);
                        } else if ("table_name".equalsIgnoreCase(fieldName)) {
                            testResult.setTableName(strValue);
                        } else {
                            Assert.fail("fieldName in output is not found: " + fieldName + ", javaType: " + javaType);
                        }
                        break;
                    case "Long":
                        Long longValue = rs.getLong(j + 1);
                        if ("index_length".equalsIgnoreCase(fieldName)) {
                            testResult.setIndexLength(longValue);
                        }
                        break;
                    default:
                        Assert.fail("javaType in output is not found: " + fieldName + ", javaType: " + javaType);
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
        private Integer id;

        private Integer cnt;

        private String pluginName;

        private Long indexLength;

        private String tableName;

        private Integer userId;

        private String userName;

        private Integer userCostly;

        private Integer userPredef1;

        private Integer userPredef2;

        private String userTextcol1;

        private String userTextcol2;

        private Integer userExtraId;

        private Integer userExtraUserId;

        private Integer userExtraExtraId;

        private String musicCol;

        private Integer authUserId;

        private String authCol1;

        private Integer authCol2;

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
            return Objects.hashCode(id, cnt, pluginName, indexLength, userId, userName, userCostly, userPredef1, userPredef2,
                userTextcol1, userTextcol2, userExtraId, userExtraUserId, userExtraExtraId, musicCol, authUserId, authCol1, authCol2);
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
            return Objects.equal(that.id, id) &&
                Objects.equal(that.cnt, cnt) &&
                Objects.equal(that.pluginName, pluginName) &&
                Objects.equal(that.indexLength, indexLength) &&
                Objects.equal(that.tableName, tableName) &&
                Objects.equal(that.userId, userId) &&
                Objects.equal(that.userName, userName) &&
                Objects.equal(that.userCostly, userCostly) &&
                Objects.equal(that.userPredef1, userPredef1) &&
                Objects.equal(that.userPredef2, userPredef2) &&
                Objects.equal(that.userTextcol1, userTextcol1) &&
                Objects.equal(that.userTextcol2, userTextcol2) &&
                Objects.equal(that.userExtraId, userExtraId) &&
                Objects.equal(that.userExtraUserId, userExtraUserId) &&
                Objects.equal(that.userExtraExtraId, userExtraExtraId) &&
                Objects.equal(that.musicCol, musicCol) &&
                Objects.equal(that.authUserId, authUserId) &&
                Objects.equal(that.authCol1, authCol1) &&
                Objects.equal(that.authCol2, authCol2);
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
