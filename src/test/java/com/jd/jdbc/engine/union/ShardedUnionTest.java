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

public class ShardedUnionTest extends SelectTestSuite {
    protected static List<TestCase> testCaseList;

    @BeforeClass
    public static void init() throws SQLException, IOException {
        conn = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        testCaseList = new ArrayList<>();
        initTestCase();
        initTestData();
    }

    public static void initTestCase() throws IOException {
        testCaseList.addAll(iterateExecFile("src/test/resources/engine/union/sharded/union_cases.json", TestCase.class));
    }

    @AfterClass
    public static void cleanup() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("delete from user_metadata;");
            stmt.execute("delete from user_extra;");
        }
        if (conn != null) {
            conn.close();
        }
    }

    protected static void initTestData() {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("delete from user_metadata;");
            stmt.execute("delete from user_extra;");
            String insertUserExtra = "insert into user_extra(id, user_id, extra_id, email) VALUES ";
            String insertUserMeta = "insert into user_metadata(id, user_id, email, address, md5) VALUES ";
            String tempInsertUserExtra = insertUserExtra + " ";
            String tempInsertUserMeta = insertUserMeta + " ";
            String userExtraValue;
            String userMetaValue;
            for (int i = 1; i <= 5; i++) {
                int i2 = i * 10 + i;
                int i3 = i * 100 + i * 10 + i;
                userExtraValue = "(" + i + ", " + i2 + ", " + i3 + ", 'tdavid5@jd.com')";
                userMetaValue = "( " + i + ", " + i2 + ", '5@jd.com', 'address1', '" + i + "')";
                if (i == 5) {
                    userExtraValue = userExtraValue + ";";
                    userMetaValue = userMetaValue + ";";
                } else {
                    userExtraValue = userExtraValue + ",";
                    userMetaValue = userMetaValue + ",";
                }
                tempInsertUserExtra = tempInsertUserExtra + userExtraValue;
                tempInsertUserMeta = tempInsertUserMeta + userMetaValue;
            }
            stmt.execute(tempInsertUserExtra);
            stmt.execute(tempInsertUserMeta);
        } catch (SQLException e) {
            Assert.fail("Failed: error init sql");
        }
    }

    @Test
    public void testStreamShardedUnion() throws SQLException, IllegalAccessException, NoSuchFieldException, NoSuchMethodException {
        execute(testCaseList, true);
    }

    @Test
    public void testShardedUnion() throws SQLException, IllegalAccessException, NoSuchFieldException, NoSuchMethodException {
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
                        if ("email".equalsIgnoreCase(fieldName)) {
                            testResult.setEmail(strValue);
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

        private String email;

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
                .hashCode(id, email);
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
                Objects.equals(that.email, email);
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
