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
import java.sql.PreparedStatement;
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

public class UnShardedJoinTest extends SelectTestSuite {

    protected static List<TestCase> testCaseList;

    @BeforeClass
    public static void init() throws SQLException, IOException {
        conn = getConnection(Driver.of(TestSuiteShardSpec.NO_SHARDS));
        testCaseList = new ArrayList<>();
        initTestCase();
        initTestData();
    }

    public static void initTestCase() throws IOException {
        testCaseList.addAll(iterateExecFile("src/test/resources/engine/join/unsharded/join_cases.json", TestCase.class));
    }

    @AfterClass
    public static void cleanup() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("delete from engine_test;");
            stmt.execute("delete from plan_test;");
        }
        if (conn != null) {
            conn.close();
        }
    }

    protected static void initTestData() {
        try (Statement stmt = conn.createStatement()) {
            PreparedStatement psmt1 = conn.prepareStatement("insert into engine_test(f_tinyint) values (?);");
            PreparedStatement psmt2 = conn.prepareStatement("insert into plan_test(f_tinyint) values (?);");
            stmt.execute("delete from engine_test;");
            stmt.execute("delete from plan_test;");

            for (int i = 1; i <= 2; i++) {
                psmt1.setByte(1, (byte) i);
                psmt1.addBatch();
            }
            psmt1.executeBatch();

            for (int i = 1; i <= 2; i++) {
                psmt2.setByte(1, (byte) i);
                psmt2.addBatch();
            }
            psmt2.executeBatch();
        } catch (SQLException e) {
            Assert.fail("Failed: error init sql");
        }
    }

    @Test
    public void testStreamUnShardedJoin() throws SQLException, IllegalAccessException, NoSuchFieldException, NoSuchMethodException {
        execute(testCaseList, true);
    }

    @Test
    public void testUnShardedJoin() throws SQLException, IllegalAccessException, NoSuchFieldException, NoSuchMethodException {
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
                    case "Long":
                        Long longValue = rs.getLong(j + 1);
                        if ("f_tinyint".equalsIgnoreCase(fieldName)) {
                            testResult.setF_tinyint(longValue);
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
        private Long f_tinyint;

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
                .hashCode(f_tinyint);
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
            return Objects.equals(that.f_tinyint, f_tinyint);
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
