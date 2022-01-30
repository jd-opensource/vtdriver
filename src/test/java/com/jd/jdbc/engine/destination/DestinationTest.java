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

package com.jd.jdbc.engine.destination;

import com.google.common.base.Objects;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;
import testsuite.internal.testcase.TestSuiteCase;

public class DestinationTest extends TestSuite {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    Connection conn;

    Connection rrConnection;

    private List<DestinationTest.SendEngineTestCase> sendEngineTestCaseList;

    @Before
    public void before() throws SQLException, IOException {
        conn = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        String baseUrl = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        rrConnection = DriverManager.getConnection(baseUrl + "&role=rr");
        this.initTestData();
    }

    @After
    public void after() throws SQLException {
        closeConnection(conn);
        closeConnection(rrConnection);
    }

    @Test
    public void conn8Test() throws SQLException {
        thrown.expect(SQLException.class);
        thrown.expectMessage("no valid tablet");
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("/*shard=-20, set for specific shard*/update user set predef1 = 10 where id = 1");
        }
    }

    @Test
    public void insert() throws SQLException {
        thrown.expect(SQLFeatureNotSupportedException.class);
        thrown.expectMessage("insert statement does not support execute by destination");
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("/*shard=80-, set for specific shard*/INSERT INTO USER_METADATA (USER_ID, NAME) VALUES (42, 'ms X'), (43, 'abc'), (44, 'def')");
        }
    }

    @Test
    public void select() throws IOException, SQLException {
        this.execute(conn, "src/test/resources/engine/destination/select_cases.json");
    }

    @Test
    public void update() throws SQLException, IOException {
        this.execute(conn, "src/test/resources/engine/destination/update_cases.json");
    }

    @Test
    public void delete() throws SQLException, IOException {
        this.execute(conn, "src/test/resources/engine/destination/delete_cases.json");
    }

    @Test
    public void selectRR() throws IOException, SQLException {
        sleep(5);
        this.execute(rrConnection, "src/test/resources/engine/destination/select_cases.json");
    }

    @Test
    public void updateRR() throws SQLException, IOException {
        thrown.expect(SQLException.class);
        thrown.expectMessage("is not allowed for read only connection");
        this.execute(rrConnection, "src/test/resources/engine/destination/update_cases.json");
    }

    @Test
    public void deleteRR() throws SQLException, IOException {
        thrown.expect(SQLException.class);
        thrown.expectMessage("is not allowed for read only connection");
        this.execute(rrConnection, "src/test/resources/engine/destination/delete_cases.json");
    }

    protected void initTestData() {
        try (Statement stmt = this.conn.createStatement()) {
            stmt.executeUpdate("delete from user_metadata;");
            stmt.executeUpdate("delete from user;");

            String insertUserMeta = "insert into user_metadata(id, user_id, email, address, md5) VALUES ";
            String insertUser = "insert into user(id, name, costly, predef1, predef2, textcol1, textcol2) VALUES ";
            String tempInsertUserMeta = insertUserMeta + " ";
            String tempInsertUser = insertUser + " ";
            for (int i = 1; i <= 50; i++) {
                int i1 = i;
                int i2 = i * 10 + i;
                int i3 = i * 100 + i * 10 + i;

                String userMetaValue = "( " + i1 + ", " + i2 + ", '5@jd.com', 'address1', '" + i1 + "')";
                String userValue = "(" + i1 + ", 'name" + i % 10 + "', " + i2 + ", " + i3 + ", " + i3 + ", 'text1', 'text2')";
                if (i % 10 == 0) {
                    userMetaValue = userMetaValue + ";";
                    userValue = userValue + ";";
                } else {
                    userMetaValue = userMetaValue + ",";
                    userValue = userValue + ",";
                }
                tempInsertUserMeta = tempInsertUserMeta + userMetaValue;
                tempInsertUser = tempInsertUser + userValue;

                if (i % 10 == 0) {
                    stmt.executeUpdate(tempInsertUserMeta);
                    stmt.executeUpdate(tempInsertUser);
                    printInfo("finish " + i + " insert");
                    tempInsertUserMeta = insertUserMeta + " ";
                    tempInsertUser = insertUser + " ";
                }
            }
        } catch (SQLException e) {
            Assert.fail("Failed: error init sql");
        }
    }

    public void execute(Connection connection, String fileName) throws SQLException, IOException {
        this.sendEngineTestCaseList = iterateExecFile(fileName, DestinationTest.SendEngineTestCase.class);
        try (Statement stmt = connection.createStatement()) {
            for (int index = 0; index < sendEngineTestCaseList.size(); index++) {
                DestinationTest.SendEngineTestCase sendEngineTestCase = this.sendEngineTestCaseList.get(index);
                //如果需要先执行update语句则先执行update语句
                if (!StringUtils.isEmpty(sendEngineTestCase.getUpdateSql())) {
                    boolean hasUpdateResult = stmt.execute(sendEngineTestCase.updateSql);
                    if (hasUpdateResult) {
                        throw new SQLException("it should be dml sql");
                    }
                    Assert.assertEquals(printFail("result: " + sendEngineTestCase.updateSql), sendEngineTestCase.updateCount, Long.valueOf(stmt.getUpdateCount()));
                }
                //执行验证sql
                boolean hasSelectResult = stmt.execute(sendEngineTestCase.verfiySql);
                if (!hasSelectResult) {
                    throw new SQLException("no result set for sql: " + sendEngineTestCase.verfiySql);
                }
                ResultSet rs = null;
                //验证结果
                try {
                    rs = stmt.getResultSet();
                    List<DestinationTest.TestResult> driverRsList = makeTestResult(rs, sendEngineTestCase);
                    List<DestinationTest.TestResult> expectRsList = sendEngineTestCase.getExpectedList();
                    Assert.assertEquals(printFail("Failed: " + sendEngineTestCase.verfiySql), driverRsList, expectRsList);
                    printNormal("No." + index + " driver: " + driverRsList);
                    printNormal("No." + index + " file:   " + expectRsList);
                } finally {
                    rs.close();
                }
            }
        }
    }

    private List<DestinationTest.TestResult> makeTestResult(ResultSet rs, TestSuiteCase testCase) throws SQLException {
        List<DestinationTest.TestResult> testResultList = new ArrayList<>();
        TestSuiteCase.Field[] fields = testCase.getFields();
        while (rs.next()) {
            DestinationTest.TestResult testResult = new DestinationTest.TestResult();
            for (int j = 0; j < fields.length; j++) {
                TestSuiteCase.Field field = fields[j];
                String fieldName = field.getName();
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
                        if ("userName".equalsIgnoreCase(fieldName)) {
                            testResult.setUserName(strValue);
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
    public static class SendEngineTestCase extends TestSuiteCase {
        private List<String> initSql;

        private String updateSql;

        private Object[][] verfiyResult;

        private String verfiySql;

        private Long updateCount;

        public List<DestinationTest.TestResult> getExpectedList() {
            List<DestinationTest.TestResult> rs = new ArrayList<>();
            TestSuiteCase.Field[] fields = this.getFields();
            for (Object[] objects : this.verfiyResult) {
                TestResult tmpRs = new TestResult();
                for (int j = 0; j < fields.length; ++j) {
                    Field field = fields[j];
                    String fieldName = field.getName();
                    String javaType = field.getJavaType();
                    switch (javaType) {
                        case "Integer":
                            int intValue = Integer.parseInt(objects[j].toString());
                            if ("id".equalsIgnoreCase(fieldName)) {
                                tmpRs.setId(intValue);
                            } else {
                                Assert.fail("fieldName in expected output is not found: " + fieldName + ", javaType: " + javaType);
                            }
                            break;
                        case "String":
                            String strValue = objects[j].toString();
                            if ("userName".equalsIgnoreCase(fieldName)) {
                                tmpRs.setUserName(strValue);
                            } else {
                                Assert.fail("fieldName in expected output is not found: " + fieldName + ", javaType: " + javaType);
                            }
                            break;
                        default:
                            Assert.fail("javaType in expected output is not found: " + fieldName + ", javaType: " + javaType);
                            break;
                    }
                }
                rs.add(tmpRs);
            }
            return rs;
        }
    }

    @Data
    private static class TestResult implements Comparable<DestinationTest.TestResult> {

        private Integer id;

        private String userName;

        @Override
        public int compareTo(DestinationTest.TestResult o) {
            if (this.hashCode() == o.hashCode()) {
                return 0;
            } else if (this.hashCode() > o.hashCode()) {
                return 1;
            }
            return -1;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(id, userName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DestinationTest.TestResult)) {
                return false;
            }
            DestinationTest.TestResult that = (DestinationTest.TestResult) o;
            return Objects.equal(that.id, id) &&
                Objects.equal(that.userName, userName);
        }

        @Override
        public String toString() {
            StringJoiner joiner = new StringJoiner(", ", DestinationTest.TestResult.class.getSimpleName() + "[", "]");
            Optional.ofNullable(id).ifPresent(v -> joiner.add("id=" + v));
            Optional.ofNullable(userName).ifPresent(v -> joiner.add("userName=" + v));
            return joiner.toString();
        }
    }
}
