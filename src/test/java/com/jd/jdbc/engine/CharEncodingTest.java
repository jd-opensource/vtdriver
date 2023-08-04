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

package com.jd.jdbc.engine;

import com.jd.jdbc.util.InnerConnectionPoolUtil;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;
import testsuite.internal.environment.DriverEnv;

public class CharEncodingTest extends TestSuite {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private Connection conn;

    @BeforeClass
    public static void beforeClass() throws NoSuchFieldException, IllegalAccessException {
        InnerConnectionPoolUtil.clearAll();
    }

    public void init(String charEncoding) throws SQLException {
        DriverEnv driverEnv = TestSuite.Driver.of(TestSuiteShardSpec.TWO_SHARDS, charEncoding);
        this.conn = getConnection(driverEnv);
        try (Statement stmt = this.conn.createStatement()) {
            stmt.executeUpdate("delete from plan_test");
        }
    }

    @After
    public void after() throws IllegalAccessException, NoSuchFieldException, SQLException {
        InnerConnectionPoolUtil.removeInnerConnectionConfig(conn);
        closeConnection(conn);
    }

    @Test
    @Ignore
    public void testGBK() throws SQLException {
        init("GBK");
        String insertPstmtSql = "INSERT INTO plan_test (f_tinyint, f_varchar, f_text) VALUES (1, ?, ?);";
        String insertStmtSql = "INSERT INTO plan_test (f_tinyint, f_varchar, f_text) VALUES (2, '赵欣2', '小明');";
        String selectStmtSql = "SELECT f_tinyint, f_varchar, f_text FROM plan_test;";
        try (Statement stmt = this.conn.createStatement();
             PreparedStatement pstmt = this.conn.prepareStatement(insertPstmtSql)) {
            pstmt.setString(1, "赵欣1");
            pstmt.setString(2, "小明");
            pstmt.executeUpdate();
            stmt.executeUpdate(insertStmtSql);
            ResultSet rs = stmt.executeQuery(selectStmtSql);
            while (rs.next()) {
                int i = rs.getInt(1);
                String name = rs.getString(2);
                Assert.assertArrayEquals(("赵欣" + i).getBytes(StandardCharsets.UTF_8), rs.getBytes(2));
                Assert.assertArrayEquals("小明".getBytes(StandardCharsets.UTF_8), rs.getBytes(3));
                Assert.assertEquals("赵欣" + i, name);
            }
        }
    }

    @Test
    @Ignore
    public void testDual() throws SQLException {
        init("GBK");
        try (Statement stmt = this.conn.createStatement()) {
            // dual
            ResultSet rs = stmt.executeQuery("select 'abc字符串'");
            Assert.assertTrue(rs.next());
            Assert.assertEquals("abc字符串", rs.getString(1));
            Assert.assertArrayEquals("abc字符串".getBytes(StandardCharsets.UTF_8), rs.getBytes(1));
            Assert.assertFalse(rs.next());

            // dual + stream
            stmt.setFetchSize(Integer.MIN_VALUE);
            rs = stmt.executeQuery("select 'abc字符串'");
            Assert.assertTrue(rs.next());
            Assert.assertEquals("abc字符串", rs.getString(1));
            Assert.assertArrayEquals("abc字符串".getBytes(StandardCharsets.UTF_8), rs.getBytes(1));
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void testGBK2() throws SQLException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Only supports utf8 encoding, please check characterEncoding in jdbcurl and file.encoding in environment variable,characterEncoding = GBK, file.encoding=UTF-8");
        init("GBK");
    }

    @Test
    public void testUTF8() throws SQLException {
        init("UTF-8");
        String insertPstmtSql = "INSERT INTO plan_test (f_tinyint, f_varchar, f_text) VALUES (1, ?, ?);";
        String insertStmtSql = "INSERT INTO plan_test (f_tinyint, f_varchar, f_text) VALUES (2, '赵欣2', '小明');";
        String selectStmtSql = "SELECT f_tinyint, f_varchar, f_text FROM plan_test;";
        try (Statement stmt = this.conn.createStatement();
             PreparedStatement pstmt = this.conn.prepareStatement(insertPstmtSql)) {
            pstmt.setString(1, "赵欣1");
            pstmt.setString(2, "小明");
            pstmt.executeUpdate();
            stmt.executeUpdate(insertStmtSql);
            ResultSet rs = stmt.executeQuery(selectStmtSql);
            while (rs.next()) {
                int i = rs.getInt(1);
                String name = rs.getString(2);
                Assert.assertEquals("赵欣" + i, name);
            }
        }
    }
}
