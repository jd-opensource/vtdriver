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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;
import testsuite.internal.environment.DriverEnv;

public class CharEncodingTest extends TestSuite {
    protected Connection conn;

    public void init(String charEncoding) throws SQLException {
        DriverEnv driverEnv = TestSuite.Driver.of(TestSuiteShardSpec.TWO_SHARDS, charEncoding);
        String url = getConnectionUrl(driverEnv);
        System.out.println("Test Url: " + url);
        this.conn = getConnection(driverEnv);
        try (Statement stmt = this.conn.createStatement()) {
            stmt.executeUpdate("delete from plan_test");
        }
    }

    @Test
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
                System.out.println("Expect:" + "赵欣" + i + "; Actual: " + name);
                Assert.assertEquals("赵欣" + i, name);
            }
        }
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
                System.out.println("Expect:" + "赵欣" + i + "; Actual: " + name);
                Assert.assertEquals("赵欣" + i, name);
            }
        }
    }

    @After
    public void close() throws SQLException {
        if (this.conn != null) {
            this.conn.close();
            System.out.println("Connection has been closed");
        }
    }
}
