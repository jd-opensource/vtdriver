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

package com.jd.jdbc.vitess;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VitessDriverFindColumnByName extends TestSuite {
    Connection conn;

    @Before
    public void testLoadDriver() throws Exception {
        conn = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        init();
    }

    @After
    public void close() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

    public void init() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("delete from test");
            stmt.executeUpdate("insert into test (f_tinyint,f_int) values(123, 456)");
        }
    }

    @Test
    public void test1() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            ResultSet resultSet = stmt.executeQuery("select * from test");
            assertTrue(resultSet.next());
            assertEquals(123, resultSet.getInt("f_tinyint"));
            assertEquals(123, resultSet.getInt("F_TinyInt"));
            assertEquals(123, resultSet.getInt("test.F_TinyInt"));
            assertEquals(456, resultSet.getInt("f_int"));
            assertEquals(456, resultSet.getInt("F_INT"));
            assertEquals(456, resultSet.getInt("test.F_INT"));
        }
    }

    @Test
    public void test2() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            ResultSet resultSet = stmt.executeQuery("select f_tinyint as c1, f_int as c2 from test");
            assertTrue(resultSet.next());
            assertEquals(123, resultSet.getInt("c1"));
            assertEquals(123, resultSet.getInt("test.c1"));
            assertEquals(456, resultSet.getInt("c2"));
            assertEquals(456, resultSet.getInt("test.c2"));
        }
    }

    @Test
    public void test3() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            ResultSet resultSet = stmt.executeQuery("select test.f_tinyint, test.f_int from test");
            assertTrue(resultSet.next());
            assertEquals(123, resultSet.getInt("F_tinyint"));
            assertEquals(123, resultSet.getInt("Test.F_tinyint"));
            assertEquals(456, resultSet.getInt("F_INT"));
            assertEquals(456, resultSet.getInt("Test.F_INT"));
        }
    }

    @Test
    public void test4() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            ResultSet resultSet = stmt.executeQuery("select f_tinyint as a, f_int as a from test");
            assertTrue(resultSet.next());
            assertEquals(123, resultSet.getInt("a"));
            assertEquals(123, resultSet.getInt("test.a"));
        }
    }

    @Test
    public void test5() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            ResultSet resultSet = stmt.executeQuery("select f_int as a, f_tinyint as a from test");
            assertTrue(resultSet.next());
            assertEquals(456, resultSet.getInt("a"));
            assertEquals(456, resultSet.getInt("test.a"));
        }
    }

    @Test
    public void test6() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            ResultSet resultSet = stmt.executeQuery("select f_tinyint as f_int, f_int from test");
            assertTrue(resultSet.next());
            assertEquals(123, resultSet.getInt("F_INT"));
            assertEquals(123, resultSet.getInt("test.F_INT"));
        }
    }

    @Test
    public void test7() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            ResultSet resultSet = stmt.executeQuery("select f_int as f_tinyint, f_tinyint from test");
            assertTrue(resultSet.next());
            assertEquals(456, resultSet.getInt("f_tinyint"));
            assertEquals(456, resultSet.getInt("test.f_tinyint"));
        }
    }
}
