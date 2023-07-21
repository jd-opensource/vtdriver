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

package com.jd.jdbc.table.engine;

import com.jd.jdbc.table.TableTestUtil;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

@Ignore
public class TableSequenceTest extends TestSuite {
    private static Connection conn2;

    private static Statement stmt2;

    private static Connection conn;

    private static Statement stmt;

    @Before
    public void init() throws SQLException {
        conn = getConnection(Driver.of(TestSuiteShardSpec.NO_SHARDS));
        stmt = conn.createStatement();

        conn2 = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        stmt2 = conn2.createStatement();
    }

    @After
    public void close() throws SQLException {
        if (stmt != null) {
            stmt.close();
        }
        if (stmt2 != null) {
            stmt2.close();
        }
        closeConnection(conn);
        closeConnection(conn2);
        TableTestUtil.setDefaultTableConfig();
    }

    @Test
    public void testSequence() throws SQLException {
        TableTestUtil.setSplitTableConfig("engine/tableengine/split-table-seq.yml");
        testSequence(stmt);
        testSequence(stmt2);
    }

    @Test
    public void testSequenceAsTindex() throws SQLException {
        TableTestUtil.setSplitTableConfig("engine/tableengine/split-table-seq-tindex.yml");
        testSequence(stmt);
        testSequence(stmt2);
    }

    private void testSequence(Statement stmt) throws SQLException {
        int num = 120;
        stmt.executeUpdate("delete from table_engine_test");
        String sql1 = "insert into table_engine_test (id, f_key) values (null, '%s')";
        for (int i = 0; i < num; i++) {
            stmt.executeUpdate(String.format(sql1, i));
        }
        checkByCountDistinct(stmt, num, "table_engine_test", "id");
        checkByCount(stmt, num, "table_engine_test");

        stmt.executeUpdate("delete from table_engine_test");
        String sql2 = "insert into table_engine_test (f_key, f_tinyint) values ('%s' , 1)";
        for (int i = 0; i < num; i++) {
            stmt.executeUpdate(String.format(sql2, i));
        }
        checkByCountDistinct(stmt, num, "table_engine_test", "id");
        checkByCount(stmt, num, "table_engine_test");
    }

    public void checkByCountDistinct(Statement stmt, final int expected, String tableName, String column) throws SQLException {
        ResultSet resultSet = stmt.executeQuery("select count(distinct(" + column + ")) from " + tableName);
        Assert.assertTrue(resultSet.next());
        int actual = resultSet.getInt(1);
        Assert.assertEquals(printFail("[FAIL]"), expected, actual);
        Assert.assertFalse(resultSet.next());
    }

    public void checkByCount(Statement stmt, final int expected, String tableName) throws SQLException {
        ResultSet resultSet = stmt.executeQuery("select count(*) from " + tableName);
        Assert.assertTrue(resultSet.next());
        int actual = resultSet.getInt(1);
        Assert.assertEquals(printFail("[FAIL]"), expected, actual);
        Assert.assertFalse(resultSet.next());
    }
}
