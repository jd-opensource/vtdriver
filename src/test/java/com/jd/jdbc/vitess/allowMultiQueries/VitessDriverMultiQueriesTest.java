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

package com.jd.jdbc.vitess.allowMultiQueries;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class VitessDriverMultiQueriesTest extends TestSuite {
    private final String mixSql =
        "select id from auto order by id; insert into auto (id,ai,email) values(100,100,'abc'),(101,101,'abc'); select id from auto order by id;delete from auto where id=100 or id=101;select id from auto order by id;";

    private final String selectSql =
        "select id from auto where id < 50 and id < 30 order by id; select id from auto where id < 20 and id < 30 order by id;select id from auto where id < 10 and id < 30 order by id;";

    private final String dmlSql =
        "insert into auto (id,ai,email) values(100,100,'abc'),(101,101,'abc');insert into auto (id,ai,email) values(102,102,'abc');delete from auto where id in (100,101,102);";

    private final String preparedMixSql =
        "select id from auto where 1=? order by id; insert into auto (id,ai,email) values(?,?,?),(?,?,?); select id from auto where 1=? order by id;delete from auto where id=? or id=?;select id from auto where 1=? order by id;";

    private final String preparedSelectSql =
        "select id from auto where id < ? and id < ? order by id; select id from auto where id < ? and id < ? order by id;select id from auto where id < ? and id < ? order by id;";

    private final String preparedDmlSql = "insert into auto (id,ai,email) values(?,?,?),(?,?,?);insert into auto (id,ai,email) values(?,?,?);delete from auto where id in (?,?,?);";

    Connection conn;

    static private void verifyResult(long expect, ResultSet resultSet) throws SQLException {
        long idx = 0;
        while (resultSet.next()) {
            assertEquals(idx, resultSet.getLong(1));
            idx++;
        }
        assertEquals(expect, idx);
    }

    static private void verifyExecuteCloseCurrent(Statement stmt, boolean hasResult) throws SQLException {
        int sqlIdx = 0;
        ResultSet resultSet = null;
        do {
            if (hasResult) {
                resultSet = stmt.getResultSet();
                if (sqlIdx == 0) {
                    verifyResult(100, resultSet);
                } else if (sqlIdx == 2) {
                    verifyResult(102, resultSet);
                } else if (sqlIdx == 4) {
                    verifyResult(100, resultSet);
                } else {
                    fail();
                }
            } else {
                if (sqlIdx == 1) {
                    assertEquals(2, stmt.getUpdateCount());
                } else if (sqlIdx == 3) {
                    assertEquals(2, stmt.getUpdateCount());
                } else if (sqlIdx == 5) {
                    assertEquals(-1, stmt.getUpdateCount());
                    break;
                } else {
                    fail();
                }
            }

            sqlIdx++;

            hasResult = stmt.getMoreResults();
            assertTrue(resultSet.isClosed());
        } while (true);

        assertEquals(5, sqlIdx);
    }

    static private void verifyExecuteQueryCloseCurrent(Statement stmt, ResultSet resultSet) throws SQLException {
        int sqlIdx = 0;
        boolean hasResult;

        do {
            if (sqlIdx == 0) {
                verifyResult(30, resultSet);
            } else if (sqlIdx == 1) {
                verifyResult(20, resultSet);
            } else if (sqlIdx == 2) {
                verifyResult(10, resultSet);
            } else {
                fail();
            }

            sqlIdx++;

            hasResult = stmt.getMoreResults();
            assertTrue(resultSet.isClosed());
            resultSet = stmt.getResultSet();
        } while (hasResult);

        assertEquals(3, sqlIdx);
    }

    static private void verifyExecuteUpdate(Statement stmt, int affectedCount) throws SQLException {
        int sqlIdx = 0;

        boolean hasResult;
        do {
            if (sqlIdx == 0) {
                assertEquals(2, affectedCount);
                assertEquals(2, stmt.getUpdateCount());
            } else if (sqlIdx == 1) {
                assertEquals(1, stmt.getUpdateCount());
            } else if (sqlIdx == 2) {
                assertEquals(3, stmt.getUpdateCount());
            } else {
                fail();
            }

            sqlIdx++;

            hasResult = stmt.getMoreResults() || stmt.getUpdateCount() != -1;
        } while (hasResult);

        assertEquals(3, sqlIdx);
    }

    static private void verifyExecuteKeepCurrent(Statement stmt, boolean hasResult) throws SQLException {
        int sqlIdx = 0;
        List<ResultSet> resultSetList = new ArrayList<>();
        ResultSet resultSet = null;
        do {
            if (hasResult) {
                resultSet = stmt.getResultSet();
                resultSetList.add(resultSet);
            } else {
                if (sqlIdx == 1) {
                    assertEquals(2, stmt.getUpdateCount());
                } else if (sqlIdx == 3) {
                    assertEquals(2, stmt.getUpdateCount());
                } else if (sqlIdx == 5) {
                    assertEquals(-1, stmt.getUpdateCount());
                    break;
                } else {
                    fail();
                }
            }

            sqlIdx++;

            hasResult = stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT);
            assertFalse(resultSet.isClosed());
        } while (true);

        assertEquals(5, sqlIdx);
        assertEquals(3, resultSetList.size());

        assertFalse(resultSetList.get(0).isClosed());
        assertFalse(resultSetList.get(1).isClosed());
        assertFalse(resultSetList.get(2).isClosed());

        verifyResult(100, resultSetList.get(0));
        verifyResult(102, resultSetList.get(1));
        verifyResult(100, resultSetList.get(2));

        stmt.close();

        assertTrue(resultSetList.get(0).isClosed());
        assertTrue(resultSetList.get(1).isClosed());
        assertTrue(resultSetList.get(2).isClosed());
    }

    static private void verifyExecuteQueryKeepCurrent(Statement stmt, ResultSet resultSet) throws SQLException {
        int sqlIdx = 0;
        List<ResultSet> resultSetList = new ArrayList<>();

        boolean hasResult;
        do {
            resultSetList.add(resultSet);

            sqlIdx++;

            hasResult = stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT);
            assertFalse(resultSet.isClosed());
            resultSet = stmt.getResultSet();
        } while (hasResult);

        assertEquals(3, sqlIdx);
        assertEquals(3, resultSetList.size());

        assertFalse(resultSetList.get(0).isClosed());
        assertFalse(resultSetList.get(1).isClosed());
        assertFalse(resultSetList.get(2).isClosed());

        verifyResult(30, resultSetList.get(0));
        verifyResult(20, resultSetList.get(1));
        verifyResult(10, resultSetList.get(2));

        stmt.close();

        assertTrue(resultSetList.get(0).isClosed());
        assertTrue(resultSetList.get(1).isClosed());
        assertTrue(resultSetList.get(2).isClosed());
    }

    static private void verifyExecuteCloseAll(Statement stmt, boolean hasResult) throws SQLException {
        int sqlIdx = 0;
        ResultSet resultSet;
        List<ResultSet> resultSetList = new ArrayList<>();
        do {
            if (hasResult) {
                resultSet = stmt.getResultSet();
                resultSetList.add(resultSet);
            } else {
                if (sqlIdx == 1) {
                    assertEquals(2, stmt.getUpdateCount());
                } else if (sqlIdx == 3) {
                    assertEquals(2, stmt.getUpdateCount());
                } else if (sqlIdx == 5) {
                    assertEquals(-1, stmt.getUpdateCount());
                    break;
                } else {
                    fail();
                }
            }
            sqlIdx++;

            //after the last DML, close all previous result set
            if (sqlIdx == 4) {
                assertEquals(2, resultSetList.size());
                assertFalse(resultSetList.get(0).isClosed());
                assertFalse(resultSetList.get(1).isClosed());
                verifyResult(100, resultSetList.get(0));
                verifyResult(102, resultSetList.get(1));
                hasResult = stmt.getMoreResults(Statement.CLOSE_ALL_RESULTS);
                assertTrue(resultSetList.get(0).isClosed());
                assertTrue(resultSetList.get(1).isClosed());
            } else {
                hasResult = stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT);
            }
        } while (true);

        assertEquals(5, sqlIdx);

        assertEquals(3, resultSetList.size());
        verifyResult(100, resultSetList.get(2));

        stmt.close();

        assertTrue(resultSetList.get(0).isClosed());
        assertTrue(resultSetList.get(1).isClosed());
        assertTrue(resultSetList.get(2).isClosed());
    }

    static private void verifyExecuteQueryCloseAll(Statement stmt, ResultSet resultSet) throws SQLException {
        int sqlIdx = 0;
        List<ResultSet> resultSetList = new ArrayList<>();
        boolean hasResult;
        do {
            resultSetList.add(resultSet);

            sqlIdx++;

            //after the last DML, close all previous result set
            if (sqlIdx == 2) {
                assertEquals(2, resultSetList.size());
                assertFalse(resultSetList.get(0).isClosed());
                assertFalse(resultSetList.get(1).isClosed());
                verifyResult(30, resultSetList.get(0));
                verifyResult(20, resultSetList.get(1));
                hasResult = stmt.getMoreResults(Statement.CLOSE_ALL_RESULTS);
                assertTrue(resultSetList.get(0).isClosed());
                assertTrue(resultSetList.get(1).isClosed());
            } else {
                hasResult = stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT);
            }
            resultSet = stmt.getResultSet();
        } while (hasResult);

        assertEquals(3, sqlIdx);

        assertEquals(3, resultSetList.size());
        verifyResult(10, resultSetList.get(2));

        stmt.close();

        assertTrue(resultSetList.get(0).isClosed());
        assertTrue(resultSetList.get(1).isClosed());
        assertTrue(resultSetList.get(2).isClosed());
    }

    @Before
    public void testLoadDriver() throws Exception {
        conn = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        cleanData();
        prepareData();
    }

    @After
    public void after() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

    protected void cleanData() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("delete from auto;");
        }
    }

    protected void prepareData() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            for (int i = 0; i < 100; i++) {
                stmt.addBatch(String.format("insert into auto (id,ai,email) values(%d,%d,'%s')", i, i, "xxx" + i));
            }
            stmt.executeBatch();
        }
    }

    /*
     * Statement execute
     * Statement executeQuery
     * Statement executeUpdate
     *
     * CLOSE_CURRENT_RESULT/CLOSE_ALL_RESULTS/KEEP_CURRENT_RESULT
     * */

    @Test
    public void test01StatementExecuteCloseCurrent() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            boolean hasResult = stmt.execute(mixSql);
            verifyExecuteCloseCurrent(stmt, hasResult);
        }
    }

    @Test
    public void test02StatementExecuteKeepCurrent() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            boolean hasResult = stmt.execute(mixSql);
            verifyExecuteKeepCurrent(stmt, hasResult);
        }
    }

    @Test
    public void test03StatementExecuteCloseAll() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            boolean hasResult = stmt.execute(mixSql);
            verifyExecuteCloseAll(stmt, hasResult);
        }
    }

    @Test
    public void test04StatementExecuteQueryCloseCurrent() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            ResultSet resultSet = stmt.executeQuery(selectSql);
            verifyExecuteQueryCloseCurrent(stmt, resultSet);
        }
    }

    @Test
    public void test05StatementExecuteQueryKeepCurrent() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            ResultSet resultSet = stmt.executeQuery(selectSql);
            verifyExecuteQueryKeepCurrent(stmt, resultSet);
        }
    }

    @Test
    public void test06StatementExecuteQueryCloseAll() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            ResultSet resultSet = stmt.executeQuery(selectSql);
            verifyExecuteQueryCloseAll(stmt, resultSet);
        }
    }

    @Test
    public void test07StatementExecuteUpdate() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            int affected = stmt.executeUpdate(dmlSql);
            verifyExecuteUpdate(stmt, affected);
        }
    }

    /*
     * PreparedStatement execute
     * PreparedStatement executeQuery
     * PreparedStatement executeUpdate
     *
     * CLOSE_CURRENT_RESULT/CLOSE_ALL_RESULTS/KEEP_CURRENT_RESULT
     * */
    @Test
    public void test08PreparedStatementExecuteCloseCurrent() throws Exception {
        try (PreparedStatement stmt = conn.prepareStatement(preparedMixSql)) {
            //"select id from auto where 1=? order by id; insert into auto auto (id,ai,email) values(?,?,?),(?,?,?); select id from auto where 1=? order by id;delete from auto where id=? or id=?;select id from auto where 1=? order by id;";
            stmt.setInt(1, 1);
            stmt.setInt(2, 100);
            stmt.setInt(3, 100);
            stmt.setString(4, "abc");
            stmt.setInt(5, 101);
            stmt.setInt(6, 101);
            stmt.setString(7, "abc");
            stmt.setInt(8, 1);
            stmt.setInt(9, 100);
            stmt.setInt(10, 101);
            stmt.setInt(11, 1);
            boolean hasResult = stmt.execute();
            verifyExecuteCloseCurrent(stmt, hasResult);
        }
    }

    @Test
    public void test09PreparedStatementExecuteKeepCurrent() throws Exception {
        try (PreparedStatement stmt = conn.prepareStatement(preparedMixSql)) {
            //"select id from auto where 1=? order by id; insert into auto auto (id,ai,email) values(?,?,?),(?,?,?); select id from auto where 1=? order by id;delete from auto where id=? or id=?;select id from auto where 1=? order by id;";
            stmt.setInt(1, 1);
            stmt.setInt(2, 100);
            stmt.setInt(3, 100);
            stmt.setString(4, "abc");
            stmt.setInt(5, 101);
            stmt.setInt(6, 101);
            stmt.setString(7, "abc");
            stmt.setInt(8, 1);
            stmt.setInt(9, 100);
            stmt.setInt(10, 101);
            stmt.setInt(11, 1);
            boolean hasResult = stmt.execute();
            verifyExecuteKeepCurrent(stmt, hasResult);
        }
    }

    @Test
    public void test10PreparedStatementExecuteCloseAll() throws Exception {
        try (PreparedStatement stmt = conn.prepareStatement(preparedMixSql)) {
            //"select id from auto where 1=? order by id; insert into auto auto (id,ai,email) values(?,?,?),(?,?,?); select id from auto where 1=? order by id;delete from auto where id=? or id=?;select id from auto where 1=? order by id;";
            stmt.setInt(1, 1);
            stmt.setInt(2, 100);
            stmt.setInt(3, 100);
            stmt.setString(4, "abc");
            stmt.setInt(5, 101);
            stmt.setInt(6, 101);
            stmt.setString(7, "abc");
            stmt.setInt(8, 1);
            stmt.setInt(9, 100);
            stmt.setInt(10, 101);
            stmt.setInt(11, 1);
            boolean hasResult = stmt.execute();
            verifyExecuteCloseAll(stmt, hasResult);
        }
    }

    @Test
    public void test11PreparedStatementExecuteQueryCloseCurrent() throws Exception {
        try (PreparedStatement stmt = conn.prepareStatement(preparedSelectSql)) {
            //"select id from auto where id < ? and id < ? order by id; select id from auto where id < ? and id < ? order by id;select id from auto where id < ? and id < ? order by id;";
            stmt.setInt(1, 30);
            stmt.setInt(2, 50);
            stmt.setInt(3, 20);
            stmt.setInt(4, 50);
            stmt.setInt(5, 10);
            stmt.setInt(6, 50);
            ResultSet resultSet = stmt.executeQuery();
            verifyExecuteQueryCloseCurrent(stmt, resultSet);
        }
    }

    @Test
    public void test12PreparedStatementExecuteQueryKeepCurrent() throws Exception {
        try (PreparedStatement stmt = conn.prepareStatement(preparedSelectSql)) {
            //"select id from auto where id < ? and id < ? order by id; select id from auto where id < ? and id < ? order by id;select id from auto where id < ? and id < ? order by id;";
            stmt.setInt(1, 30);
            stmt.setInt(2, 50);
            stmt.setInt(3, 20);
            stmt.setInt(4, 50);
            stmt.setInt(5, 10);
            stmt.setInt(6, 50);
            ResultSet resultSet = stmt.executeQuery();
            verifyExecuteQueryKeepCurrent(stmt, resultSet);
        }
    }

    @Test
    public void test13PreparedStatementExecuteQueryCloseAll() throws Exception {
        try (PreparedStatement stmt = conn.prepareStatement(preparedSelectSql)) {
            //"select id from auto where id < ? and id < ? order by id; select id from auto where id < ? and id < ? order by id;select id from auto where id < ? and id < ? order by id;";
            stmt.setInt(1, 30);
            stmt.setInt(2, 50);
            stmt.setInt(3, 20);
            stmt.setInt(4, 50);
            stmt.setInt(5, 10);
            stmt.setInt(6, 50);
            ResultSet resultSet = stmt.executeQuery();
            verifyExecuteQueryCloseAll(stmt, resultSet);
        }
    }

    @Test
    public void test14PreparedStatementExecuteUpdate() throws Exception {
        try (PreparedStatement stmt = conn.prepareStatement(preparedDmlSql)) {
            //"insert into auto auto (id,ai,email) values(?,?,?),(?,?,?);insert into auto auto (id,ai,email) values(?,?,?);delete from auto where id in (?,?,?);";
            stmt.setInt(1, 100);
            stmt.setInt(2, 100);
            stmt.setString(3, "xxxx");
            stmt.setInt(4, 101);
            stmt.setInt(5, 101);
            stmt.setString(6, "xxxx");
            stmt.setInt(7, 102);
            stmt.setInt(8, 102);
            stmt.setString(9, "xxxx");
            stmt.setInt(10, 100);
            stmt.setInt(11, 101);
            stmt.setInt(12, 102);
            int affected = stmt.executeUpdate();
            verifyExecuteUpdate(stmt, affected);
        }
    }

    /*
     * mix
     * */
    @Test
    public void test15PreparedStatementMix() throws Exception {
        try (PreparedStatement stmt = conn.prepareStatement(preparedMixSql)) {
            //"select id from auto where 1=? order by id; insert into auto auto (id,ai,email) values(?,?,?),(?,?,?); select id from auto where 1=? order by id;delete from auto where id=? or id=?;select id from auto where 1=? order by id;";
            stmt.setInt(1, 1);
            stmt.setInt(2, 100);
            stmt.setInt(3, 100);
            stmt.setString(4, "abc");
            stmt.setInt(5, 101);
            stmt.setInt(6, 101);
            stmt.setString(7, "abc");
            stmt.setInt(8, 1);
            stmt.setInt(9, 100);
            stmt.setInt(10, 101);
            stmt.setInt(11, 1);
            boolean hasResult = stmt.execute();
            verifyExecuteCloseCurrent(stmt, hasResult);

            hasResult = stmt.execute(mixSql);
            verifyExecuteCloseCurrent(stmt, hasResult);

            hasResult = stmt.execute();
            verifyExecuteCloseCurrent(stmt, hasResult);

            ResultSet resultSet = stmt.executeQuery(selectSql);
            verifyExecuteQueryCloseCurrent(stmt, resultSet);

            hasResult = stmt.execute();
            verifyExecuteCloseCurrent(stmt, hasResult);

            int affected = stmt.executeUpdate(dmlSql);
            verifyExecuteUpdate(stmt, affected);
        }
    }

    /*
     * mix
     * */
    @Test
    public void test16CloseConnection() throws Exception {
        Statement stmt1 = conn.createStatement();
        Statement stmt2 = conn.createStatement();
        Statement stmt3 = conn.createStatement();

        PreparedStatement pstmt1 = conn.prepareStatement(preparedMixSql);
        PreparedStatement pstmt2 = conn.prepareStatement(preparedSelectSql);
        PreparedStatement pstmt3 = conn.prepareStatement(preparedDmlSql);

        stmt1.execute(mixSql);
        stmt2.executeQuery(selectSql);
        stmt3.executeUpdate(dmlSql);

        pstmt1.setInt(1, 1);
        pstmt1.setInt(2, 100);
        pstmt1.setInt(3, 100);
        pstmt1.setString(4, "abc");
        pstmt1.setInt(5, 101);
        pstmt1.setInt(6, 101);
        pstmt1.setString(7, "abc");
        pstmt1.setInt(8, 1);
        pstmt1.setInt(9, 100);
        pstmt1.setInt(10, 101);
        pstmt1.setInt(11, 1);
        pstmt1.execute();

        pstmt2.setInt(1, 30);
        pstmt2.setInt(2, 50);
        pstmt2.setInt(3, 20);
        pstmt2.setInt(4, 50);
        pstmt2.setInt(5, 10);
        pstmt2.setInt(6, 50);
        pstmt2.executeQuery();

        pstmt3.setInt(1, 100);
        pstmt3.setInt(2, 100);
        pstmt3.setString(3, "xxxx");
        pstmt3.setInt(4, 101);
        pstmt3.setInt(5, 101);
        pstmt3.setString(6, "xxxx");
        pstmt3.setInt(7, 102);
        pstmt3.setInt(8, 102);
        pstmt3.setString(9, "xxxx");
        pstmt3.setInt(10, 100);
        pstmt3.setInt(11, 101);
        pstmt3.setInt(12, 102);
        pstmt3.executeUpdate();

        assertFalse(stmt1.isClosed());
        assertFalse(stmt2.isClosed());
        assertFalse(stmt3.isClosed());
        assertFalse(pstmt1.isClosed());
        assertFalse(pstmt2.isClosed());
        assertFalse(pstmt3.isClosed());

        conn.close();

        assertTrue(stmt1.isClosed());
        assertTrue(stmt2.isClosed());
        assertTrue(stmt3.isClosed());
        assertTrue(pstmt1.isClosed());
        assertTrue(pstmt2.isClosed());
        assertTrue(pstmt3.isClosed());
    }
}
