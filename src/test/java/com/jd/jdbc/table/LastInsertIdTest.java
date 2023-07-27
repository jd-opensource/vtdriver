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

package com.jd.jdbc.table;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

public class LastInsertIdTest extends TestSuite {


    protected Connection driverConnection;

    protected int effectRows;

    @Before
    public void init() throws SQLException {
        getConn();
    }

    protected void getConn() throws SQLException {
        driverConnection = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
    }

    @Test
    public void splitTableSingleValueTest() throws SQLException {
        try (Statement stmt = driverConnection.createStatement()) {
            stmt.executeUpdate("delete from table_engine_test where id = 999999");
            effectRows = stmt.executeUpdate("insert into table_engine_test(id, f_key) values (999999, 'ABC')");
            lastInsertId(stmt, 999999, 1, true);
        }
    }

    @Test
    public void splitTableMutiValueTest() throws SQLException {
        try (Statement stmt = driverConnection.createStatement()) {
            stmt.executeUpdate("delete from table_engine_test");
            String insertEngineTest = "insert into table_engine_test(id, f_key) values ";
            for (int i = 1; i < 50; ++i) {
                insertEngineTest += "( " + i + ", '" + i + "'), ";
            }
            insertEngineTest += "( 51, '51');";
            effectRows = stmt.executeUpdate(insertEngineTest);
            lastInsertId(stmt, 51, 50, false);
        }
    }

    private void lastInsertId(final Statement stmt, final Object expectedValue, final int expectedEffectRows, final boolean isSingleValue) throws SQLException {
        Assert.assertEquals(expectedEffectRows, effectRows);
        if (isSingleValue) {
            // last insert id
            this.testByQuery(stmt, "select last_insert_id()", "last_insert_id()", expectedValue);
            this.testByQuery(stmt, "select LAST_INSERT_ID()", "LAST_INSERT_ID()", expectedValue);
            this.testByQuery(stmt, "select last_insert_id() as id", "id", expectedValue);
            this.testByQuery(stmt, "select LAST_INSERT_ID() as lastId", "lastId", expectedValue);

            // @@identity
            this.testByQuery(stmt, "select @@identity", "@@identity", expectedValue);
            this.testByQuery(stmt, "select @@IDENTITY", "@@IDENTITY", expectedValue);
            this.testByQuery(stmt, "select @@identity as id", "id", expectedValue);
            this.testByQuery(stmt, "select @@IDENTITY as idx", "idx", expectedValue);
        }
    }

    private void testByQuery(final Statement stmt, final String query, final String columnLabel, final Object expectedValue) throws SQLException {
        ResultSet rs = stmt.executeQuery(query);
        rs.next();
        Assert.assertEquals(expectedValue, rs.getObject(columnLabel, expectedValue.getClass()));
        rs.close();

        rs = stmt.executeQuery(query);
        rs.next();
        Assert.assertEquals(expectedValue, rs.getObject(columnLabel, expectedValue.getClass()));
        rs.close();

        rs = stmt.executeQuery(query);
        rs.next();
        Assert.assertEquals(expectedValue, rs.getObject(columnLabel, expectedValue.getClass()));
        rs.close();

        rs = stmt.executeQuery(query);
        rs.next();
        Assert.assertEquals(expectedValue, rs.getObject(columnLabel, expectedValue.getClass()));
        rs.close();
    }

    @Test
    @Ignore
    public void testResultIdBigInteger() throws SQLException {
        String insertSql = "insert into `type_test` (`id`, `f_key`, `f_decimal`) values('17000098931012360000', 'x', 2)";
        String initSql = "delete from type_test";
        BigInteger expectedValue = new BigInteger("17000098931012360000");

        try (Statement stmt = driverConnection.createStatement()) {
            stmt.executeUpdate(initSql);
            effectRows = stmt.executeUpdate(insertSql);
            lastInsertId(stmt, expectedValue, 1, true);
        }
    }
}
