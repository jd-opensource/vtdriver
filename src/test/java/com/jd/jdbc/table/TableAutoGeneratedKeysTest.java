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

import com.jd.jdbc.vitess.VitessAutoGeneratedKeysTest;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import testsuite.internal.TestSuiteShardSpec;

public class TableAutoGeneratedKeysTest extends VitessAutoGeneratedKeysTest {

    @Before
    @Override
    public void testLoadDriver() throws Exception {
        getConn();
        TableTestUtil.setSplitTableConfig("table/autoGeneratedKeys.yml");
        clean();

        sql1 = "insert into table_auto (id,ai,email) values(1,1,'x')";
        sql100 = "insert into table_auto (id,ai,email) values(1,100,'x')";
        sqlx = "insert into table_auto (id,ai,email) values(?,?,'x')";
        sqld = "insert into table_auto (id,ai,email) values(%d,%d,'x')";
        updateSql = "update table_auto set email = 'zz' where id = %d";
        deleteSql = "delete from table_auto where id = %d";
        updateSql100 = "update table_auto set email = 'zz' where id = 1";
        sql200 = "insert into table_auto (id,ai,email) values(200,200,'x')";
        updatesql200 = "update table_auto set email = 'zz' where id = ?";
    }

    protected void getConn() throws SQLException {
        String url = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        conn = DriverManager.getConnection(url);
    }

    @After
    public void close() throws Exception {
        closeConnection(conn);
        TableTestUtil.setDefaultTableConfig();
    }

    @Override
    protected void clean() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("delete from table_auto");
        }
    }

    @Test
    public void test23SetNull() throws Exception {
        clean();
        String sql = sqlx;
        try (PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            stmt.setInt(1, 100);
            stmt.setInt(2, 1000);

            int updateCount = stmt.executeUpdate();

            Assert.assertEquals(1, updateCount);
            Assert.assertEquals(1, stmt.getUpdateCount());

            ResultSet generatedKeys = stmt.getGeneratedKeys();
            int count = 0;
            while (generatedKeys.next()) {
                Assert.assertEquals(1000, generatedKeys.getLong(1));
                count++;
            }
            Assert.assertEquals(1, count);

            //set null
            stmt.setInt(1, 100);
            stmt.setNull(2, 0);
            thrown.expect(SQLException.class);
            thrown.expectMessage("cannot calculate split table, logic table: table_auto； shardingColumnValue: null");
            stmt.executeUpdate();
        }
    }

}
