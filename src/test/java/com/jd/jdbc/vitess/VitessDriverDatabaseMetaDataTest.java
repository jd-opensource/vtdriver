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

import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class VitessDriverDatabaseMetaDataTest extends TestSuite {
    protected Connection connection;

    @Before
    public void init() throws Exception {
        connection = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
    }

    @After
    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void testPstmtResultMetadata() {
        try (PreparedStatement pstmt = connection.prepareStatement("select f_tinyint,f_int from test")) {
            ResultSetMetaData metaData = pstmt.getMetaData();
            Assert.assertTrue(metaData.getColumnCount() == 2);
            Assert.assertTrue(metaData.getColumnName(1).equalsIgnoreCase("f_tinyint"));
            Assert.assertTrue(metaData.getColumnName(2).equalsIgnoreCase("f_int"));
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testResultMetadata() {
        try (
            PreparedStatement pstmt = connection.prepareStatement("select f_tinyint,f_int from test")) {
            ResultSet resultSet = pstmt.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            Assert.assertTrue(metaData.getColumnCount() == 2);
            Assert.assertTrue(metaData.getColumnName(1).equalsIgnoreCase("f_tinyint"));
            Assert.assertTrue(metaData.getColumnName(2).equalsIgnoreCase("f_int"));
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void testResultMetadata2() {
        try (
            Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("select f_tinyint,f_int from test");
            ResultSetMetaData metaData = resultSet.getMetaData();
            Assert.assertTrue(metaData.getColumnCount() == 2);
            Assert.assertTrue(metaData.getColumnName(1).equalsIgnoreCase("f_tinyint"));
            Assert.assertTrue(metaData.getColumnName(2).equalsIgnoreCase("f_int"));
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage(), false);
        }
    }

    @Test
    public void getSuperTypes() throws SQLException {
        DatabaseMetaData driverMetaData = connection.getMetaData();
        ResultSet rs = driverMetaData.getSuperTypes(null, null, null);
        ResultSetMetaData rsmd = rs.getMetaData();

        assertEquals(6, rsmd.getColumnCount());
        assertEquals("TYPE_CAT", rsmd.getColumnName(1));
        assertEquals("TYPE_SCHEM", rsmd.getColumnName(2));
        assertEquals("TYPE_NAME", rsmd.getColumnName(3));
        assertEquals("SUPERTYPE_CAT", rsmd.getColumnName(4));
        assertEquals("SUPERTYPE_SCHEM", rsmd.getColumnName(5));
        assertEquals("SUPERTYPE_NAME", rsmd.getColumnName(6));
    }

    @Test
    public void testGetAttributes() throws Exception {
        try (ResultSet rs = connection.getMetaData().getAttributes(null, null, null, null)) {
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void getSchemas() throws Exception {
        try (ResultSet rs = connection.getMetaData().getSchemas()) {
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void getSchemas2() throws Exception {
        try (ResultSet rs = connection.getMetaData().getSchemas(getCatalog(), null)) {
            while (rs.next()) {
                String table_name = rs.getString("TABLE_NAME");
                System.out.println(table_name);
            }
        }
    }

    protected String getCatalog() throws SQLException {
        return connection.getCatalog();
    }

    @Test
    public void getTableTypes() throws Exception {
        try (ResultSet rs = connection.getMetaData().getTableTypes()) {
            while (rs.next()) {
                String catalogName = rs.getString(1);
            }
        }
    }

    @Test
    public void getColumns() throws Exception {
        try (ResultSet rs = connection.getMetaData().getColumns(getCatalog(), null, null, null)) {
        }
    }

    @Test
    public void getColumnPrivileges() throws Exception {
        try (ResultSet rs = connection.getMetaData().getColumnPrivileges(getCatalog(), null, null, null)) {
            ResultSetMetaData rsMeta = rs.getMetaData();
            assertEquals(8, rsMeta.getColumnCount());
        }
    }

    @Test
    public void getTablePrivileges() throws Exception {
        try (ResultSet rs = connection.getMetaData().getTablePrivileges(null, null, null)) {
            ResultSetMetaData rsmd = rs.getMetaData();
        }
    }

    @Test
    public void getBestRowIdentifier() throws Exception {
        try (ResultSet rs = connection.getMetaData().getBestRowIdentifier(getCatalog(), null, "t_users", 1, true)) {
            ResultSetMetaData rsmd = rs.getMetaData();
            assertEquals(8, rsmd.getColumnCount());
            while (rs.next()) {
                Assert.assertEquals("id", rs.getString("COLUMN_NAME"));
            }
        }
    }

    @Test
    public void getTypeInfo() throws Exception {
        try (ResultSet info = connection.getMetaData().getTypeInfo()) {
            //Printing the column name and size
            while (info.next()) {
                System.out.println("Data type name: " + info.getString("TYPE_NAME"));
                System.out.println("Integer value representing this datatype: " + info.getInt("DATA_TYPE"));
                System.out.println("Maximum precision of this datatype: " + info.getInt("PRECISION"));
                if (info.getBoolean("CASE_SENSITIVE")) {
                    System.out.println("Current datatype is case sensitive ");
                } else {
                    System.out.println("Current datatype is not case sensitive ");
                }
                if (info.getBoolean("AUTO_INCREMENT")) {
                    System.out.println("Current datatype can be used for auto increment  ");
                } else {
                    System.out.println("Current datatype can not be used for auto increment  ");
                }
            }
        }
    }

    @Test
    public void getPrimaryKeys() throws Exception {
        ResultSet driverRs = connection.getMetaData().getPrimaryKeys(getCatalog(), null, "t_users");
        assertEquals(6, driverRs.getMetaData().getColumnCount());
        while (driverRs.next()) {
            Assert.assertEquals("id", driverRs.getString("COLUMN_NAME"));
        }
    }

    @Test
    public void getFunctionColumns() throws Exception {
        ResultSet driverRs = connection.getMetaData().getFunctionColumns(null, null, null, null);
        ResultSetMetaData rsMeta = driverRs.getMetaData();
        assertEquals(17, rsMeta.getColumnCount());
        assertEquals("FUNCTION_CAT", rsMeta.getColumnName(1));
        assertEquals("FUNCTION_SCHEM", rsMeta.getColumnName(2));
        assertEquals("FUNCTION_NAME", rsMeta.getColumnName(3));
        assertEquals("COLUMN_NAME", rsMeta.getColumnName(4));
        assertEquals("COLUMN_TYPE", rsMeta.getColumnName(5));
        assertEquals("DATA_TYPE", rsMeta.getColumnName(6));
        assertEquals("TYPE_NAME", rsMeta.getColumnName(7));
        assertEquals("PRECISION", rsMeta.getColumnName(8));
        assertEquals("LENGTH", rsMeta.getColumnName(9));
        assertEquals("SCALE", rsMeta.getColumnName(10));
        assertEquals("RADIX", rsMeta.getColumnName(11));
        assertEquals("NULLABLE", rsMeta.getColumnName(12));
        assertEquals("REMARKS", rsMeta.getColumnName(13));
        assertEquals("CHAR_OCTET_LENGTH", rsMeta.getColumnName(14));
        assertEquals("ORDINAL_POSITION", rsMeta.getColumnName(15));
        assertEquals("IS_NULLABLE", rsMeta.getColumnName(16));
        assertEquals("SPECIFIC_NAME", rsMeta.getColumnName(17));
    }

    @Test
    public void getFunctions() throws Exception {
        ResultSet driverRs = connection.getMetaData().getFunctions(null, null, null);
        ResultSetMetaData rsMeta = driverRs.getMetaData();
        assertEquals(6, rsMeta.getColumnCount());
        assertEquals("FUNCTION_CAT", rsMeta.getColumnName(1));
        assertEquals("FUNCTION_SCHEM", rsMeta.getColumnName(2));
        assertEquals("FUNCTION_NAME", rsMeta.getColumnName(3));
        assertEquals("REMARKS", rsMeta.getColumnName(4));
        assertEquals("FUNCTION_TYPE", rsMeta.getColumnName(5));
        assertEquals("SPECIFIC_NAME", rsMeta.getColumnName(6));
    }

    @Test
    public void getIndexInfo() throws Exception {
        ResultSet driverRs = connection.getMetaData().getIndexInfo(getCatalog(), null, "t_users", true, true);
        while (driverRs.next()) {
            Assert.assertEquals("id", driverRs.getString("COLUMN_NAME"));
        }
    }

    @Test
    public void getProcedures() throws Exception {
        ResultSet driverRs = connection.getMetaData().getProcedures(getCatalog(), null, null);
        assertFalse(driverRs.next());
    }

    @Test
    public void getTables() throws Exception {
        ResultSet driverRs = connection.getMetaData().getTables(getCatalog(), null, "t_users", null);
        while (driverRs.next()) {
            Assert.assertEquals("t_users", driverRs.getString("TABLE_NAME"));
        }
    }

    @Test
    public void getCatalogs() throws Exception {
        ResultSet driverRs = connection.getMetaData().getCatalogs();
        List<String> strings = Lists.newArrayList("information_schema", "mysql", "performance_schema", "sys");
        strings.add(getCatalog());
        int i = 0;
        while (driverRs.next()) {
            Assert.assertEquals(strings.get(i++), driverRs.getString("TABLE_CAT"));
        }
    }

    @Test
    public void getDriverVersion() throws Exception {
        try (Connection connection = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS))) {
            String driverVersion = connection.getMetaData().getDriverVersion();
            System.out.println(driverVersion);
        }
    }
}
