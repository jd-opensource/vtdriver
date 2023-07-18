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

package com.jd.jdbc.vitess.metadata;

import com.jd.jdbc.pool.InnerConnection;
import com.jd.jdbc.pool.StatefulConnectionPool;
import com.jd.jdbc.queryservice.util.RoleUtils;
import com.jd.jdbc.util.KeyspaceUtil;
import com.jd.jdbc.vitess.VitessConnection;
import com.jd.jdbc.vitess.resultset.DatabaseMetaDataResultSet;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static com.jd.jdbc.common.Constant.DRIVER_MAJOR_VERSION;
import static com.jd.jdbc.common.Constant.DRIVER_MINOR_VERSION;
import static com.jd.jdbc.common.Constant.DRIVER_NAME;

public class VitessDatabaseMetaData extends AbstractDatabaseMetaData {
    private static String version;

    static {
        Properties properties = new Properties();
        try {
            properties.load(VitessDatabaseMetaData.class.getClassLoader().getResourceAsStream("vtdriver-version.properties"));
            if (!properties.isEmpty()) {
                version = properties.getProperty("version");
            }
        } catch (IOException e) {
        }
    }

    private final VitessConnection connection;

    public VitessDatabaseMetaData(VitessConnection connection) throws SQLException {
        super(connection.getCachedDatabaseMetaData());
        this.connection = connection;
    }

    public static String getVersion() {
        return version;
    }

    @Override
    public String getDriverName() throws SQLException {
        return DRIVER_NAME;
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return getVersion();
    }

    @Override
    public int getDriverMajorVersion() {
        return DRIVER_MAJOR_VERSION;
    }

    @Override
    public int getDriverMinorVersion() {
        return DRIVER_MINOR_VERSION;
    }

    @Override
    public final String getURL() {
        return connection.getUrl();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getProcedures(getCatalog(catalog), schemaPattern, procedureNamePattern);
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getProcedureColumns(getCatalog(catalog), schemaPattern, procedureNamePattern, columnNamePattern);
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getTables(getCatalog(catalog), schemaPattern, tableNamePattern, types);
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getSchemas();
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getCatalogs();
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getTableTypes();
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getColumns(getCatalog(catalog), schemaPattern, tableNamePattern, columnNamePattern);
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getColumnPrivileges(getCatalog(catalog), getSchema(schema), table, columnNamePattern);
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getTablePrivileges(getCatalog(catalog), schemaPattern, tableNamePattern);
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getBestRowIdentifier(getCatalog(catalog), getSchema(schema), table, scope, nullable);
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getVersionColumns(getCatalog(catalog), getSchema(schema), table);
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getPrimaryKeys(getCatalog(catalog), getSchema(schema), table);
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getImportedKeys(getCatalog(catalog), getSchema(schema), table);
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getExportedKeys(getCatalog(catalog), getSchema(schema), table);
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getCrossReference(parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable);
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getTypeInfo();
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getIndexInfo(getCatalog(catalog), getSchema(schema), table, unique, approximate);
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getUDTs(getCatalog(catalog), schemaPattern, typeNamePattern, types);
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getSuperTypes(getCatalog(catalog), schemaPattern, typeNamePattern);
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getSuperTables(getCatalog(catalog), schemaPattern, tableNamePattern);
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getAttributes(getCatalog(catalog), schemaPattern, typeNamePattern, attributeNamePattern);
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getSchemas(getCatalog(catalog), schemaPattern);
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getClientInfoProperties();
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getFunctions(getCatalog(catalog), schemaPattern, functionNamePattern);
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getFunctionColumns(getCatalog(catalog), schemaPattern, functionNamePattern, columnNamePattern);
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        try (InnerConnection innerConnection = getJdbcConnection()) {
            DatabaseMetaData metaData = getDatabaseMetaData(innerConnection);
            ResultSet resultSet = metaData.getPseudoColumns(getCatalog(catalog), schemaPattern, tableNamePattern, columnNamePattern);
            return new DatabaseMetaDataResultSet(resultSet);
        }
    }

    private InnerConnection getJdbcConnection() throws SQLException {
        return StatefulConnectionPool.getJdbcConnection(connection.getDefaultKeyspace(), RoleUtils.getTabletType(connection.getCtx()));
    }

    private DatabaseMetaData getDatabaseMetaData(InnerConnection innerConnection) throws SQLException {
        Connection connection = innerConnection.getConnection();
        return connection.getMetaData();
    }

    private String getCatalog(String catalog) {
        return KeyspaceUtil.getRealSchema(catalog);
    }

    private String getSchema(String schema) {
        return KeyspaceUtil.getRealSchema(schema);
    }
}