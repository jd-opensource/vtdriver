/*
Copyright 2021 JD Project Authors. Licensed under Apache-2.0.

Copyright 2019 The Vitess Authors.

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

import com.jd.jdbc.VSchemaManager;
import com.jd.jdbc.common.Constant;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.context.VtContext;
import com.jd.jdbc.discovery.HealthCheck;
import com.jd.jdbc.pool.InnerConnection;
import com.jd.jdbc.pool.StatefulConnectionPool;
import com.jd.jdbc.queryservice.IParentQueryService;
import com.jd.jdbc.queryservice.RoleType;
import com.jd.jdbc.queryservice.TabletDialer;
import com.jd.jdbc.queryservice.util.RoleUtils;
import com.jd.jdbc.session.SafeSession;
import com.jd.jdbc.session.VitessSession;
import com.jd.jdbc.sqlparser.ast.statement.SQLCommitStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLRollbackStatement;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqlparser.utils.Utils;
import com.jd.jdbc.srvtopo.Resolver;
import com.jd.jdbc.tindexes.ActualTable;
import com.jd.jdbc.tindexes.LogicTable;
import com.jd.jdbc.topo.TopoServer;
import com.jd.jdbc.util.KeyspaceUtil;
import com.jd.jdbc.util.TimeUtil;
import com.jd.jdbc.vitess.metadata.CachedDatabaseMetaData;
import com.jd.jdbc.vitess.metadata.VitessDatabaseMetaData;
import com.jd.jdbc.vitess.mysql.VitessPropertyKey;
import com.mysql.cj.jdbc.JdbcConnection;
import io.vitess.proto.Topodata;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.Getter;
import lombok.Setter;

public class VitessConnection extends AbstractVitessConnection {
    public static final Integer MAX_MEMORY_ROWS = 300000;

    private static final Log log = LogFactory.getLog(VitessConnection.class);

    private static final Map<String, CachedDatabaseMetaData> vitessMetaDataInfoMap = new ConcurrentHashMap<>();

    @Getter
    public Map<String, Object> serverSessionPropertiesMap = new ConcurrentHashMap<>();

    @Getter
    private final Resolver resolver;

    private final CopyOnWriteArrayList<Statement> openStatements = new CopyOnWriteArrayList<>();

    @Getter
    VSchemaManager vm;

    private boolean isClosed;

    @Getter
    private com.jd.jdbc.Executor executor;

    @Getter
    private IContext ctx;

    @Getter
    private Properties properties;

    @Getter
    private String url;

    @Getter
    @Setter
    private VitessSession session;

    @Getter
    private String defaultKeyspace;

    public VitessConnection(String url, Properties prop, TopoServer topoServer, Resolver resolver, VSchemaManager vSchemaManager, String defaultKeyspace) throws SQLException {
        this.isClosed = false;
        this.url = url;
        this.properties = prop;
        this.resolver = resolver;
        this.executor = com.jd.jdbc.Executor.getInstance(Utils.getInteger(prop, "vtPlanCacheCapacity"));
        this.vm = vSchemaManager;
        this.ctx = VtContext.withCancel(VtContext.background());
        this.ctx.setContextValue(Constant.DRIVER_PROPERTY_ROLE_KEY, getRoleType(prop));
        this.ctx.setContextValue(ContextKey.CTX_TOPOSERVER, topoServer);
        this.ctx.setContextValue(ContextKey.CTX_SCATTER_CONN, resolver.getScatterConn());
        this.ctx.setContextValue(ContextKey.CTX_TX_CONN, resolver.getScatterConn().getTxConn());
        this.ctx.setContextValue(ContextKey.CTX_VSCHEMA_MANAGER, this.vm);

        this.defaultKeyspace = defaultKeyspace;
        VitessSession vitessSession = new VitessSession();
        vitessSession.setAutocommit(true);
        this.session = vitessSession;
        buildServerSessionPropertiesMap();
        if (log.isDebugEnabled()) {
            log.debug("create VitessConnection");
        }
    }

    public VitessConnection(Resolver resolver, VitessSession session) {
        this.resolver = resolver;
        this.session = session;
    }

    public Object getConnectionMutex() {
        return this;
    }

    public void checkClosed() throws SQLException {
        if (this.isClosed) {
            throw new SQLException("connection has been closed");
        }
    }

    public void registerStatement(Statement stmt) {
        this.openStatements.addIfAbsent(stmt);
    }

    public void unregisterStatement(Statement stmt) {
        this.openStatements.remove(stmt);
    }

    private void closeAllOpenStatements() throws SQLException {
        SQLException exp = null;

        for (Statement stmt : this.openStatements) {
            try {
                stmt.close();
            } catch (SQLException e) {
                exp = e;
            }
        }

        if (exp != null) {
            throw exp;
        }
    }

    @Override
    public Statement createStatement() throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();

            VitessStatement vitessStatement = new VitessStatement(this, executor);
            registerStatement(vitessStatement);
            return vitessStatement;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();
            VitessPreparedStatement vitessPreparedStatement = new VitessPreparedStatement(sql, this, executor);
            registerStatement(vitessPreparedStatement);
            return vitessPreparedStatement;
        }
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return null;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();
            return this.session.getAutocommit();
        }
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();

            try {
                // false --> true
                if (autoCommit && !this.session.getAutocommit()) {
                    commit();
                }
            } finally {
                this.session.setAutocommit(autoCommit);
            }
        }
    }

    @Override
    public void commit() throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();
            if (this.session.getAutocommit()) {
                throw new SQLException("Can't call commit when autocommit=true");
            }
            this.executor.execute(this.ctx, "", SafeSession.newSafeSession(this), defaultKeyspace, new SQLCommitStatement(), null);
        }
    }

    @Override
    public void rollback() throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();
            if (this.session.getAutocommit()) {
                throw new SQLException("Can't call rollback when autocommit=true");
            }
            this.executor.execute(this.ctx, "", SafeSession.newSafeSession(this), defaultKeyspace, new SQLRollbackStatement(), null);
        }
    }

    @Override
    public void close() throws SQLException {
        synchronized (getConnectionMutex()) {
            if (!isClosed) {
                ctx.close();
                closeAllOpenStatements();
                //leave toposerver/hc/topowatcher/monitorServer alive
                isClosed = true;
            }
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        synchronized (getConnectionMutex()) {
            return isClosed;
        }
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();
            return new VitessDatabaseMetaData(this);
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public String getCatalog() throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();
            return defaultKeyspace;
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_REPEATABLE_READ;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        if (Connection.TRANSACTION_REPEATABLE_READ != level) {
            throw new SQLFeatureNotSupportedException("only support TRANSACTION_REPEATABLE_READ ");
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        if (ResultSet.TYPE_FORWARD_ONLY != resultSetType || ResultSet.CONCUR_READ_ONLY != resultSetConcurrency) {
            throw new SQLFeatureNotSupportedException("only support TYPE_FORWARD_ONLY CONCUR_READ_ONLY statement");
        }

        synchronized (getConnectionMutex()) {
            checkClosed();

            VitessStatement vitessStatement = new VitessStatement(this, executor);
            registerStatement(vitessStatement);
            return vitessStatement;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        if (ResultSet.TYPE_FORWARD_ONLY != resultSetType || ResultSet.CONCUR_READ_ONLY != resultSetConcurrency) {
            throw new SQLFeatureNotSupportedException("only support TYPE_FORWARD_ONLY CONCUR_READ_ONLY statement");
        }

        synchronized (getConnectionMutex()) {
            checkClosed();
            VitessPreparedStatement vitessPreparedStatement = new VitessPreparedStatement(sql, this, executor);
            registerStatement(vitessPreparedStatement);
            return vitessPreparedStatement;
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        if (ResultSet.HOLD_CURSORS_OVER_COMMIT != holdability) {
            throw new SQLFeatureNotSupportedException("only support HOLD_CURSORS_OVER_COMMIT");
        }
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (ResultSet.TYPE_FORWARD_ONLY != resultSetType || ResultSet.CONCUR_READ_ONLY != resultSetConcurrency || ResultSet.HOLD_CURSORS_OVER_COMMIT != resultSetHoldability) {
            throw new SQLFeatureNotSupportedException("only support TYPE_FORWARD_ONLY CONCUR_READ_ONLY HOLD_CURSORS_OVER_COMMIT statement");
        }

        synchronized (getConnectionMutex()) {
            checkClosed();

            VitessStatement vitessStatement = new VitessStatement(this, executor);
            registerStatement(vitessStatement);
            return vitessStatement;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (ResultSet.TYPE_FORWARD_ONLY != resultSetType || ResultSet.CONCUR_READ_ONLY != resultSetConcurrency || ResultSet.HOLD_CURSORS_OVER_COMMIT != resultSetHoldability) {
            throw new SQLFeatureNotSupportedException("only support TYPE_FORWARD_ONLY CONCUR_READ_ONLY HOLD_CURSORS_OVER_COMMIT statement");
        }

        synchronized (getConnectionMutex()) {
            checkClosed();
            VitessPreparedStatement vitessPreparedStatement = new VitessPreparedStatement(sql, this, executor);
            registerStatement(vitessPreparedStatement);
            return vitessPreparedStatement;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();

            VitessPreparedStatement vitessPreparedStatement = new VitessPreparedStatement(sql, this, executor);
            registerStatement(vitessPreparedStatement);
            vitessPreparedStatement.setRetrieveGeneratedKeys(autoGeneratedKeys == java.sql.Statement.RETURN_GENERATED_KEYS);
            return vitessPreparedStatement;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();

            VitessPreparedStatement vitessPreparedStatement = new VitessPreparedStatement(sql, this, executor);
            registerStatement(vitessPreparedStatement);
            vitessPreparedStatement.setRetrieveGeneratedKeys((columnIndexes != null) && (columnIndexes.length > 0));
            return vitessPreparedStatement;
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();

            VitessPreparedStatement vitessPreparedStatement = new VitessPreparedStatement(sql, this, executor);

            vitessPreparedStatement.setRetrieveGeneratedKeys((columnNames != null) && (columnNames.length > 0));
            return vitessPreparedStatement;
        }
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return !isClosed();
    }

    @Override
    public String getSchema() throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();
            return defaultKeyspace;
        }
    }

    public CachedDatabaseMetaData getCachedDatabaseMetaData() throws SQLException {
        CachedDatabaseMetaData cachedDatabaseMetaData = vitessMetaDataInfoMap.get(defaultKeyspace);
        if (cachedDatabaseMetaData != null) {
            return cachedDatabaseMetaData;
        }
        synchronized (VitessConnection.class) {
            cachedDatabaseMetaData = vitessMetaDataInfoMap.get(defaultKeyspace);
            if (cachedDatabaseMetaData != null) {
                return cachedDatabaseMetaData;
            }
            try (InnerConnection innerConnection = StatefulConnectionPool.getJdbcConnection(defaultKeyspace, RoleUtils.getTabletType(ctx))) {
                Connection connection = innerConnection.getConnection();
                cachedDatabaseMetaData = new CachedDatabaseMetaData(connection.getMetaData());
                vitessMetaDataInfoMap.putIfAbsent(defaultKeyspace, cachedDatabaseMetaData);
                return cachedDatabaseMetaData;
            }
        }
    }

    public void closeInnerConnection() {
        List<Topodata.Tablet> tabletList = HealthCheck.INSTANCE.getHealthyTablets(KeyspaceUtil.getLogicSchema(defaultKeyspace));
        for (Topodata.Tablet tablet : tabletList) {
            IParentQueryService queryService = TabletDialer.dial(tablet);
            queryService.closeNativeQueryService();
        }
    }

    private void buildServerSessionPropertiesMap() throws SQLException {
        try (InnerConnection innerConnection = StatefulConnectionPool.getJdbcConnection(defaultKeyspace, RoleUtils.getTabletType(ctx))) {
            JdbcConnection connectionImpl = innerConnection.getConnectionImpl();
            Integer maxAllowedPacket = connectionImpl.getPropertySet().getIntegerProperty(VitessPropertyKey.MAX_ALLOWED_PACKET.getKeyName()).getValue();
            serverSessionPropertiesMap.put(VitessPropertyKey.MAX_ALLOWED_PACKET.getKeyName(), maxAllowedPacket);
        }
        serverSessionPropertiesMap.put("DEFAULT_TIME_ZONE", TimeZone.getDefault());
        serverSessionPropertiesMap.put(VitessPropertyKey.SERVER_TIMEZONE.getKeyName(), TimeUtil.getTimeZone(this.properties));
    }

    public String getVindex(String table) {
        return vm.getVindex(defaultKeyspace, table);
    }

    public String getVschemaColumnVindex(String tableName) {
        return vm.getVindex(this.defaultKeyspace, tableName);
    }

    public String getShardingColumnName(String logicTableName) {
        LogicTable logicTable = VitessDataSource.getLogicTable(this.defaultKeyspace, logicTableName);
        if (logicTable == null) {
            return null;
        }
        return logicTable.getTindexCol().getColumnName();
    }

    public List<String> getActualTables(String logicTableName) {
        LogicTable logicTable = VitessDataSource.getLogicTable(this.defaultKeyspace, logicTableName);
        if (logicTable == null) {
            return null;
        }
        List<String> actualTables = new ArrayList<>(logicTable.getActualTableList().size());
        for (ActualTable actualTable : logicTable.getActualTableList()) {
            actualTables.add(actualTable.getActualTableName());
        }
        return actualTables;
    }

    private RoleType getRoleType(Properties prop) throws SQLException {
        String role = prop.getProperty(Constant.DRIVER_PROPERTY_ROLE_KEY, Constant.DRIVER_PROPERTY_ROLE_RW);
        return RoleUtils.buildRoleType(role);
    }

    public enum ContextKey {
        CTX_TOPOSERVER,
        CTX_SCATTER_CONN,
        CTX_TX_CONN,
        CTX_VSCHEMA_MANAGER
    }
}
