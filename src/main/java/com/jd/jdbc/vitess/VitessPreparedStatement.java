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

import com.jd.jdbc.Executor;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.context.VtContext;
import com.jd.jdbc.pool.InnerConnection;
import com.jd.jdbc.pool.StatefulConnectionPool;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.vitess.mysql.VitessPropertyKey;
import com.jd.jdbc.vitess.mysql.VitessQueryBindVariable;
import com.mysql.cj.ParseInfo;
import com.mysql.cj.Session;
import com.mysql.cj.jdbc.JdbcConnection;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;

import static com.jd.jdbc.common.Constant.DRIVER_PROPERTY_ROLE_KEY;

public class VitessPreparedStatement extends AbstractVitessPreparedStatement {
    private static final Log LOGGER = LogFactory.getLog(VitessPreparedStatement.class);

    private final List<String> sqls;

    /*
     * sqls = insert into (?,?,?,?); select (?,?,?);
     *         V
     * clientPreparedQueryBindingsList = [v1,v2,v3,v4], [v5,v6,v7]
     *         V
     * clientPreparedQueryBindingsIndexes = [0,0],[0,1],[0,2],[0,3],[1,0],[1,1],[1,2]
     *         V
     * setXXX(6, xxx);
     *         V
     * clientPreparedQueryBindingsIndexes[6-1] --> [1,1] --> clientPreparedQueryBindingsList[1][1] --> v6
     * */
    private final List<VitessQueryBindVariable> clientPreparedQueryBindingsList = new ArrayList<>();

    private final List<VitessPreparedIndices> clientPreparedQueryBindingsIndexes = new ArrayList<>();

    private final String inputSQL;

    public VitessPreparedStatement(String inputSQL, VitessConnection connection, Executor executor) throws SQLException {
        super(connection, executor);
        LOGGER.debug("prepared statement: " + inputSQL);
        this.inputSQL = inputSQL;
        this.sqls = split(inputSQL);

        for (int i = 0; i < sqls.size(); i++) {
            try (InnerConnection innerConnection = StatefulConnectionPool.getJdbcConnection(this.connection.getDefaultKeyspace(),
                (Topodata.TabletType) context.getContextValue(DRIVER_PROPERTY_ROLE_KEY))) {
                JdbcConnection connectionImpl = innerConnection.getConnectionImpl();
                String encoding = this.connection.getProperties().getProperty(VitessPropertyKey.CHARACTER_ENCODING.getKeyName());
                Session session = connectionImpl.getSession();
                ParseInfo parseInfo = new ParseInfo(sqls.get(i), session, encoding);
                int parameterCount = parseInfo.getStaticSql().length - 1;
                this.clientPreparedQueryBindingsList.add(new VitessQueryBindVariable(parameterCount, session, encoding));
                for (int p = 0; p < parameterCount; p++) {
                    this.clientPreparedQueryBindingsIndexes.add(new VitessPreparedIndices(i, p));
                }
            }
        }
    }

    public List<VitessQueryBindVariable> getClientPreparedQueryBindings() {
        return clientPreparedQueryBindingsList;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        LOGGER.debug("prepared statement execute: " + sqls);

        cleanResultSets();

        ResultSet returnResultSet;
        synchronized (checkClosed().getConnectionMutex()) {
            try (IContext ctx = this.queryTimeout == 0 ? VtContext.withCancel(this.context) : VtContext.withDeadline(this.context, this.queryTimeout, TimeUnit.SECONDS)) {
                if (sqls.size() > 1) {
                    List<Map<String, Query.BindVariable>> bindVariableMapList = clientPreparedQueryBindingsList.stream().map(VitessQueryBindVariable::getBindVariableMap).collect(Collectors.toList());
                    returnResultSet = executeMultiQueryInternal(ctx, sqls, bindVariableMapList, this.retrieveGeneratedKeys);
                    return returnResultSet;
                }

                if (streamResults()) {
                    returnResultSet = executeStreamQueryInternal(ctx, sqls.get(0), clientPreparedQueryBindingsList.get(0).getBindVariableMap(), this.retrieveGeneratedKeys);
                } else {
                    returnResultSet = executeQueryInternal(ctx, sqls.get(0), clientPreparedQueryBindingsList.get(0).getBindVariableMap(), this.retrieveGeneratedKeys);
                }
                return returnResultSet;
            }
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        LOGGER.debug("prepared statement execute: " + sqls);

        cleanResultSets();

        int rc;
        synchronized (checkClosed().getConnectionMutex()) {
            try (IContext ctx = this.queryTimeout == 0 ? VtContext.withCancel(this.context) : VtContext.withDeadline(this.context, this.queryTimeout, TimeUnit.SECONDS)) {
                if (sqls.size() > 1) {
                    List<Map<String, Query.BindVariable>> bindVariableMapList = clientPreparedQueryBindingsList.stream().map(VitessQueryBindVariable::getBindVariableMap).collect(Collectors.toList());
                    rc = executeMultiQueryUpdateInternal(ctx, sqls, bindVariableMapList, true, this.retrieveGeneratedKeys);
                    return rc;
                }
                rc = executeUpdateInternal(ctx, sqls.get(0), clientPreparedQueryBindingsList.get(0).getBindVariableMap(), true, this.retrieveGeneratedKeys);
            }
        }

        return rc;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setNull(vitessPreparedIndices.getParameterIndex());
        }
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setBoolean(vitessPreparedIndices.getParameterIndex(), x);
        }
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setByte(vitessPreparedIndices.getParameterIndex(), x);
        }
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setShort(vitessPreparedIndices.getParameterIndex(), x);
        }
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setInt(vitessPreparedIndices.getParameterIndex(), x);
        }
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setLong(vitessPreparedIndices.getParameterIndex(), x);
        }
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setFloat(vitessPreparedIndices.getParameterIndex(), x);
        }
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setDouble(vitessPreparedIndices.getParameterIndex(), x);
        }
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setBigDecimal(vitessPreparedIndices.getParameterIndex(), x);
        }
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setString(vitessPreparedIndices.getParameterIndex(), x);
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setBytes(vitessPreparedIndices.getParameterIndex(), x);
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setDate(vitessPreparedIndices.getParameterIndex(), x);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setTime(vitessPreparedIndices.getParameterIndex(), x);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setTimestamp(vitessPreparedIndices.getParameterIndex(), x);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setAsciiStream(vitessPreparedIndices.getParameterIndex(), x);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setBinaryStream(vitessPreparedIndices.getParameterIndex(), x);
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            clientPreparedQueryBindingsList.forEach(VitessQueryBindVariable::clearBindValues);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {

        if (x == null) {
            setNull(parameterIndex, Types.NULL);
            return;
        }

        if (x instanceof Boolean) {
            setBoolean(parameterIndex, (Boolean) x);
            return;
        }
        if (x instanceof Byte) {
            setByte(parameterIndex, (Byte) x);
            return;
        }
        if (x instanceof Short) {
            setShort(parameterIndex, (Short) x);
            return;
        }
        if (x instanceof Integer) {
            setInt(parameterIndex, (Integer) x);
            return;
        }
        if (x instanceof Long) {
            setLong(parameterIndex, (Long) x);
            return;
        }
        if (x instanceof Float) {
            setFloat(parameterIndex, (Float) x);
            return;
        }
        if (x instanceof Double) {
            setDouble(parameterIndex, (Double) x);
            return;
        }
        if (x instanceof BigDecimal) {
            setBigDecimal(parameterIndex, (BigDecimal) x);
            return;
        }
        if (x instanceof String) {
            setString(parameterIndex, (String) x);
            return;
        }
        if (x instanceof byte[]) {
            setBytes(parameterIndex, (byte[]) x);
            return;
        }
        if (x instanceof Date) {
            setDate(parameterIndex, (Date) x);
            return;
        }
        if (x instanceof Time) {
            setTime(parameterIndex, (Time) x);
            return;
        }
        if (x instanceof Timestamp) {
            setTimestamp(parameterIndex, (Timestamp) x);
            return;
        }
        if (x instanceof Blob) {
            setBlob(parameterIndex, (Blob) x);
            return;
        }
        if (x instanceof Clob) {
            setClob(parameterIndex, (Clob) x);
            return;
        }
        throw new SQLFeatureNotSupportedException("not supported " + x.getClass().getName());
    }

    @Override
    public boolean execute() throws SQLException {
        LOGGER.debug("prepared statement execute: " + sqls);

        cleanResultSets();

        synchronized (checkClosed().getConnectionMutex()) {
            try (IContext ctx = this.queryTimeout == 0 ? VtContext.withCancel(this.context) : VtContext.withDeadline(this.context, this.queryTimeout, TimeUnit.SECONDS)) {
                if (sqls.size() > 1) {
                    List<Map<String, Query.BindVariable>> bindVariableMapList = clientPreparedQueryBindingsList.stream().map(VitessQueryBindVariable::getBindVariableMap).collect(Collectors.toList());
                    executeMultiQueryInternal(ctx, sqls, bindVariableMapList, this.retrieveGeneratedKeys);
                    return getExecuteInternalResult();
                }

                executeInternal(ctx, sqls.get(0), clientPreparedQueryBindingsList.get(0).getBindVariableMap(), this.retrieveGeneratedKeys);
                return getExecuteInternalResult();
            }
        }
    }

    @Override
    public void addBatch() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            //batch does not support multiple queries
            if (batchSqls == null) {
                batchSqls = new ArrayList<>();
            }
            batchSqls.add(sqls.get(0));
            if (bindVariableMapList == null) {
                bindVariableMapList = new ArrayList<>();
            }
            bindVariableMapList.add(clientPreparedQueryBindingsList.get(0).getBindVariableMap());
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setCharacterStream(vitessPreparedIndices.getParameterIndex(), reader, length);
        }
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setBlob(vitessPreparedIndices.getParameterIndex(), x);
        }
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setClob(vitessPreparedIndices.getParameterIndex(), x);
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            try (InnerConnection innerConnection = StatefulConnectionPool
                .getJdbcConnection(this.connection.getDefaultKeyspace(), (Topodata.TabletType) context.getContextValue(DRIVER_PROPERTY_ROLE_KEY))) {
                PreparedStatement pstmt = innerConnection.getConnection().prepareStatement(inputSQL);
                return pstmt.getMetaData();
            }
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setDate(vitessPreparedIndices.getParameterIndex(), x, cal);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setTime(vitessPreparedIndices.getParameterIndex(), x, cal);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setTimestamp(vitessPreparedIndices.getParameterIndex(), x, cal);
        }
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setNString(vitessPreparedIndices.getParameterIndex(), value);
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setNCharacterStream(vitessPreparedIndices.getParameterIndex(), value, length);
        }
    }


    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setNClob(vitessPreparedIndices.getParameterIndex(), value);
        }
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setClob(vitessPreparedIndices.getParameterIndex(), reader, length);
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setBlob(vitessPreparedIndices.getParameterIndex(), inputStream, length);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setNClob(vitessPreparedIndices.getParameterIndex(), reader, length);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setAsciiStream(vitessPreparedIndices.getParameterIndex(), x, length);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setBinaryStream(vitessPreparedIndices.getParameterIndex(), x, length);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setCharacterStream(vitessPreparedIndices.getParameterIndex(), reader, length);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setAsciiStream(vitessPreparedIndices.getParameterIndex(), x);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setBinaryStream(vitessPreparedIndices.getParameterIndex(), x);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setCharacterStream(vitessPreparedIndices.getParameterIndex(), reader);
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setNCharacterStream(vitessPreparedIndices.getParameterIndex(), value);
        }
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setClob(vitessPreparedIndices.getParameterIndex(), reader);
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setBlob(vitessPreparedIndices.getParameterIndex(), inputStream);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            VitessPreparedIndices vitessPreparedIndices = clientPreparedQueryBindingsIndexes.get(parameterIndex - 1);
            clientPreparedQueryBindingsList.get(vitessPreparedIndices.getSqlIndex()).setNClob(vitessPreparedIndices.getParameterIndex(), reader);
        }
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        LOGGER.debug("prepared statement execute: " + sql);

        cleanResultSets();

        List<String> sqls = split(sql);

        ResultSet returnResultSet;
        synchronized (checkClosed().getConnectionMutex()) {
            try (IContext ctx = this.queryTimeout == 0 ? VtContext.withCancel(this.context) : VtContext.withDeadline(this.context, this.queryTimeout, TimeUnit.SECONDS)) {
                if (sqls.size() > 1) {
                    returnResultSet = executeMultiQueryInternal(ctx, sqls, new ArrayList<>(), this.retrieveGeneratedKeys);
                    return returnResultSet;
                }
                if (streamResults()) {
                    returnResultSet = executeStreamQueryInternal(ctx, sql, null, this.retrieveGeneratedKeys);
                } else {
                    returnResultSet = executeQueryInternal(ctx, sql, null, this.retrieveGeneratedKeys);
                }
                return returnResultSet;
            }
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        LOGGER.debug("prepared statement execute: " + sql);

        cleanResultSets();

        List<String> sqls = split(sql);

        int rc;
        synchronized (checkClosed().getConnectionMutex()) {
            try (IContext ctx = this.queryTimeout == 0 ? VtContext.withCancel(this.context) : VtContext.withDeadline(this.context, this.queryTimeout, TimeUnit.SECONDS)) {
                if (sqls.size() > 1) {
                    rc = executeMultiQueryUpdateInternal(ctx, sqls, new ArrayList<>(), true, this.retrieveGeneratedKeys);
                    return rc;
                }
                rc = executeUpdateInternal(ctx, sql, null, true, this.retrieveGeneratedKeys);
            }
        }

        return rc;
    }

    @Override
    public void close() throws SQLException {
        VitessConnection locallyScopedConn = this.connection;
        if (locallyScopedConn == null || isClosed) {
            return;
        }

        this.context.close();
        clientPreparedQueryBindingsList.clear();
        clientPreparedQueryBindingsIndexes.clear();
        sqls.clear();
        clearBatchInternal();
        cleanResultSets();
        cleanOpenedResultSets();
        locallyScopedConn.unregisterStatement(this);
        this.connection = null;
        this.isClosed = true;
    }

    @Override
    public boolean execute(String inputSql) throws SQLException {
        LOGGER.debug("prepared statement execute: " + inputSql);

        cleanResultSets();

        List<String> sqls = split(inputSql);

        synchronized (checkClosed().getConnectionMutex()) {
            try (IContext ctx = this.queryTimeout == 0 ? VtContext.withCancel(this.context) : VtContext.withDeadline(this.context, this.queryTimeout, TimeUnit.SECONDS)) {
                if (sqls.size() > 1) {
                    executeMultiQueryInternal(ctx, sqls, new ArrayList<>(), false);
                    return getExecuteInternalResult();
                }
                executeInternal(ctx, inputSql, null, this.retrieveGeneratedKeys);
                return getExecuteInternalResult();
            }
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {
        cleanResultSets();

        synchronized (checkClosed().getConnectionMutex()) {
            try (IContext ctx = this.queryTimeout == 0 ? VtContext.withCancel(this.context) : VtContext.withDeadline(this.context, this.queryTimeout, TimeUnit.SECONDS)) {
                return executeBatchInternal(ctx, this.retrieveGeneratedKeys);
            }
        }
    }

    @AllArgsConstructor
    @Data
    public static class VitessPreparedIndices {
        int sqlIndex;

        int parameterIndex;
    }
}
