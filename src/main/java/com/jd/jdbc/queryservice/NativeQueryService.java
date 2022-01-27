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

package com.jd.jdbc.queryservice;

import com.jd.jdbc.context.IContext;
import com.jd.jdbc.context.VtContextConstant;
import com.jd.jdbc.exception.SQLExceptionTranslator;
import com.jd.jdbc.monitor.QueryServiceCollector;
import com.jd.jdbc.pool.ExecuteResult;
import com.jd.jdbc.pool.InnerConnection;
import com.jd.jdbc.pool.StatefulConnection;
import com.jd.jdbc.pool.StatefulConnectionPool;
import com.jd.jdbc.queryservice.util.VtResultSetUtils;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqltypes.BatchVtResultSet;
import com.jd.jdbc.sqltypes.BeginBatchVtResultSet;
import com.jd.jdbc.sqltypes.BeginVtResultSet;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtType;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.BoundQuery;
import com.jd.jdbc.topo.topoproto.TopoProto;
import com.jd.jdbc.util.SchemaUtil;
import com.jd.jdbc.vitess.mysql.VitessPropertyKey;
import io.prometheus.client.Histogram;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Native mysql protocol QueryService
 **/
public class NativeQueryService implements IQueryService {
    public static final String TRANSACTION_RESERVE_ERROR = "transactionID and reserveID must match if both are non-zero";

    public static final String STREAM_TRANSACTION_ERROR = "transactionID in streamExecute should be zero";

    public static final String BATCH_TRANSACTION_ERROR = "transactionID in batchExecute should be zero";

    private static final Log logger = LogFactory.getLog(NativeQueryService.class);

    private static final String BEGIN = "begin;";

    private static final Histogram HISTOGRAM = QueryServiceCollector.getStatementTypeHistogram();

    private final StatefulConnectionPool statefulConnectionPool;

    private Topodata.Tablet tablet;

    private Histogram.Timer histogramTimer = null;

    public NativeQueryService(final Topodata.Tablet tablet, final String user, final String password, final Properties dsProperties, final Properties properties) {
        this.tablet = tablet;
        this.statefulConnectionPool = StatefulConnectionPool.getStatefulConnectionPool(tablet, user, password, dsProperties, properties);
    }

    @Override
    public void setTablet(Topodata.Tablet tablet) {
        this.tablet = tablet;
    }

    //currently not in used
    @Override
    public Query.BeginResponse begin(IContext context, Query.Target target, Query.ExecuteOptions options) {
        return null;
    }

    @Override
    public Query.CommitResponse commit(final IContext context, final Query.Target target, final Long transactionId) throws SQLException {
        this.startSummary();
        try (StatefulConnection conn = statefulConnectionPool.getAndLock(transactionId, "commit")) {
            conn.commit();
            return Query.CommitResponse.newBuilder().build();
        } catch (SQLException e) {
            this.errorCount();
            throw SQLExceptionTranslator.translate(getReason(e), e);
        } finally {
            this.endSummary();
        }
    }

    @Override
    public Query.RollbackResponse rollback(final IContext context, final Query.Target target, final Long transactionId) throws SQLException {
        this.startSummary();
        try (StatefulConnection conn = statefulConnectionPool.getAndLock(transactionId, "rollback")) {
            conn.rollback();
            return Query.RollbackResponse.newBuilder().build();
        } catch (SQLException e) {
            this.errorCount();
            throw SQLExceptionTranslator.translate(getReason(e), e);
        } finally {
            this.endSummary();
        }
    }

    @Override
    public VtResultSet execute(final IContext context, final Query.Target target, final String sql, final Map<String, BindVariable> bindVariables, final Long transactionId,
                               final Long reservedId, final Query.ExecuteOptions options)
        throws SQLException {
        if (transactionId != 0 && reservedId != 0 && transactionId.compareTo(reservedId) != 0) {
            SQLException exception = new SQLException(TRANSACTION_RESERVE_ERROR);
            context.cancel(TRANSACTION_RESERVE_ERROR);
            throw exception;
        }
        if (context.isDone()) {
            throw new SQLException(VtContextConstant.EXECUTION_CANCELLED + context.error());
        }

        if (transactionId == 0) {
            this.startSummary();
            try (InnerConnection connection = statefulConnectionPool.getNoStatefulConn()) {
                ExecuteResult res = connection.execute(sql);
                return toVtResultSet(res.getQueryFlag(), res.getStatement());
            } catch (SQLException e) {
                this.errorCount();
                context.cancel(e.getMessage());
                throw SQLExceptionTranslator.translate(getReason(e), e);
            } finally {
                this.endSummary();
            }
        } else {
            this.startSummary();
            StatefulConnection conn = null;
            try {
                conn = statefulConnectionPool.getAndLock(transactionId, "for query");
                ExecuteResult res = conn.execute(sql);
                return toVtResultSet(res.getQueryFlag(), res.getStatement());
            } catch (SQLException e) {
                this.errorCount();
                context.cancel(e.getMessage());
                throw SQLExceptionTranslator.translate(getReason(e), e);
            } finally {
                if (conn != null) {
                    conn.unlock(false);
                }
                this.endSummary();
            }
        }
    }

    @Override
    public StreamIterator streamExecute(final IContext context, final Query.Target target, final String sql, final Map<String, BindVariable> bindVariables, final Long transactionId,
                                        final Query.ExecuteOptions options)
        throws SQLException {
        if (transactionId != 0) {
            SQLException exception = new SQLException(STREAM_TRANSACTION_ERROR);
            context.cancel(STREAM_TRANSACTION_ERROR);
            throw exception;
        }
        if (context.isDone()) {
            throw new SQLException(VtContextConstant.STREAM_EXECUTION_CANCELLED + context.error());
        }

        InnerConnection connection = statefulConnectionPool.getNoStatefulConn();
        ResultSet resultSet = null;
        try {
            resultSet = connection.streamExecute(sql);
        } catch (SQLException e) {
            connection.close();
            throw SQLExceptionTranslator.translate(getReason(e), e);
        }
        return new StreamIterator(connection, resultSet);
    }

    /**
     * Currently always called with transactionID = 0
     *
     * @param context
     * @param target
     * @param queries
     * @param asTransaction
     * @param transactionId
     * @param options
     * @return
     * @throws SQLException
     */
    @Override
    public BatchVtResultSet executeBatch(final IContext context, final Query.Target target, final List<BoundQuery> queries, final Boolean asTransaction, final Long transactionId,
                                         final Query.ExecuteOptions options)
        throws SQLException {
        if (transactionId != 0 && asTransaction) {
            SQLException exception = new SQLException(BATCH_TRANSACTION_ERROR);
            context.cancel(BATCH_TRANSACTION_ERROR);
            throw exception;
        }
        if (context.isDone()) {
            throw new SQLException(VtContextConstant.EXECUTION_CANCELLED, context.error());
        }

        if (transactionId == 0) {
            this.startSummary();
            try (InnerConnection connection = statefulConnectionPool.getNoStatefulConn()) {
                List<VtResultSet> allVtResultSets = new ArrayList<>();
                List<String> sqlList = getMultiSql(context, queries);

                for (String sql : sqlList) {
                    ExecuteResult result = connection.execute(sql);
                    List<VtResultSet> vtResultSets = toVtResultSets(result.getQueryFlag(), result.getStatement());
                    allVtResultSets.addAll(vtResultSets);
                }

                return new BatchVtResultSet(allVtResultSets);
            } catch (SQLException e) {
                this.errorCount();
                context.cancel(e.getMessage());
                throw SQLExceptionTranslator.translate(getReason(e), e);
            } finally {
                this.endSummary();
            }
        } else {
            this.startSummary();
            StatefulConnection conn = null;
            try {
                conn = statefulConnectionPool.getAndLock(transactionId, "tx executeBatch");
                List<VtResultSet> allVtResultSets = new ArrayList<>();
                List<String> sqlList = getMultiSql(context, queries);
                for (String sql : sqlList) {
                    ExecuteResult result = conn.execute(sql);
                    List<VtResultSet> vtResultSets = toVtResultSets(result.getQueryFlag(), result.getStatement());
                    allVtResultSets.addAll(vtResultSets);
                }

                return new BatchVtResultSet(allVtResultSets);
            } catch (SQLException e) {
                this.errorCount();
                context.cancel(e.getMessage());
                throw SQLExceptionTranslator.translate(getReason(e), e);
            } finally {
                if (conn != null) {
                    conn.unlock(false);
                }
                this.endSummary();
            }
        }
    }

    /**
     * Combo methods, they also return the transactionID from the
     * Begin part. If err != nil, the transactionID may still be
     * non-zero, and needs to be propagated back (like for a DB
     * Integrity Error)
     *
     * @param target
     * @param queries
     * @param asTransaction
     * @param options
     * @return
     */
    @Override
    public BeginBatchVtResultSet beginExecuteBatch(final IContext ctx, final Query.Target target, final List<BoundQuery> queries, final Boolean asTransaction, final Query.ExecuteOptions options)
        throws SQLException {
        if (ctx.isDone()) {
            throw new SQLException(VtContextConstant.BEGIN_EXECUTION_CANCELLED + ctx.error());
        }
        this.startSummary();
        StatefulConnection conn = null;
        try {
            conn = statefulConnectionPool.newConn(options.getWorkloadValue() != Query.ExecuteOptions.Workload.DBA_VALUE);
            List<VtResultSet> allVtResultSets = new ArrayList<>();
            List<String> sqlList = getMultiSql(ctx, queries);
            conn.setAutoCommitFalse();
            ExecuteResult result = conn.execute(BEGIN + sqlList.get(0));
            boolean isQuery = result.getStatement().getMoreResults();
            List<VtResultSet> vtResultSets = toVtResultSets(isQuery, result.getStatement());
            allVtResultSets.addAll(vtResultSets);

            for (int i = 1; i < sqlList.size(); i++) {
                ExecuteResult curResult = conn.execute(sqlList.get(i));
                isQuery = curResult.getQueryFlag();
                List<VtResultSet> curVtResultSets = toVtResultSets(isQuery, curResult.getStatement());
                allVtResultSets.addAll(curVtResultSets);
            }
            return new BeginBatchVtResultSet(this.tablet.getAlias(), conn.getConnID(), allVtResultSets);

        } catch (SQLException e) {
            this.errorCount();
            rollbackAndRelease(conn);
            ctx.cancel(e.getMessage());
            throw SQLExceptionTranslator.translate(getReason(e), e);
        } finally {
            if (conn != null) {
                conn.unlock(false);
            }
            this.endSummary();
        }
    }

    @Override
    public Query.ReserveBeginExecuteResponse reserveBeginExecute(IContext context, Query.Target target, List<String> preQuries, String sql, Map<String, BindVariable> bindVariables,
                                                                 Query.ExecuteOptions options) throws Exception {
        return null;
    }

    @Override
    public Query.ReserveExecuteResponse reserveExecute(IContext context, Query.Target target, List<String> preQueries, String sql, Map<String, BindVariable> bindVariables, Long transactionID,
                                                       Query.ExecuteOptions options) throws Exception {
        return null;
    }

    @Override
    public BeginVtResultSet beginExecute(final IContext context, final Query.Target target, final List<String> preQueries, final String sql, final Map<String, BindVariable> bindVariables,
                                         final Long reservedId,
                                         final Query.ExecuteOptions options) throws SQLException {
        if (context.isDone()) {
            throw new SQLException(VtContextConstant.BEGIN_EXECUTION_CANCELLED + context.error());
        }
        this.startSummary();

        StatefulConnection conn = null;
        try {
            if (reservedId != 0) {
                conn = statefulConnectionPool.getAndLock(reservedId, "start transaction on reserve conn");
            } else {
                conn = statefulConnectionPool.newConn(options.getWorkloadValue() != Query.ExecuteOptions.Workload.DBA_VALUE);
            }
            conn.setAutoCommitFalse();
            ExecuteResult res = conn.execute(BEGIN + sql);
            boolean isQuery = res.getStatement().getMoreResults();
            VtResultSet vtResultSet = toVtResultSet(isQuery, res.getStatement());
            return new BeginVtResultSet(this.tablet.getAlias(), conn.getConnID(), vtResultSet);
        } catch (SQLException e) {
            context.cancel(e.getMessage());
            rollbackAndRelease(conn);
            throw SQLExceptionTranslator.translate(getReason(e), e);
        } finally {
            if (conn != null) {
                conn.unlock(false);
            }
            this.endSummary();
        }
    }

    @Override
    public Query.ReleaseResponse release(final IContext context, final Query.Target target, final Long transactionID, final Long reservedID) {
        try {
            StatefulConnection conn = statefulConnectionPool.getAndLock(transactionID, "release");
            conn.release();
        } catch (SQLException e) {
            logger.warn(e.getMessage(), e);
        }

        return Query.ReleaseResponse.newBuilder().build();
    }

    @Override
    public void close() {
        StatefulConnectionPool.shutdown(tablet);
    }

    private List<String> getMultiSql(final IContext context, final List<BoundQuery> queries) {
        // sql1,sql2,sql3 -----> sql1;sql2;sql3
        List<String> queryList = new ArrayList<>();
        StringBuilder queryBuf = new StringBuilder();
        Integer maxAllowedPacket = (Integer) context.getContextValue(VitessPropertyKey.MAX_ALLOWED_PACKET);

        for (BoundQuery query : queries) {
            String nextSql = query.getSql();
            if (queryBuf.length() > 0 && (((queryBuf.length() + nextSql.length()) * 3)) * 2 > maxAllowedPacket) {
                queryList.add(queryBuf.toString());
                queryBuf = new StringBuilder();
            }
            queryBuf.append(nextSql);
            if (';' != (nextSql.charAt(nextSql.length() - 1))) {
                queryBuf.append(";");
            }
        }
        if (queryBuf.length() > 0) {
            queryList.add(queryBuf.toString());
        }
        return queryList;
    }

    private void rollbackAndRelease(final StatefulConnection conn) throws SQLException {
        if (conn == null) {
            return;
        }
        try {
            conn.rollback();
        } catch (SQLException e) {
            String errorMsg = e.getCause() == null ? e.getMessage() : e.getCause().getMessage();
            throw SQLExceptionTranslator.translate(errorMsg + ";  tablet :" + TopoProto.tabletToHumanString(tablet), e);
        } finally {
            conn.release();
        }
    }

    private String getReason(final Exception e) {
        return e.getMessage() + ";  tablet :" + TopoProto.tabletToHumanString(tablet);
    }

    private void startSummary() {
        histogramTimer = HISTOGRAM.labels(TopoProto.getPoolName(tablet), tablet.getKeyspace()).startTimer();
    }

    private void endSummary() {
        if (histogramTimer != null) {
            histogramTimer.observeDuration();
        }
        QueryServiceCollector.getStatementCounter().inc();
    }

    private void errorCount() {
        QueryServiceCollector.getStatementErrorCounter().labels(tablet.getKeyspace()).inc();
    }

    private List<VtResultSet> toVtResultSets(boolean isQuery, final Statement statement) throws SQLException {
        List<VtResultSet> vtResultSets = new ArrayList<>();
        while (isQuery || statement.getUpdateCount() != -1) {
            VtResultSet vtResultSet = toVtResultSet(isQuery, statement);
            vtResultSets.add(vtResultSet);
            isQuery = statement.getMoreResults();
        }
        return vtResultSets;
    }

    private VtResultSet toVtResultSet(final boolean isQuery, final Statement statement) throws SQLException {
        VtResultSet ret = new VtResultSet();
        if (isQuery) {
            ResultSet resultSet = statement.getResultSet();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int cols = metaData.getColumnCount();
            Query.Field[] fields = new Query.Field[cols];
            List<String> columnClassNames = new ArrayList<>(cols);
            for (int idx = 0, col = 1; idx < cols; idx++, col++) {
                Query.Field.Builder fieldBuilder = Query.Field.newBuilder();
                Query.Type queryType = VtType.getQueryType(metaData.getColumnTypeName(col));
                columnClassNames.add(metaData.getColumnClassName(col));
                fieldBuilder.setDatabase(SchemaUtil.getLogicSchema(metaData.getCatalogName(col)))
                    .setTable(metaData.getTableName(col))
                    .setName(metaData.getColumnLabel(col))
                    .setOrgName(metaData.getColumnName(col))
                    .setPrecision(metaData.getPrecision(col))
                    .setJdbcClassName(metaData.getColumnClassName(col))
                    .setColumnLength(metaData.getColumnDisplaySize(col))
                    .setDecimals(metaData.getScale(col))
                    .setIsSigned(metaData.isSigned(col))
                    .setType(queryType);
                fields[idx] = fieldBuilder.build();
            }
            ret.setFields(fields);
            List<List<VtResultValue>> rows = new ArrayList<>();
            while (resultSet.next()) {
                List<VtResultValue> vtValueList = new ArrayList<>(cols);
                for (int col = 1; col <= cols; col++) {
                    VtResultValue vtResultValue = VtResultSetUtils.getValue(resultSet, col, columnClassNames.get(col - 1), (int) fields[col - 1].getPrecision(), fields[col - 1].getType());
                    vtValueList.add(vtResultValue);
                }
                rows.add(vtValueList);
            }
            ret.setRows(rows);
            ret.setRowsAffected(rows.size());
            ret.setInsertID(-1);
        } else {
            ret.setRowsAffected(statement.getUpdateCount());
            ResultSet resultSet = statement.getGeneratedKeys();
            if (resultSet.next()) {
                ret.setInsertID(resultSet.getLong(1));
            }
        }
        return ret;
    }
}
