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

package com.jd.jdbc;

import com.jd.jdbc.common.Constant;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.context.VtContextConstant;
import com.jd.jdbc.engine.Plan;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.key.DestinationShard;
import com.jd.jdbc.monitor.PlanCollector;
import com.jd.jdbc.planbuilder.MultiQueryPlan;
import com.jd.jdbc.planbuilder.PlanBuilder;
import com.jd.jdbc.queryservice.ShardQueryService;
import com.jd.jdbc.queryservice.StreamIterator;
import com.jd.jdbc.queryservice.util.RoleUtils;
import com.jd.jdbc.session.SafeSession;
import com.jd.jdbc.sqlparser.Comment;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.SqlParser;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLDeleteStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLUpdateStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.BindVarNeeds;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlInsertReplaceStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtPutBindVarsVisitor;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRestoreVisitor;
import com.jd.jdbc.sqlparser.parser.SQLParserUtils;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqlparser.utils.Utils;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtRowList;
import com.jd.jdbc.sqltypes.VtSqlStatementType;
import com.jd.jdbc.sqltypes.VtStreamResultSet;
import com.jd.jdbc.srvtopo.ResolvedShard;
import com.jd.jdbc.srvtopo.ScatterConn;
import com.jd.jdbc.srvtopo.TxConn;
import com.jd.jdbc.util.cache.lrucache.LRUCache;
import com.jd.jdbc.util.consolidator.Consolidator;
import com.jd.jdbc.util.consolidator.ConsolidatorResult;
import com.jd.jdbc.util.consolidator.Result;
import com.jd.jdbc.vitess.VitessConnection;
import com.jd.jdbc.vitess.mysql.VitessPropertyKey;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import io.vitess.proto.Vtgate;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Executor implements IExecute {
    private static final Log LOGGER = LogFactory.getLog(Executor.class);

    private static final Integer LRU_CACHE_MAX_CAPACITY = 10240;

    private static final Integer MAX_PLAN_KEY_SIZE = 10240;

    private static final Integer LRU_CACHE_DEFAULT_CAPACITY = 300;

    private static Consolidator consolidator = Consolidator.getInstance();

    private static volatile Executor singletonInstance;

    private LRUCache<Plan> plans;

    private Executor(Integer planCacheCapacity) {
        this.plans = new LRUCache<>(planCacheCapacity == null ? LRU_CACHE_DEFAULT_CAPACITY : planCacheCapacity > LRU_CACHE_MAX_CAPACITY ? LRU_CACHE_MAX_CAPACITY : planCacheCapacity);
    }

    /**
     * @return
     */
    public static Executor getInstance(Integer planCacheCapacity) {
        if (singletonInstance == null) {
            synchronized (Executor.class) {
                if (singletonInstance == null) {
                    singletonInstance = new Executor(planCacheCapacity);
                }
            }
        }
        return singletonInstance;
    }

    /**
     * @param ctx
     * @param method
     * @param safeSession
     * @param stmt
     * @param bindVariableMap
     * @return
     * @throws Exception
     */
    @Override
    public VtRowList execute(IContext ctx, String method, SafeSession safeSession, String keyspace, SQLStatement stmt, Map<String, Query.BindVariable> bindVariableMap) throws SQLException {
        if (ctx.isDone()) {
            throw new SQLException(VtContextConstant.CONTEXT_CANCELLED + ctx.error());
        }
        ExecuteResponse executeResponse = this.newExecute(ctx, safeSession, keyspace, stmt, bindVariableMap);
        VtSqlStatementType stmtType = executeResponse.getSqlStatementType();
        VtRowList resultSet = executeResponse.getResultSet();
        this.saveSessionStats(safeSession, stmtType, resultSet, null);
        return resultSet;
    }

    /**
     * @param ctx
     * @param method
     * @param safeSession
     * @param keysapce
     * @param sql
     * @param bindVariableMap
     * @return
     * @throws SQLFeatureNotSupportedException
     */
    public VtRowList otherExecute(IContext ctx, String method, SafeSession safeSession, String keysapce, String sql, Map<String, Query.BindVariable> bindVariableMap)
        throws SQLException {
        VtSqlStatementType stmtType = SQLParserUtils.preview(sql);

        switch (stmtType) {
            case StmtShow:
                if (SQLParserUtils.getShowType((sql)).equalsIgnoreCase("vitess_shards")) {
                    return ShardQueryService.handleShow(safeSession, ctx, keysapce, Topodata.TabletType.MASTER);
                }
        }
        throw new SQLFeatureNotSupportedException("unrecognized statement: %s" + sql);
    }

    /**
     * @param ctx
     * @param method
     * @param safeSession
     * @param stmt
     * @param bindVariableMap
     * @return
     * @throws Exception
     */
    @Override
    public VtRowList streamExecute(IContext ctx, String method, SafeSession safeSession, String keyspace, SQLStatement stmt, Map<String, Query.BindVariable> bindVariableMap) throws SQLException {
        if (ctx.isDone()) {
            throw new SQLException(VtContextConstant.CONTEXT_CANCELLED + ctx.error());
        }
        return this.newStreamExecute(ctx, safeSession, keyspace, stmt, bindVariableMap).getResultSet();
    }

    @Override
    public List<VtRowList> batchExecute(IContext ctx, String method, SafeSession safeSession, String keyspace,
                                        List<SQLStatement> batchStmts,
                                        List<Map<String, Query.BindVariable>> bindVariableMapList) throws SQLException {
        if (ctx.isDone()) {
            throw new SQLException(VtContextConstant.CONTEXT_CANCELLED + ctx.error());
        }
        return this.newBatchExecute(ctx, safeSession, keyspace, batchStmts, bindVariableMapList).getResultSets();
    }

    /**
     * @param ctx
     * @param rss
     * @param queries
     * @param safeSession
     * @param autocommit
     * @param ignoreMaxMemoryRows
     * @return
     * @throws Exception
     */
    @Override
    public ExecuteMultiShardResponse executeMultiShard(IContext ctx, List<ResolvedShard> rss, List<Query.BoundQuery> queries, SafeSession safeSession, Boolean autocommit, Boolean ignoreMaxMemoryRows)
        throws SQLException {
        return ((ScatterConn) ctx.getContextValue(VitessConnection.ContextKey.CTX_SCATTER_CONN)).executeMultiShard(ctx, rss, queries, safeSession, autocommit, ignoreMaxMemoryRows);
    }

    @Override
    public ExecuteBatchMultiShardResponse executeBatchMultiShard(IContext ctx, List<ResolvedShard> rss, List<List<Query.BoundQuery>> queries, SafeSession safeSession, Boolean autocommit,
                                                                 Boolean ignoreMaxMemoryRows, Boolean asTransaction) throws SQLException {
        return ((ScatterConn) ctx.getContextValue(VitessConnection.ContextKey.CTX_SCATTER_CONN)).executeBatchMultiShard(ctx, rss, queries, safeSession, autocommit, ignoreMaxMemoryRows, asTransaction);
    }

    /**
     * @param ctx
     * @param rss
     * @param queries
     * @param safeSession
     * @return
     * @throws Exception
     */
    @Override
    public List<StreamIterator> streamExecuteMultiShard(IContext ctx, List<ResolvedShard> rss, List<Query.BoundQuery> queries, SafeSession safeSession) throws SQLException {
        return ((ScatterConn) ctx.getContextValue(VitessConnection.ContextKey.CTX_SCATTER_CONN)).streamExecuteMultiShard(ctx, rss, queries, safeSession);
    }

    private String extractShardPropFromStmt(final SQLStatement stmt) {
        Map<String, Object> attributes = stmt.getAttributes();
        if (!attributes.containsKey("shard")) {
            return null;
        }
        Object shardObject = attributes.get("shard");
        if (!(shardObject instanceof String)) {
            return null;
        }
        String shard = (String) shardObject;
        return shard;
    }

    private void checkNullVariable(final Map<String, Query.BindVariable> bindVariableMap) throws SQLException {
        if (null == bindVariableMap) {
            return;
        }

        for (Map.Entry<String, Query.BindVariable> e : bindVariableMap.entrySet()) {
            if (null == e.getValue()) {
                throw new SQLException("No value specified for parameter " + (Integer.parseInt(e.getKey()) + 1));
            }
        }
    }

    /**
     * getPlan computes the plan for the given query. If one is in the cache, it reuses it.
     *
     * @param ctx
     * @param stmt
     * @param bindVariableMap
     * @return
     * @throws SQLException
     */
    public PlanResult getPlan(IContext ctx, String keyspace, SQLStatement stmt, Map<String, Query.BindVariable> bindVariableMap, Boolean skipQueryPlanCache, String charEncoding) throws SQLException {
        checkNullVariable(bindVariableMap);
        totalCounterInc(stmt);
        VSchemaManager vm = (VSchemaManager) ctx.getContextValue(VitessConnection.ContextKey.CTX_VSCHEMA_MANAGER);
        if (vm == null) {
            throw new SQLException("vschema not initialized");
        }

        String sql = SQLUtils.toMySqlString(stmt, SQLUtils.NOT_FORMAT_OPTION);
        String planKey = keyspace + ":" + sql;
        BindVarNeeds bindVarNeeds = new BindVarNeeds();

        Destination destination = null;
        String dest = this.extractShardPropFromStmt(stmt);
        if (dest != null) {
            destination = new DestinationShard(dest);
            planKey = planKey + " " + dest;
        }

        if (!skipQueryPlanCache) {
            Plan cached = plans.get(planKey);
            if (cached != null) {
                cacheCounterInc(cached.getStatementType());
                return new PlanResult(cached, bindVariableMap);
            }
        }

        if (SqlParser.canNormalize(stmt)) {
            SqlParser.PrepareAstResult prepareAstResult = SqlParser.prepareAst(stmt, bindVariableMap, charEncoding);
            stmt = prepareAstResult.getAst();
            bindVarNeeds = prepareAstResult.getBindVarNeeds();
            bindVariableMap = prepareAstResult.getBindVariableMap();
            sql = SQLUtils.toMySqlString(stmt, SQLUtils.NOT_FORMAT_OPTION);
        } else {
            stmt = toFullStatement(stmt, bindVariableMap);
        }

        planKey = keyspace + ":" + sql;
        if (dest != null) {
            planKey = planKey + " " + dest;
        }
        if (!skipQueryPlanCache) {
            Plan cached = plans.get(planKey);
            if (cached != null) {
                cacheCounterInc(cached.getStatementType());
                return new PlanResult(cached, bindVariableMap);
            }
        }

        Plan plan = PlanBuilder.buildFromStmt(stmt, vm, keyspace, bindVarNeeds, destination);

        // plan cache is only for select or insert/replace statement
        if (!skipQueryPlanCache && plan.getPrimitive() != null && SqlParser.canNormalize(stmt)
            && planKey.length() < MAX_PLAN_KEY_SIZE) {
            plans.set(planKey, plan);
        }
        return new PlanResult(plan, bindVariableMap);
    }

    private SQLStatement toFullStatement(SQLStatement stmt, Map<String, Query.BindVariable> bindVariableMap) {
        if (bindVariableMap != null && bindVariableMap.isEmpty()) {
            return stmt;
        }
        VtPutBindVarsVisitor vtPutBindVarsVisitor = new VtPutBindVarsVisitor(bindVariableMap);
        stmt.accept(vtPutBindVarsVisitor);
        return stmt;
    }

    /**
     * @param ctx
     * @param plan
     * @param vCursor
     * @param bindVariableMap
     * @return
     */
    public CurrFunc executePlan(IContext ctx, Plan plan, Vcursor vCursor, Map<String, Query.BindVariable> bindVariableMap) {
        Integer maxRows = (Integer) ctx.getContextValue(VitessPropertyKey.MAX_ROWS.getKeyName());
        return safeSession -> {
            // 4: Execute!
            ExecuteMultiShardResponse executeMultiShardResponse;
            VtRowList vtRowList = null;
            try {
                executeMultiShardResponse = plan.getPrimitive().execute(ctx, vCursor, bindVariableMap, true);
                vtRowList = executeMultiShardResponse.getVtRowList().reserve(maxRows);
            } catch (SQLException e) {
                // Check if there was partial DML execution. If so, rollback the transaction.
                if (safeSession.inTransaction() && vCursor.getRollbackOnPartialExec()) {
                    try {
                        ((TxConn) ctx.getContextValue(VitessConnection.ContextKey.CTX_TX_CONN)).rollback(ctx, safeSession);
                    } catch (SQLException ex) {
                        LOGGER.error(e.getMessage(), e);
                    }
                }
                throw e;
            }
            return new ExecuteResponse(plan.getStatementType(), vtRowList);
        };
    }

    public CurrFunc streamExecutePlan(IContext ctx, Plan plan, Vcursor vCursor, Map<String, Query.BindVariable> bindVariableMap) {
        Integer maxRows = (Integer) ctx.getContextValue(VitessPropertyKey.MAX_ROWS.getKeyName());
        return safeSession -> {
            // 4: Execute!
            VtStream vtStream = null;
            try {
                vtStream = plan.getPrimitive().streamExecute(ctx, vCursor, bindVariableMap, true);
            } catch (SQLException e) {
                // Check if there was partial DML execution. If so, rollback the transaction.
                if (safeSession.inTransaction() /*&& vCursor.rollbackOnPartialExec*/) {
                    try {
                        ((TxConn) ctx.getContextValue(VitessConnection.ContextKey.CTX_TX_CONN)).rollback(ctx, safeSession);
                    } catch (SQLException ex) {
                        LOGGER.error(e.getMessage(), e);
                    }
                }
                throw e;
            }
            return new ExecuteResponse(plan.getStatementType(), new VtStreamResultSet(vtStream).reserve(maxRows));
        };
    }

    public BatchCurrFunc batchExecutePlan(IContext ctx, PrimitiveEngine primitive, Vcursor vCursor) {
        Integer maxRows = (Integer) ctx.getContextValue(VitessPropertyKey.MAX_ROWS.getKeyName());
        return safeSession -> {
            // 4: Execute!
            List<ExecuteMultiShardResponse> executeMultiShardResponses;
            List<VtRowList> vtRowLists = new ArrayList<>();
            try {
                executeMultiShardResponses = primitive.batchExecute(ctx, vCursor, false);
                for (ExecuteMultiShardResponse executeResponse : executeMultiShardResponses) {
                    VtRowList resultSet = executeResponse.getVtRowList().reserve(maxRows);
                    vtRowLists.add(resultSet);
                }
            } catch (SQLException e) {
                // Check if there was partial DML execution. If so, rollback the transaction.
                if (safeSession.inTransaction() /*&& vCursor.rollbackOnPartialExec*/) {
                    try {
                        ((TxConn) ctx.getContextValue(VitessConnection.ContextKey.CTX_TX_CONN)).rollback(ctx, safeSession);
                    } catch (SQLException ex) {
                        LOGGER.error(e.getMessage(), e);
                    }
                }
                throw e;
            }
            return new BatchExecuteResponse(null, vtRowLists);
        };
    }

    /**
     * @param safeSession
     * @param stmtType
     * @param resultSet
     * @param e
     */
    private void saveSessionStats(SafeSession safeSession, VtSqlStatementType stmtType, VtRowList resultSet, Exception e) {
        safeSession.getVitessConnection().setSession(safeSession.getVitessConnection().getSession().toBuilder().setRowCount(-1).build());
        if (e != null) {
            return;
        }
        Vtgate.Session.Builder sessionBuilder = safeSession.getVitessConnection().getSession().toBuilder().setFoundRows(resultSet.getRowsAffected());
        if (resultSet.getInsertID() > 0) {
            sessionBuilder.setLastInsertId(resultSet.getInsertID());
        }
        switch (stmtType) {
            case StmtInsert:
            case StmtReplace:
            case StmtUpdate:
            case StmtDelete:
                sessionBuilder.setRowCount(resultSet.getRowsAffected());
                break;
            case StmtDDL:
            case StmtSet:
            case StmtBegin:
            case StmtCommit:
            case StmtRollback:
                sessionBuilder.setRowCount(0L);
                break;
            default:
                break;
        }
        safeSession.getVitessConnection().setSession(sessionBuilder.build());
    }

    /**
     * @param ctx
     * @param safeSession
     * @param stmt
     * @param bindVariableMap
     * @return
     * @throws Exception
     */
    private ExecuteResponse newExecute(IContext ctx, SafeSession safeSession, String keyspace, SQLStatement stmt, Map<String, Query.BindVariable> bindVariableMap) throws SQLException {
        // 1: Prepare before planning and execution

        // Start an implicit transaction if necessary.
        this.startTxIfNecessary(ctx, safeSession);

        StringBuffer sql = new StringBuffer();
        stmt.output(sql);
        Vcursor vCursor = new VcursorImpl(ctx, safeSession, new Comment(sql.toString()), this, (VSchemaManager) ctx.getContextValue(VitessConnection.ContextKey.CTX_VSCHEMA_MANAGER),
            safeSession.getVitessConnection().getResolver());

        // 2: Create a plan for the query
        String charEncoding = safeSession.getCharEncoding();
        PlanResult planResult = this.getPlan(ctx, keyspace, stmt, bindVariableMap, this.getSkipQueryPlanCache(safeSession), charEncoding);
        Plan plan = planResult.getPlan();
        bindVariableMap = planResult.getBindVariableMap();

        // We need to explicitly handle errors, and begin/commit/rollback, since these control transactions. Everything else
        // will fall through and be handled through planning
        VtRowList resultSet;
        switch (plan.getStatementType()) {
            case StmtBegin:
                resultSet = this.handleBegin(ctx, safeSession);
                return new ExecuteResponse(VtSqlStatementType.StmtBegin, resultSet);
            case StmtCommit:
                resultSet = this.handleCommit(ctx, safeSession);
                return new ExecuteResponse(VtSqlStatementType.StmtCommit, resultSet);
            case StmtRollback:
                resultSet = this.handleRollback(ctx, safeSession);
                return new ExecuteResponse(VtSqlStatementType.StmtRollback, resultSet);
            default:
                break;
        }

        if (plan.getPrimitive().needsTransaction()) {
            return this.insideTransaction(ctx, safeSession, this.executePlan(ctx, plan, vCursor, bindVariableMap));
        }

        if (getConsolidatorFlag(ctx, safeSession, plan)) {
            return getConsolidatorResponse(ctx, safeSession, keyspace, stmt, bindVariableMap, vCursor, plan);
        }
        // 3: Prepare for execution
        return this.executePlan(ctx, plan, vCursor, bindVariableMap).exec(safeSession);
    }

    private Boolean getConsolidatorFlag(IContext ctx, SafeSession safeSession, Plan plan) {
        Properties properties = safeSession.getVitessConnection().getProperties();
        Boolean consolidatorFlag = Utils.getBoolean(properties, Constant.DRIVER_PROPERTY_QUERY_CONSOLIDATOR);
        return consolidatorFlag != null && consolidatorFlag && plan.getStatementType() == VtSqlStatementType.StmtSelect
            && RoleUtils.notMaster(ctx);
    }

    private ExecuteResponse getConsolidatorResponse(IContext ctx, SafeSession safeSession, String keyspace, SQLStatement stmt, Map<String, Query.BindVariable> bindVariableMap, Vcursor vCursor,
                                                    Plan plan) throws SQLException {
        // bulid key
        StringBuilder querySqlBuffer = new StringBuilder();
        String charEncoding = safeSession.getCharEncoding();
        VtRestoreVisitor restoreVisitor = new VtRestoreVisitor(querySqlBuffer, bindVariableMap, charEncoding);
        stmt.accept(restoreVisitor);
        String consolidatorKey = keyspace + ":" + querySqlBuffer;

        // find consolidatorResultInMap
        ConsolidatorResult consolidatorResult = consolidator.get(consolidatorKey);
        if (consolidatorResult == null) {
            ConsolidatorResult consolidatorTmpResult = new ConsolidatorResult(new Result());
            consolidatorTmpResult.writeLockLock();
            try {
                ConsolidatorResult putIfAbsent = consolidator.putIfAbsent(consolidatorKey, consolidatorTmpResult);
                if (putIfAbsent == null) {
                    try {
                        ExecuteResponse response = this.executePlan(ctx, plan, vCursor, bindVariableMap).exec(safeSession);
                        VtRowList vtRowList = null;
                        if (response.resultSet != null) {
                            vtRowList = ((VtResultSet) response.getResultSet()).clone();
                        }
                        ExecuteResponse executeResponse = new ExecuteResponse(response.getSqlStatementType(), vtRowList);
                        consolidatorTmpResult.setExecuteResponse(executeResponse);
                        return response;
                    } catch (SQLException e) {
                        consolidatorTmpResult.setSQLException(e);
                        throw e;
                    } finally {
                        if (consolidatorTmpResult.getQueryCount() == 0) {
                            consolidator.remove(consolidatorKey);
                        }
                    }
                } else {
                    consolidatorResult = putIfAbsent;
                    consolidatorResult.incrementQueryCounter();
                }
            } finally {
                consolidatorTmpResult.writeLockUnLock();
            }
        } else {
            consolidatorResult.incrementQueryCounter();
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("consolidatorKey:" + consolidatorKey + " is wait other result");
        }
        consolidatorResult.readLockLock();
        try {
            if (consolidatorResult.getSQLException() != null) {
                throw consolidatorResult.getSQLException();
            }
            ExecuteResponse response = consolidatorResult.getExecuteResponse();
            VtRowList vtRowList = null;
            if (response.resultSet != null) {
                vtRowList = ((VtResultSet) response.getResultSet()).clone();
            }
            return new ExecuteResponse(response.getSqlStatementType(), vtRowList);
        } finally {
            consolidatorResult.decrementQueryCounter();
            if (consolidatorResult.getQueryCount() == 0) {
                consolidator.remove(consolidatorKey);
            }
            consolidatorResult.readLockUnLock();
        }
    }

    /**
     * @param ctx
     * @param safeSession
     * @param batchStmts
     * @param bindVariableMapList
     * @return
     * @throws Exception
     */
    private BatchExecuteResponse newBatchExecute(IContext ctx, SafeSession safeSession, String keyspace,
                                                 List<SQLStatement> batchStmts,
                                                 List<Map<String, Query.BindVariable>> bindVariableMapList) throws SQLException {
        // 1: Prepare before planning and execution

        // Start an implicit transaction if necessary.
        this.startTxIfNecessary(ctx, safeSession);

        List<IExecute.ResolvedShardQuery> shardQueryList = new ArrayList<>();
        List<PrimitiveEngine> primitiveEngines = new ArrayList<>();
        List<Map<String, Query.BindVariable>> batchBindVariableMap = new ArrayList<>();

        VSchemaManager vm = (VSchemaManager) ctx.getContextValue(VitessConnection.ContextKey.CTX_VSCHEMA_MANAGER);

        for (int stmtIdx = 0; stmtIdx < batchStmts.size(); stmtIdx++) {
            SQLStatement stmt = batchStmts.get(stmtIdx);
            Map<String, Query.BindVariable> bindVariableMap;
            if (bindVariableMapList == null || bindVariableMapList.isEmpty()) {
                bindVariableMap = new HashMap<>(16, 1);
            } else {
                bindVariableMap = bindVariableMapList.get(stmtIdx);
            }

            StringBuffer sql = new StringBuffer();
            stmt.output(sql);
            Vcursor vCursor = new VcursorImpl(ctx, safeSession, new Comment(sql.toString()), this, vm, safeSession.getVitessConnection().getResolver());

            // 2: Create a plan for the query
            String charEncoding = safeSession.getCharEncoding();
            PlanResult planResult = this.getPlan(ctx, keyspace, stmt, bindVariableMap, this.getSkipQueryPlanCache(safeSession), charEncoding);
            Plan plan = planResult.getPlan();
            bindVariableMap = planResult.getBindVariableMap();
            batchBindVariableMap.add(bindVariableMap);

            IExecute.ResolvedShardQuery shardQueries = plan.getPrimitive().resolveShardQuery(ctx, vCursor, bindVariableMap);
            shardQueryList.add(shardQueries);
            primitiveEngines.add(plan.getPrimitive());
        }

        Vcursor vCursor = new VcursorImpl(ctx, safeSession, null, this, vm, safeSession.getVitessConnection().getResolver());
        PrimitiveEngine primitive = MultiQueryPlan.buildMultiQueryPlan(primitiveEngines, shardQueryList, batchBindVariableMap);
        if (primitive.needsTransaction()) {
            return this.insideTransaction(ctx, safeSession, this.batchExecutePlan(ctx, primitive, vCursor));
        }

        // 3: Prepare for execution
        return this.batchExecutePlan(ctx, primitive, vCursor).exec(safeSession);
    }

    private ExecuteResponse newStreamExecute(IContext ctx, SafeSession safeSession, String keyspace, SQLStatement stmt, Map<String, Query.BindVariable> bindVariableMap) throws SQLException {
        // 1: Prepare before planning and execution

        // Start an implicit transaction if necessary.
        this.startTxIfNecessary(ctx, safeSession);

        StringBuffer sql = new StringBuffer();
        stmt.output(sql);
        VSchemaManager vm = (VSchemaManager) ctx.getContextValue(VitessConnection.ContextKey.CTX_VSCHEMA_MANAGER);
        VcursorImpl vCursor = new VcursorImpl(ctx, safeSession, new Comment(sql.toString()), this, vm, safeSession.getVitessConnection().getResolver());
        vCursor.setIgnoreMaxMemoryRows(Boolean.FALSE);

        // 2: Create a plan for the query
        String charEncoding = safeSession.getCharEncoding();
        PlanResult planResult = this.getPlan(ctx, keyspace, stmt, bindVariableMap, this.getSkipQueryPlanCache(safeSession), charEncoding);
        Plan plan = planResult.getPlan();
        bindVariableMap = planResult.getBindVariableMap();

        // 3: Prepare for execution
        return this.streamExecutePlan(ctx, plan, vCursor, bindVariableMap).exec(safeSession);
    }

    /**
     * @param ctx
     * @param safeSession
     * @return
     */
    private void startTxIfNecessary(IContext ctx, SafeSession safeSession) throws SQLException {
        if (!safeSession.getVitessConnection().getSession().getAutocommit() && !safeSession.getVitessConnection().getSession().getInTransaction()) {
            ((TxConn) ctx.getContextValue(VitessConnection.ContextKey.CTX_TX_CONN)).begin(ctx, safeSession);
        }
    }

    /**
     * @param ctx
     * @param safeSession
     * @param f
     * @return
     */
    private ExecuteResponse insideTransaction(IContext ctx, SafeSession safeSession, CurrFunc f) throws SQLException {
        TxConn txConn = (TxConn) ctx.getContextValue(VitessConnection.ContextKey.CTX_TX_CONN);
        boolean mustCommit = false;
        boolean defer = false;
        try {
            if (safeSession.getVitessConnection().getSession().getAutocommit() && !safeSession.inTransaction()) {
                mustCommit = true;
                txConn.begin(ctx, safeSession);
                // The defer acts as a failsafe. If commit was successful,
                // the rollback will be a no-op.
                defer = true;
            }

            // The SetAutocommitable flag should be same as mustCommit.
            // If we started a transaction because of autocommit, then mustCommit
            // will be true, which means that we can autocommit. If we were already
            // in a transaction, it means that the app started it, or we are being
            // called recursively. If so, we cannot autocommit because whatever we
            // do is likely not final.
            // The control flow is such that autocommitable can only be turned on
            // at the beginning, but never after.
            safeSession.setAutocommittable(mustCommit);

            // Execute!
            ExecuteResponse executeResponse = f.exec(safeSession);

            if (mustCommit) {
                txConn.commit(ctx, safeSession);
            }
            return new ExecuteResponse(executeResponse.sqlStatementType, executeResponse.resultSet);
        } finally {
            if (defer) {
                try {
                    txConn.rollback(ctx, safeSession);
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }
    }

    private BatchExecuteResponse insideTransaction(IContext ctx, SafeSession safeSession, BatchCurrFunc f) throws SQLException {
        TxConn txConn = (TxConn) ctx.getContextValue(VitessConnection.ContextKey.CTX_TX_CONN);
        boolean mustCommit = false;
        boolean defer = false;
        try {
            if (safeSession.getVitessConnection().getSession().getAutocommit() && !safeSession.inTransaction()) {
                mustCommit = true;
                txConn.begin(ctx, safeSession);
                // The defer acts as a failsafe. If commit was successful,
                // the rollback will be a no-op.
                defer = true;
            }

            // The SetAutocommitable flag should be same as mustCommit.
            // If we started a transaction because of autocommit, then mustCommit
            // will be true, which means that we can autocommit. If we were already
            // in a transaction, it means that the app started it, or we are being
            // called recursively. If so, we cannot autocommit because whatever we
            // do is likely not final.
            // The control flow is such that autocommitable can only be turned on
            // at the beginning, but never after.
            safeSession.setAutocommittable(mustCommit);

            // Execute!
            BatchExecuteResponse batchExecuteResponse = f.exec(safeSession);

            if (mustCommit) {
                txConn.commit(ctx, safeSession);
            }
            return new BatchExecuteResponse(batchExecuteResponse.sqlStatementType, batchExecuteResponse.getResultSets());
        } finally {
            if (defer) {
                try {
                    txConn.rollback(ctx, safeSession);
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * @param ctx
     * @param safeSession
     * @return
     */
    private VtRowList handleBegin(IContext ctx, SafeSession safeSession) throws SQLException {
        ((TxConn) ctx.getContextValue(VitessConnection.ContextKey.CTX_TX_CONN)).begin(ctx, safeSession);
        return new VtResultSet();
    }

    /**
     * @param ctx
     * @param safeSession
     * @return
     */
    private VtRowList handleCommit(IContext ctx, SafeSession safeSession) throws SQLException {
        ((TxConn) ctx.getContextValue(VitessConnection.ContextKey.CTX_TX_CONN)).commit(ctx, safeSession);
        return new VtResultSet();
    }

    /**
     * @param ctx
     * @param safeSession
     * @return
     */
    private VtRowList handleRollback(IContext ctx, SafeSession safeSession) throws SQLException {
        ((TxConn) ctx.getContextValue(VitessConnection.ContextKey.CTX_TX_CONN)).rollback(ctx, safeSession);
        return new VtResultSet();
    }

    /**
     * @param safeSession
     * @return
     */
    private Boolean getSkipQueryPlanCache(SafeSession safeSession) {
        if (safeSession == null || safeSession.getVitessConnection() == null
            || safeSession.getVitessConnection().getSession() == null) {
            return false;
        }
        Query.ExecuteOptions executeOptions = safeSession.getVitessConnection().getSession().getOptions();
        if (executeOptions.isInitialized()) {
            return executeOptions.getSkipQueryPlanCache();
        }
        return false;
    }

    private void totalCounterInc(SQLStatement stmt) {
        if (stmt instanceof SQLSelectStatement) {
            PlanCollector.getTotalCounter().labels("Select").inc();
        } else if (stmt instanceof MySqlInsertReplaceStatement) {
            PlanCollector.getTotalCounter().labels("Insert").inc();
        } else if (stmt instanceof SQLUpdateStatement) {
            PlanCollector.getTotalCounter().labels("Update").inc();
        } else if (stmt instanceof SQLDeleteStatement) {
            PlanCollector.getTotalCounter().labels("Delete").inc();
        } else {
            PlanCollector.getTotalCounter().labels("Other").inc();
        }
    }

    private void cacheCounterInc(VtSqlStatementType type) {
        switch (type) {
            case StmtInsert:
                PlanCollector.getCacheCounter().labels("Insert").inc();
                break;
            case StmtSelect:
                PlanCollector.getCacheCounter().labels("Select").inc();
                break;
            case StmtUpdate:
                PlanCollector.getCacheCounter().labels("Update").inc();
                break;
            case StmtDelete:
                PlanCollector.getCacheCounter().labels("Delete").inc();
                break;
            default:
                PlanCollector.getCacheCounter().labels("Other").inc();
        }
    }

    @FunctionalInterface
    interface CurrFunc {
        /**
         * @param safeSession
         * @return
         */
        ExecuteResponse exec(SafeSession safeSession) throws SQLException;
    }

    @FunctionalInterface
    interface BatchCurrFunc {
        /**
         * @param safeSession
         * @return
         */
        BatchExecuteResponse exec(SafeSession safeSession) throws SQLException;
    }

    @Getter
    @AllArgsConstructor
    public static class ExecuteResponse {
        VtSqlStatementType sqlStatementType;

        VtRowList resultSet;
    }

    @Getter
    @AllArgsConstructor
    static class BatchExecuteResponse {
        VtSqlStatementType sqlStatementType;

        List<VtRowList> resultSets;
    }

    @Getter
    @AllArgsConstructor
    public static class PlanResult {
        private final Plan plan;

        private final Map<String, Query.BindVariable> bindVariableMap;
    }
}
