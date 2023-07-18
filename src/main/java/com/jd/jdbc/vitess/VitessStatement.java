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
import com.jd.jdbc.monitor.SqlErrorCollector;
import com.jd.jdbc.monitor.StatementCollector;
import com.jd.jdbc.planbuilder.RoutePlan;
import com.jd.jdbc.session.SafeSession;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLMethodInvokeExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLDeleteStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLInsertStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLJoinTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLReplaceStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQueryBlock;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSubqueryTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQueryTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLUpdateStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlInsertReplaceStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.dialect.mysql.parser.MySqlLexer;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtChangeSchemaVisitor;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRemoveBacktickVisitor;
import com.jd.jdbc.sqlparser.parser.Lexer;
import com.jd.jdbc.sqlparser.parser.ParserException;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqlparser.utils.SplitMultiQueryUtils;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqlparser.utils.TableNameUtils;
import com.jd.jdbc.sqlparser.utils.Utils;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtRowList;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.vitess.mysql.VitessPropertyKey;
import io.netty.util.internal.StringUtil;
import io.prometheus.client.Histogram;
import io.vitess.proto.Query;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;

public class VitessStatement extends AbstractVitessStatement {

    private static final Log logger = LogFactory.getLog(VitessStatement.class);

    private static final Histogram statementTypeHistogram = StatementCollector.getStatementTypeHistogram();

    private static final String START_OF_MULTI_LINE_COMMENT = "/*";

    private static final String END_OF_MULTI_LINE_COMMENT = "*/";

    private static final String TABLE_DUAL = "dual";

    private static final String FUNCATION_LAST_INSERT_ID = "LAST_INSERT_ID";

    private static final String FUNCATION_IDENTITY = "@@IDENTITY";

    private static Query.Field[] generatedKeyField;

    @Getter
    protected final Executor executor;

    protected final int CURRENT_RESULT_INDEX = 0;

    protected final int NEXT_RESULT_INDEX = 1;

    protected List<String> batchSqls;

    protected List<Map<String, BindVariable>> bindVariableMapList;

    protected volatile VitessConnection connection;

    protected List<VtRowList> resultSets = new ArrayList<>();

    protected Set<VtRowList> openedResultSets = new HashSet<>();

    protected volatile Boolean isClosed;

    protected boolean retrieveGeneratedKeys = false;

    protected long lastInsertId = -1;

    protected List<VtResultSet> batchedGeneratedKeys = null;

    protected int resultSetType = ResultSet.TYPE_FORWARD_ONLY;

    protected int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;

    protected int fetchSize = 0;

    protected int queryTimeout = 0;

    protected IContext context;

    private Histogram.Timer statementTypeHistogramTimer = null;

    public VitessStatement(VitessConnection connection, Executor executor) {
        this.connection = connection;
        this.executor = executor;
        this.context = VtContext.withCancel(connection.getCtx());
        this.context.setContextValue(VitessPropertyKey.MAX_ROWS.getKeyName(),
            Integer.valueOf(connection.getProperties().getProperty(VitessPropertyKey.MAX_ROWS.getKeyName(), "0")));
        this.isClosed = false;
    }

    protected VitessConnection checkClosed() throws SQLException {
        VitessConnection c = this.connection;
        if (c == null) {
            throw new SQLException("connection has been closed");
        }
        return c;
    }

    public void setRetrieveGeneratedKeys(boolean flag) {
        this.retrieveGeneratedKeys = flag;
    }

    public ParseResult parseStatements(String sql) throws SQLException {
        Map<String, String> prop = parseComment(sql);
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        VtRemoveBacktickVisitor visitor = new VtRemoveBacktickVisitor();
        stmt.accept(visitor);

        if (!(stmt instanceof SQLSelectStatement)
            && !(stmt instanceof MySqlInsertReplaceStatement)
            && !(stmt instanceof SQLUpdateStatement)
            && !(stmt instanceof SQLDeleteStatement)) {
            throw new SQLException("not supported sql: " + SQLUtils.toMySqlString(stmt, SQLUtils.NOT_FORMAT_OPTION));
        }

        for (Map.Entry<String, String> entry : prop.entrySet()) {
            stmt.putAttribute(entry.getKey(), entry.getValue());
        }

        return changeSchema(stmt);
    }

    private ParseResult changeSchema(SQLStatement stmt) throws SQLException {
        String defaultKeyspace = this.connection.getDefaultKeyspace();

        if (stmt instanceof SQLSelectStatement) {
            SQLSelectQuery selectQuery = ((SQLSelectStatement) stmt).getSelect().getQuery();
            if (selectQuery instanceof MySqlSelectQueryBlock) {
                SQLTableSource tableSource = ((MySqlSelectQueryBlock) selectQuery).getFrom();
                if (tableSource != null
                    && !(tableSource instanceof SQLExprTableSource)
                    && !(tableSource instanceof SQLJoinTableSource)
                    && !(tableSource instanceof SQLSubqueryTableSource)
                    && !(tableSource instanceof SQLUnionQueryTableSource)) {
                    throw new SQLException("not supported sql: " + SQLUtils.toMySqlString(stmt, SQLUtils.NOT_FORMAT_OPTION));
                }
                if (tableSource == null) {
                    ((MySqlSelectQueryBlock) selectQuery).setFrom(new SQLExprTableSource(new SQLIdentifierExpr(TABLE_DUAL)));
                    return new ParseResult(defaultKeyspace, stmt);
                } else if (tableSource instanceof SQLExprTableSource) {
                    String tableName = TableNameUtils.getTableSimpleName(((SQLExprTableSource) tableSource));
                    if (TABLE_DUAL.equalsIgnoreCase(tableName) || RoutePlan.systemTable(tableName)) {
                        return new ParseResult(defaultKeyspace, stmt);
                    }
                }
            } else if (!(selectQuery instanceof SQLUnionQuery)) {
                throw new SQLException("not supported sql: " + SQLUtils.toMySqlString(stmt, SQLUtils.NOT_FORMAT_OPTION));
            }
        }

        VtChangeSchemaVisitor visitor = new VtChangeSchemaVisitor(defaultKeyspace);
        stmt.accept(visitor);
        return new ParseResult(visitor.getNewDefaultKeyspace(), stmt);
    }

    private Map<String, String> parseComment(String sql) {
        //only support parse comment from the beginning of sql statement.
        Lexer lexer = new MySqlLexer(sql);
        lexer.setKeepComments(true);
        lexer.nextToken();

        Map<String, String> prop = new HashMap<>(16, 1);

        List<String> comments = lexer.readAndResetComments();
        if (comments == null || comments.isEmpty()) {
            return prop;
        }

        for (String comment : comments) {
            if (comment.startsWith(START_OF_MULTI_LINE_COMMENT) && comment.endsWith(END_OF_MULTI_LINE_COMMENT)) {
                comment = comment.substring(2, comment.length() - 2);
            }

            String[] commentContents = comment.split(",");
            for (String content : commentContents) {
                int operatorIedx = content.indexOf("=");
                if (operatorIedx == -1 || operatorIedx == 0 || operatorIedx == content.length() - 1) {
                    continue;
                }
                String leftExpr = content.substring(0, operatorIedx).trim();
                String rightExpr = content.substring(operatorIedx + 1).trim();
                prop.put(leftExpr, rightExpr);
            }
        }

        return prop;
    }

    public void cleanResultSets() throws SQLException {
        SQLException exp = null;
        for (VtRowList result : this.resultSets) {
            try {
                result.close();
            } catch (SQLException e) {
                exp = e;
            }
        }
        resultSets.clear();
        if (exp != null) {
            throw exp;
        }
    }

    public void cleanOpenedResultSets() throws SQLException {
        SQLException exp = null;
        for (VtRowList result : this.openedResultSets) {
            try {
                result.close();
            } catch (SQLException e) {
                exp = e;
            }
        }
        openedResultSets.clear();
        if (exp != null) {
            throw exp;
        }
    }

    protected boolean streamResults() {
        return (this.resultSetType == ResultSet.TYPE_FORWARD_ONLY &&
            this.resultSetConcurrency == ResultSet.CONCUR_READ_ONLY &&
            this.fetchSize == Integer.MIN_VALUE);
    }

    protected ResultSet executeQueryInternal(IContext ctx, String sql, Map<String, BindVariable> bindVariableMap, boolean returnGeneratedKeys) throws SQLException {
        this.retrieveGeneratedKeys = returnGeneratedKeys;
        this.batchedGeneratedKeys = null;

        ParseResult parseResult = null;
        try {
            parseResult = parseStatements(sql);
        } catch (ParserException e) {
            VtRowList result = executor.otherExecute(ctx, "", SafeSession.newSafeSession(this.connection), this.connection.getDefaultKeyspace(), sql, bindVariableMap);
            this.resultSets.add(result);
            return new VitessResultSet(result, this.connection);
        }

        this.startSummary(parseResult.getStatement());
        try {
            if (specialFunctionProcessing(parseResult)) {
                return new VitessResultSet(this.resultSets.get(0), this.connection);
            }

            if (!(parseResult.statement instanceof SQLSelectStatement)) {
                throw new SQLException("Can not issue data manipulation statements with executeQuery()");
            }

            VtRowList result = executor.execute(ctx, "", SafeSession.newSafeSession(this.connection), parseResult.schema, parseResult.statement, bindVariableMap);
            this.resultSets.add(result);
            if (result != null) {
                this.lastInsertId = result.getInsertID();
            } else {
                this.lastInsertId = 0;
            }
            return new VitessResultSet(result, this.connection);
        } catch (SQLException e) {
            cleanResultSets();
            this.lastInsertId = 0;
            errorCount(sql, bindVariableMap, e);
            throw e;
        } finally {
            this.endSummary();
        }
    }

    protected ResultSet executeMultiQueryInternal(IContext ctx, List<String> sqls, List<Map<String, BindVariable>> bindVariableMapList, boolean returnGeneratedKeys) throws SQLException {
        this.retrieveGeneratedKeys = returnGeneratedKeys;
        this.batchedGeneratedKeys = null;
        List<ParseResult> parseResults = new ArrayList<>(sqls.size());
        for (String sql : sqls) {
            ParseResult parseResult = parseStatements(sql);
            parseResults.add(parseResult);
        }
        try {
            List<SQLStatement> batchStmts = parseResults.stream().map(ParseResult::getStatement).collect(Collectors.toList());
            List<VtRowList> vtRowLists = executor.batchExecute(ctx, "multiQuery", SafeSession.newSafeSession(this.connection), parseResults.get(0).getSchema(), batchStmts, bindVariableMapList);
            this.resultSets.addAll(vtRowLists);
            return new VitessResultSet(resultSets.get(0), this.connection);
        } catch (SQLException e) {
            cleanResultSets();
            this.lastInsertId = 0;
            throw e;
        }
    }

    protected int executeMultiQueryUpdateInternal(IContext ctx, List<String> sqls, List<Map<String, BindVariable>> bindVariableMapList, boolean clearBatchedGeneratedKeys,
                                                  boolean returnGeneratedKeys) throws SQLException {
        this.retrieveGeneratedKeys = returnGeneratedKeys;
        if (clearBatchedGeneratedKeys) {
            batchedGeneratedKeys = null;
        }

        List<ParseResult> parseResults = new ArrayList<>(sqls.size());
        for (String sql : sqls) {
            ParseResult parseResult = parseStatements(sql);
            parseResults.add(parseResult);
        }

        try {
            List<SQLStatement> batchStmts = parseResults.stream().map(ParseResult::getStatement).collect(Collectors.toList());
            List<VtRowList> vtRowLists = executor.batchExecute(ctx, "multiQuery", SafeSession.newSafeSession(this.connection), parseResults.get(0).getSchema(), batchStmts, bindVariableMapList);
            this.resultSets.addAll(vtRowLists);

            if (!resultSets.isEmpty()) {
                this.lastInsertId = resultSets.get(0).getInsertID();
                return (int) resultSets.get(0).getRowsAffected();
            } else {
                this.lastInsertId = 0;
                return 0;
            }
        } catch (SQLException e) {
            cleanResultSets();
            this.lastInsertId = 0;
            throw e;
        }
    }

    protected ResultSet executeStreamQueryInternal(IContext ctx, String sql, Map<String, BindVariable> bindVariableMap, boolean returnGeneratedKeys) throws SQLException {
        this.retrieveGeneratedKeys = returnGeneratedKeys;
        this.batchedGeneratedKeys = null;

        ParseResult parseResult = parseStatements(sql);
        this.startSummary(parseResult.getStatement());

        try {
            if (!(parseResult.statement instanceof SQLSelectStatement)) {
                throw new SQLException("Can not issue data manipulation statements with executeQuery()");
            }

            if (specialFunctionProcessing(parseResult)) {
                return new VitessResultSet(this.resultSets.get(0), this.connection);
            }
            this.resultSets.add(executor.streamExecute(ctx, "", SafeSession.newSafeSession(this.connection), parseResult.schema, parseResult.statement, bindVariableMap));
            this.lastInsertId = 0;

            return new VitessResultSet(this.resultSets.get(0), this.connection);
        } catch (SQLException e) {
            cleanResultSets();
            this.lastInsertId = 0;
            errorCount(sql, bindVariableMap, e);
            throw e;
        } finally {
            this.endSummary();
        }
    }

    protected void executeInternal(IContext ctx, String sql, Map<String, BindVariable> bindVariableMap, boolean returnGeneratedKeys) throws SQLException {
        this.retrieveGeneratedKeys = returnGeneratedKeys;
        this.batchedGeneratedKeys = null;

        ParseResult parseResult = null;
        try {
            parseResult = parseStatements(sql);
        } catch (ParserException e) {
            VtRowList result = executor.otherExecute(ctx, "", SafeSession.newSafeSession(this.connection), this.connection.getDefaultKeyspace(), sql, bindVariableMap);
            this.resultSets.add(result);
            return;
        }

        this.startSummary(parseResult.getStatement());

        try {
            if (specialFunctionProcessing(parseResult)) {
                return;
            }
            if (streamResults() && parseResult.statement instanceof SQLSelectStatement) {
                this.resultSets.add(executor.streamExecute(ctx, "", SafeSession.newSafeSession(this.connection), parseResult.schema, parseResult.statement, bindVariableMap));
            } else {
                this.resultSets.add(executor.execute(ctx, "", SafeSession.newSafeSession(this.connection), parseResult.schema, parseResult.statement, bindVariableMap));
            }

            if (!this.resultSets.isEmpty()) {
                this.lastInsertId = resultSets.get(resultSets.size() - 1).getInsertID();
            } else {
                this.lastInsertId = 0;
            }
            return;
        } catch (SQLException e) {
            cleanResultSets();
            this.lastInsertId = 0;
            errorCount(sql, bindVariableMap, e);
            throw e;
        } finally {
            this.endSummary();
        }
    }

    protected boolean getExecuteInternalResult() {
        if (resultSets.isEmpty()) {
            return false;
        }
        return resultSets.get(0).isQuery();
    }

    public void removeOpenResultSet(VtRowList result) {
        openedResultSets.remove(result);
    }

    protected int executeUpdateInternal(IContext ctx, String sql, Map<String, BindVariable> bindVariableMap, boolean clearBatchedGeneratedKeys, boolean returnGeneratedKeys) throws SQLException {
        this.retrieveGeneratedKeys = returnGeneratedKeys;
        if (clearBatchedGeneratedKeys) {
            batchedGeneratedKeys = null;
        }

        ParseResult parseResult = parseStatements(sql);
        this.startSummary(parseResult.getStatement());

        try {
            if (parseResult.statement instanceof SQLSelectStatement) {
                throw new SQLException("Can not issue select statements with executeUpdate()");
            }
            VtRowList result = executor.execute(ctx, "", SafeSession.newSafeSession(this.connection), parseResult.schema, parseResult.statement, bindVariableMap);
            this.resultSets.add(result);
            if (result != null) {
                this.lastInsertId = result.getInsertID();
                return (int) result.getRowsAffected();
            } else {
                this.lastInsertId = 0;
                return 0;
            }
        } catch (SQLException e) {
            cleanResultSets();
            this.lastInsertId = 0;
            errorCount(sql, bindVariableMap, e);
            throw e;
        } finally {
            this.endSummary();
        }
    }

    private int[] executeBatchUsingMultiQueries(final IContext ctx, final boolean returnGeneratedKeys) throws SQLException {
        if (batchSqls == null || batchSqls.isEmpty()) {
            return new int[0];
        }

        this.retrieveGeneratedKeys = returnGeneratedKeys;
        int nbrCommands = batchSqls.size();
        if (this.retrieveGeneratedKeys) {
            this.batchedGeneratedKeys = new ArrayList<>(nbrCommands);
        }

        int[] updateCounts = new int[nbrCommands];
        for (int i = 0; i < nbrCommands; i++) {
            updateCounts[i] = EXECUTE_FAILED;
        }

        List<ParseResult> parseResults = new ArrayList<>(nbrCommands);
        for (String sql : batchSqls) {
            ParseResult parseResult = parseStatements(sql);
            parseResults.add(parseResult);
        }

        try {
            List<SQLStatement> batchStmts = parseResults.stream().map(ParseResult::getStatement).collect(Collectors.toList());
            List<VtRowList> vtRowLists = executor.batchExecute(ctx, "multiQuery", SafeSession.newSafeSession(this.connection), parseResults.get(0).getSchema(), batchStmts, bindVariableMapList);
            this.resultSets.addAll(vtRowLists);
            if (resultSets != null && !resultSets.isEmpty()) {
                for (int rowIndex = 0; rowIndex < nbrCommands; rowIndex++) {
                    VtRowList vtRowList = resultSets.get(rowIndex);
                    this.lastInsertId = vtRowList.getInsertID();
                    boolean query = vtRowList.isQuery();
                    if (query) {
                        updateCounts[rowIndex] = -1;
                    } else {
                        updateCounts[rowIndex] = (int) vtRowList.getRowsAffected();
                    }
                    getBatchedGeneratedKeys(0);
                }
                return updateCounts;
            } else {
                this.lastInsertId = 0;
                return new int[0];
            }
        } catch (SQLException e) {
            cleanResultSets();
            this.lastInsertId = 0;
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            clearBatchInternal();
        }
    }

    protected int[] executeBatchInternal(IContext ctx, boolean returnGeneratedKeys) throws SQLException {
        if (batchSqls == null || batchSqls.isEmpty()) {
            return new int[0];
        }
        Boolean rewriteBatchedStatements = Utils.getBoolean(this.connection.getProperties(), VitessPropertyKey.REWRITE_BATCHED_STATEMENTS.getKeyName());
        if (rewriteBatchedStatements != null && rewriteBatchedStatements) {
            return executeBatchUsingMultiQueries(ctx, returnGeneratedKeys);
        }

        this.retrieveGeneratedKeys = returnGeneratedKeys;
        int nbrCommands = batchSqls.size();
        if (this.retrieveGeneratedKeys) {
            this.batchedGeneratedKeys = new ArrayList<>(nbrCommands);
        }

        int[] updateCounts = new int[nbrCommands];
        for (int i = 0; i < nbrCommands; i++) {
            updateCounts[i] = EXECUTE_FAILED;
        }

        int bindVarSize = bindVariableMapList.size();
        int commandIndex = 0;
        try {
            for (commandIndex = 0; commandIndex < nbrCommands; commandIndex++) {
                Map<String, BindVariable> bindVariableMap = (commandIndex < bindVarSize) ? bindVariableMapList.get(commandIndex) : new HashMap<>(16, 1);
                updateCounts[commandIndex] = executeUpdateInternal(ctx, batchSqls.get(commandIndex), bindVariableMap, false, returnGeneratedKeys);
                getBatchedGeneratedKeys(0);
            }
            return updateCounts;
        } finally {
            clearBatchInternal();
        }
    }

    protected VtResultSet getGeneratedKeysInternal() throws SQLException {
        long numKeys = getUpdateCount();
        return getGeneratedKeysInternal(numKeys);
    }

    protected VtResultSet getGeneratedKeysInternal(long numKeys) throws SQLException {
        if (numKeys > 1) {
            throw new SQLException("does not support insert multiple rows in one sql statement");
        }
        long generatedKey = lastInsertId;
        List<List<VtResultValue>> rows = new ArrayList<>();
        VtResultSet vtStaticResultSet = new VtResultSet(getGeneratedKeyField(), rows);
        if (!this.resultSets.isEmpty()) {
            if (generatedKey < 0) {
                throw new SQLException("generatedKey error");
            }
            if (generatedKey != 0 && (numKeys > 0)) {
                List<VtResultValue> row = Collections.singletonList(VtResultValue.newVtResultValue(Query.Type.UINT64, BigInteger.valueOf(generatedKey)));
                rows.add(row);
            }
        }
        return vtStaticResultSet;
    }

    protected void getBatchedGeneratedKeys(int maxKeys) throws SQLException {
        if (this.retrieveGeneratedKeys) {
            VtResultSet rs = maxKeys == 0 ? getGeneratedKeysInternal() : getGeneratedKeysInternal(maxKeys);
            this.batchedGeneratedKeys.add(rs);
        }
    }

    protected List<String> split(String sql) throws SQLException {
        if (StringUtils.isEmpty(sql)) {
            throw new SQLException("trying to execute empty queries " + sql);
        }
        List<String> sqls = SplitMultiQueryUtils.splitMulti(sql);
        if (sqls.isEmpty()) {
            throw new SQLException("trying to execute empty queries " + sqls);
        }

        return sqls;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        logger.debug("statement execute: " + sql);

        cleanResultSets();

        List<String> sqls = split(sql);

        ResultSet returnResultSet = null;
        synchronized (checkClosed().getConnectionMutex()) {
            try (IContext ctx = this.queryTimeout == 0 ? VtContext.withCancel(this.context) : VtContext.withDeadline(this.context, this.queryTimeout, TimeUnit.SECONDS)) {
                if (sqls.size() > 1) {
                    returnResultSet = executeMultiQueryInternal(ctx, sqls, new ArrayList<>(), false);
                    return returnResultSet;
                }
                if (streamResults()) {
                    returnResultSet = executeStreamQueryInternal(ctx, sql, null, false);
                } else {
                    returnResultSet = executeQueryInternal(ctx, sql, null, false);
                }
                return returnResultSet;
            }
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        logger.debug("statement execute: " + sql);

        cleanResultSets();

        List<String> sqls = split(sql);

        int rc = -1;
        synchronized (checkClosed().getConnectionMutex()) {
            try (IContext ctx = this.queryTimeout == 0 ? VtContext.withCancel(this.context) : VtContext.withDeadline(this.context, this.queryTimeout, TimeUnit.SECONDS)) {
                if (sqls.size() > 1) {
                    rc = executeMultiQueryUpdateInternal(ctx, sqls, new ArrayList<>(), true, false);
                    return rc;
                }
                rc = executeUpdateInternal(ctx, sql, null, true, false);
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
        clearBatchInternal();
        cleanResultSets();
        cleanOpenedResultSets();
        locallyScopedConn.unregisterStatement(this);
        this.connection = null;
        this.isClosed = true;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRows() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            Object val = this.context.getContextValue("maxRows");
            return val != null ? (int) val : 0;
        }
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        if (max < 0) {
            throw new SQLException("max rows must be >= 0");
        }
        synchronized (checkClosed().getConnectionMutex()) {
            this.context.setContextValue("maxRows", max);
        }
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return this.queryTimeout;
        }
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (seconds < 0) {
            throw new SQLException("invalid query timeout " + seconds);
        }
        synchronized (checkClosed().getConnectionMutex()) {
            this.queryTimeout = seconds;
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public boolean execute(String inputSql) throws SQLException {
        logger.debug("statement execute: " + inputSql);

        cleanResultSets();

        List<String> sqls = split(inputSql);

        synchronized (checkClosed().getConnectionMutex()) {
            try (IContext ctx = this.queryTimeout == 0 ? VtContext.withCancel(this.context) : VtContext.withDeadline(this.context, this.queryTimeout, TimeUnit.SECONDS)) {
                if (sqls.size() > 1) {
                    executeMultiQueryInternal(ctx, sqls, new ArrayList<>(), false);
                    return getExecuteInternalResult();
                }
                executeInternal(ctx, inputSql, null, false);
                return getExecuteInternalResult();
            }
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (!resultSets.isEmpty() && resultSets.get(CURRENT_RESULT_INDEX).isQuery()) {
                return new VitessResultSet(resultSets.get(CURRENT_RESULT_INDEX), this.connection);
            }
            return null;
        }
    }

    @Override
    public int getUpdateCount() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.resultSets.isEmpty()) {
                return -1;
            }

            if (this.resultSets.get(CURRENT_RESULT_INDEX).isQuery()) {
                return -1;
            }

            return (int) this.resultSets.get(CURRENT_RESULT_INDEX).getRowsAffected();
        }
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return getMoreResults(CLOSE_CURRENT_RESULT);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_UNKNOWN;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return this.fetchSize;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        this.fetchSize = rows;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return this.resultSetConcurrency;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return this.resultSetType;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        logger.debug("statement add batch: " + sql);

        synchronized (checkClosed().getConnectionMutex()) {
            if (batchSqls == null) {
                batchSqls = new ArrayList<>();
            }
            batchSqls.add(sql);
            if (bindVariableMapList == null) {
                bindVariableMapList = new ArrayList<>();
            }
            bindVariableMapList.add(new HashMap<>(16, 1));
        }
    }

    @Override
    public void clearBatch() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            clearBatchInternal();
        }
    }

    protected void clearBatchInternal() throws SQLException {
        if (batchSqls != null) {
            batchSqls.clear();
        }
        if (bindVariableMapList != null) {
            bindVariableMapList.clear();
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {
        cleanResultSets();

        synchronized (checkClosed().getConnectionMutex()) {
            try (IContext ctx = this.queryTimeout == 0 ? VtContext.withCancel(this.context) : VtContext.withDeadline(this.context, this.queryTimeout, TimeUnit.SECONDS)) {
                // The JDBC spec doesn't forbid this, but doesn't provide for it either...we do..
                return executeBatchInternal(ctx, true);
            }
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return connection;
        }
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (resultSets.isEmpty()) {
                return false;
            }

            switch (current) {
                case CLOSE_CURRENT_RESULT:
                    resultSets.get(CURRENT_RESULT_INDEX).close();
                    break;
                case CLOSE_ALL_RESULTS:
                    cleanOpenedResultSets();
                    resultSets.get(CURRENT_RESULT_INDEX).close();
                    break;
                case KEEP_CURRENT_RESULT:
                    openedResultSets.add(resultSets.get(CURRENT_RESULT_INDEX));
                    break;
                default:
                    throw new SQLException("invalid argument " + current);
            }

            resultSets = resultSets.stream().skip(NEXT_RESULT_INDEX).limit(resultSets.size() - NEXT_RESULT_INDEX).collect(Collectors.toList());
            return !resultSets.isEmpty() && resultSets.get(CURRENT_RESULT_INDEX).isQuery();
        }
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (!this.retrieveGeneratedKeys) {
                throw new SQLException("Generated keys not requested");
            }

            if (this.batchedGeneratedKeys == null || this.batchedGeneratedKeys.isEmpty()) {
                return new VitessResultSet(getGeneratedKeysInternal(), this.connection);
            }

            // batch
            if (this.batchedGeneratedKeys.size() == 1) {
                return new VitessResultSet(this.batchedGeneratedKeys.get(0), this.connection);
            }

            VtResultSet vtResultSet = this.batchedGeneratedKeys.get(0);
            for (int i = 1; i < this.batchedGeneratedKeys.size(); i++) {
                vtResultSet.appendResult(this.batchedGeneratedKeys.get(i));
            }

            return new VitessResultSet(vtResultSet, this.connection);
        }
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        logger.debug("statement execute: " + sql);

        cleanResultSets();

        List<String> sqls = split(sql);

        int rc = -1;
        synchronized (checkClosed().getConnectionMutex()) {
            try (IContext ctx = this.queryTimeout == 0 ? VtContext.withCancel(this.context) : VtContext.withDeadline(this.context, this.queryTimeout, TimeUnit.SECONDS)) {
                if (sqls.size() > 1) {
                    rc = executeMultiQueryUpdateInternal(ctx, sqls, new ArrayList<>(), true, autoGeneratedKeys == RETURN_GENERATED_KEYS);
                    return rc;
                }
                rc = executeUpdateInternal(ctx, sql, null, true, autoGeneratedKeys == RETURN_GENERATED_KEYS);
            }
        }

        return rc;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        logger.debug("statement execute: " + sql);

        cleanResultSets();

        List<String> sqls = split(sql);

        int rc = -1;
        synchronized (checkClosed().getConnectionMutex()) {
            try (IContext ctx = this.queryTimeout == 0 ? VtContext.withCancel(this.context) : VtContext.withDeadline(this.context, this.queryTimeout, TimeUnit.SECONDS)) {
                if (sqls.size() > 1) {
                    rc = executeMultiQueryUpdateInternal(ctx, sqls, new ArrayList<>(), true, columnIndexes != null && columnIndexes.length > 0);
                    return rc;
                }
                rc = executeUpdateInternal(ctx, sql, null, true, columnIndexes != null && columnIndexes.length > 0);
            }
        }

        return rc;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        logger.debug("statement execute: " + sql);

        cleanResultSets();

        List<String> sqls = split(sql);

        int rc = -1;
        synchronized (checkClosed().getConnectionMutex()) {
            try (IContext ctx = this.queryTimeout == 0 ? VtContext.withCancel(this.context) : VtContext.withDeadline(this.context, this.queryTimeout, TimeUnit.SECONDS)) {
                if (sqls.size() > 1) {
                    rc = executeMultiQueryUpdateInternal(ctx, sqls, new ArrayList<>(), true, columnNames != null && columnNames.length > 0);
                    return rc;
                }
                rc = executeUpdateInternal(ctx, sql, null, true, columnNames != null && columnNames.length > 0);
            }
        }

        return rc;
    }

    @Override
    public boolean execute(String inputSql, int autoGeneratedKeys) throws SQLException {
        logger.debug("statement execute: " + inputSql);

        cleanResultSets();

        List<String> sqls = split(inputSql);

        synchronized (checkClosed().getConnectionMutex()) {
            try (IContext ctx = this.queryTimeout == 0 ? VtContext.withCancel(this.context) : VtContext.withDeadline(context, this.queryTimeout, TimeUnit.SECONDS)) {
                if (sqls.size() > 1) {
                    executeMultiQueryInternal(ctx, sqls, new ArrayList<>(), autoGeneratedKeys == RETURN_GENERATED_KEYS);
                    return getExecuteInternalResult();
                }
                executeInternal(ctx, inputSql, null, autoGeneratedKeys == RETURN_GENERATED_KEYS);
                return getExecuteInternalResult();
            }
        }
    }

    @Override
    public boolean execute(String inputSql, int[] columnIndexes) throws SQLException {
        logger.debug("statement execute: " + inputSql);

        cleanResultSets();

        List<String> sqls = split(inputSql);

        synchronized (checkClosed().getConnectionMutex()) {
            try (IContext ctx = this.queryTimeout == 0 ? VtContext.withCancel(this.context) : VtContext.withDeadline(this.context, this.queryTimeout, TimeUnit.SECONDS)) {
                if (sqls.size() > 1) {
                    executeMultiQueryInternal(ctx, sqls, new ArrayList<>(), columnIndexes != null && columnIndexes.length > 0);
                    return getExecuteInternalResult();
                }
                executeInternal(ctx, inputSql, null, columnIndexes != null && columnIndexes.length > 0);
                return getExecuteInternalResult();
            }
        }
    }

    @Override
    public boolean execute(String inputSql, String[] columnNames) throws SQLException {
        logger.debug("statement execute: " + inputSql);

        cleanResultSets();

        List<String> sqls = split(inputSql);

        synchronized (checkClosed().getConnectionMutex()) {
            try (IContext ctx = this.queryTimeout == 0 ? VtContext.withCancel(this.context) : VtContext.withDeadline(this.context, this.queryTimeout, TimeUnit.SECONDS)) {
                if (sqls.size() > 1) {
                    executeMultiQueryInternal(ctx, sqls, new ArrayList<>(), columnNames != null && columnNames.length > 0);
                    return getExecuteInternalResult();
                }
                executeInternal(ctx, inputSql, null, columnNames != null && columnNames.length > 0);
                return getExecuteInternalResult();
            }
        }
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        VitessConnection locallyScopedConn = this.connection;
        if (locallyScopedConn == null) {
            return true;
        }
        synchronized (locallyScopedConn.getConnectionMutex()) {
            return this.isClosed;
        }
    }

    private Boolean specialFunctionProcessing(ParseResult parseResult) throws SQLException {
        SQLStatement stmt = parseResult.getStatement();

        if (!(stmt instanceof SQLSelectStatement)) {
            return Boolean.FALSE;
        }

        SQLSelectQuery selectQuery = ((SQLSelectStatement) stmt).getSelect().getQuery();
        if (!(selectQuery instanceof SQLSelectQueryBlock)) {
            return Boolean.FALSE;
        }

        List<SQLSelectItem> selectList = ((SQLSelectQueryBlock) selectQuery).getSelectList();
        if (selectList == null || selectList.size() != 1) {
            return Boolean.FALSE;
        }

        SQLSelectItem selectItem = selectList.get(0);
        if (selectItem == null || selectItem.getExpr() == null) {
            return Boolean.FALSE;
        }
        return this.processByExpr(selectItem);
    }

    private Boolean processByExpr(SQLSelectItem selectItem) throws SQLException {
        SQLExpr expr = selectItem.getExpr();
        String alias = selectItem.getAlias();

        String methodName = "";
        long lastInsertId = -1L;
        if (expr instanceof SQLMethodInvokeExpr) {
            methodName = ((SQLMethodInvokeExpr) expr).getMethodName();
            if (SQLUtils.nameEquals(methodName, FUNCATION_LAST_INSERT_ID)) {
                List<SQLExpr> parameterList = ((SQLMethodInvokeExpr) expr).getParameters();
                if (!parameterList.isEmpty()) {
                    return Boolean.FALSE;
                }
                methodName += "()";
                lastInsertId = this.connection.getSession().getLastInsertId();
            }
        } else if (expr instanceof SQLVariantRefExpr) {
            String name = ((SQLVariantRefExpr) expr).getName();
            if (SQLUtils.nameEquals(name, FUNCATION_IDENTITY)) {
                methodName = name;
                lastInsertId = this.connection.getSession().getLastInsertId();
            }
        }

        if (!StringUtil.isNullOrEmpty(methodName) && lastInsertId != -1L) {
            List<List<VtResultValue>> rows = new ArrayList<>();
            List<VtResultValue> valueList = new ArrayList<>();
            valueList.add(VtResultValue.newVtResultValue(Query.Type.UINT64, lastInsertId));
            rows.add(valueList);

            Query.Field field = Query.Field.newBuilder()
                .setName(StringUtil.isNullOrEmpty(alias) ? methodName : alias)
                .setJdbcClassName("java.math.BigInteger")
                .setPrecision(20)
                .setColumnLength(20)
                .setType(Query.Type.UINT64).build();

            this.resultSets.add(new VtResultSet(new Query.Field[] {field}, rows));
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    private void startSummary(SQLStatement sqlStatement) {
        String sqlType;
        if (sqlStatement instanceof SQLSelectStatement) {
            sqlType = "Select";
        } else if (sqlStatement instanceof SQLInsertStatement) {
            sqlType = "Insert";
        } else if (sqlStatement instanceof SQLReplaceStatement) {
            sqlType = "Replace";
        } else if (sqlStatement instanceof SQLUpdateStatement) {
            sqlType = "Update";
        } else if (sqlStatement instanceof SQLDeleteStatement) {
            sqlType = "Delete";
        } else {
            sqlType = "Other";
        }
        statementTypeHistogramTimer = statementTypeHistogram.labels(sqlType, connection.getDefaultKeyspace(), VitessJdbcProperyUtil.getRole(connection.getProperties()))
            .startTimer();
    }

    private void endSummary() {
        if (statementTypeHistogramTimer != null) {
            statementTypeHistogramTimer.observeDuration();
        }
        StatementCollector.getStatementCounter().inc();
    }

    private void errorCount(String sql, Map<String, BindVariable> bindVariableMap, SQLException e) {
        String charEncoding = connection.getProperties().getProperty(VitessPropertyKey.CHARACTER_ENCODING.getKeyName());
        SqlErrorCollector.getInstance().add(connection.getDefaultKeyspace(), sql, bindVariableMap, charEncoding, e);
        StatementCollector.getStatementErrorCounter().labels(connection.getDefaultKeyspace(), VitessJdbcProperyUtil.getRole(connection.getProperties())).inc();
    }

    private Query.Field[] getGeneratedKeyField() {
        if (generatedKeyField == null) {
            synchronized (VitessStatement.class) {
                if (generatedKeyField == null) {
                    Query.Field field = Query.Field.newBuilder().setName("GENERATED_KEY").setJdbcClassName("java.math.BigInteger")
                        .setType(Query.Type.UINT64).setColumnLength(20).setPrecision(20).build();
                    generatedKeyField = new Query.Field[] {field};
                    return generatedKeyField;
                }
            }
        }
        return generatedKeyField;
    }

    @Getter
    @AllArgsConstructor
    public static class ParseResult {
        private final String schema;

        private final SQLStatement statement;
    }

}
