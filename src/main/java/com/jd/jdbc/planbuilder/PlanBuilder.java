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

package com.jd.jdbc.planbuilder;

import com.google.common.collect.Sets;
import com.jd.jdbc.VSchemaManager;
import com.jd.jdbc.common.Constant;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.JoinEngine;
import com.jd.jdbc.engine.Plan;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.ProjectionEngine;
import com.jd.jdbc.engine.SendEngine;
import com.jd.jdbc.engine.SingleRowEngine;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.key.Bytes;
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.planbuilder.gen4.Gen4Planner;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.SqlParser;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator;
import com.jd.jdbc.sqlparser.ast.expr.SQLHexExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLInListExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntegerExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLLiteralExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLTextLiteralExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLCommitStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLDeleteStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLJoinTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLReleaseSavePointStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLRollbackStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelect;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQueryBlock;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSetStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLStartTransactionStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSubqueryTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQueryTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLUpdateStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.BindVarNeeds;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlInsertReplaceStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.CheckNodeTypesVisitor;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtHasSubqueryVisitor;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRemoveDbNameVisitor;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqlparser.utils.TableNameUtils;
import com.jd.jdbc.sqltypes.VtPlanValue;
import com.jd.jdbc.tindexes.LogicTable;
import com.jd.jdbc.vindexes.VKeyspace;
import com.jd.jdbc.vitess.VitessDataSource;
import io.netty.util.internal.StringUtil;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import vschema.Vschema;

public class PlanBuilder {
    private static final Log LOGGER = LogFactory.getLog(PlanBuilder.class);

    private static final boolean GEN4_PLAN_ENABLE = Boolean.getBoolean(Constant.GEN4_PLAN_ENABLE);

    /**
     * BuildFromStmt builds a plan based on the AST provided.
     *
     * @param stmt
     * @param vm
     * @param defaultKeyspace
     * @param bindVarNeeds
     * @return Plan
     * @throws SQLException
     */
    public static Plan buildFromStmt(final SQLStatement stmt, final VSchemaManager vm, final String defaultKeyspace, final BindVarNeeds bindVarNeeds, Destination destination) throws SQLException {
        PrimitiveEngine instruction = createInstructionFor(stmt, "", vm, defaultKeyspace, destination);
        return new Plan(SqlParser.astToStatementType(stmt), instruction, bindVarNeeds);
    }

    /**
     * @param stmt
     * @param vm
     * @param defaultKeyspace
     * @return
     * @throws SQLException
     */
    private static PrimitiveEngine createInstructionFor(final SQLStatement stmt, final String query, final VSchemaManager vm, final String defaultKeyspace, Destination destination)
        throws SQLException {
        if (destination != null) {
            return buildPlanForBypass(stmt, vm, defaultKeyspace, destination);
        }

        if (stmt instanceof SQLSelectStatement) {
            if (GEN4_PLAN_ENABLE) {
                return Gen4Planner.gen4SelectStmtPlanner(query, defaultKeyspace, (SQLSelectStatement) stmt, null, vm);
            }
            if (((SQLSelectStatement) stmt).getSelect().getQuery() instanceof SQLUnionQuery) {
                return buildUnionPlan((SQLSelectStatement) stmt, vm, defaultKeyspace);
            }
            return buildSelectPlan((SQLSelectStatement) stmt, vm, defaultKeyspace);
        } else if (stmt instanceof MySqlInsertReplaceStatement) {
            return InsertPlan.newBuildInsertPlan((MySqlInsertReplaceStatement) stmt, vm, defaultKeyspace);
        } else if (stmt instanceof SQLUpdateStatement) {
            return UpdatePlan.newBuildUpdatePlan((SQLUpdateStatement) stmt, vm, defaultKeyspace);
        } else if (stmt instanceof SQLDeleteStatement) {
            return DeletePlan.newBuildDeletePlan((SQLDeleteStatement) stmt, vm, defaultKeyspace);
        } else if (stmt instanceof SQLSetStatement) {
            return SetPlan.buildSetPlan((SQLSetStatement) stmt);
        } else if (stmt instanceof SQLStartTransactionStatement || stmt instanceof SQLCommitStatement
            || stmt instanceof SQLRollbackStatement || stmt instanceof SQLReleaseSavePointStatement) {
            return null;
        }
        throw new SQLException("BUG: unexpected statement type: " + SQLUtils.toMySqlString(stmt, SQLUtils.NOT_FORMAT_OPTION));
    }

    private static PrimitiveEngine buildPlanForBypass(final SQLStatement stmt, final VSchemaManager vm, final String defaultKeyspace, final Destination destination)
        throws SQLException {
        if (stmt instanceof MySqlInsertReplaceStatement) {
            throw new SQLFeatureNotSupportedException("insert statement does not support execute by destination");
        }
        if (stmt instanceof SQLUpdateStatement) {
            SQLTableSource tableSource = ((SQLUpdateStatement) stmt).getTableSource();
            if (!(tableSource instanceof SQLExprTableSource)) {
                throw new SQLFeatureNotSupportedException("unsupported: multi-table update");
            }
            UpdatePlan.buildChangedVindexesValues((SQLUpdateStatement) stmt, vm.getTable(defaultKeyspace, TableNameUtils.getTableSimpleName((SQLExprTableSource) tableSource)), null);
        }

        Vschema.Keyspace ks = vm.getKeyspace(defaultKeyspace);
        VKeyspace keyspace = new VKeyspace(defaultKeyspace, ks.getSharded());

        return new SendEngine(keyspace, destination, SQLUtils.toMySqlString(stmt, SQLUtils.NOT_FORMAT_OPTION).trim(), stmt, isDMLStatement(stmt), true, true, true);
    }

    private static boolean isDMLStatement(SQLStatement stmt) {
        return stmt instanceof MySqlInsertReplaceStatement || stmt instanceof SQLUpdateStatement || stmt instanceof SQLDeleteStatement;
    }

    /**
     * @param stmt
     * @param vm
     * @param defaultKeyspace
     * @return
     * @throws SQLException
     */
    public static PrimitiveEngine buildSelectPlan(final SQLSelectStatement stmt, final VSchemaManager vm, final String defaultKeyspace) throws SQLException {
        PrimitiveEngine p = handleDualSelects(stmt.getSelect().getQuery(), vm, defaultKeyspace);
        if (p != null) {
            return p;
        }
        PrimitiveBuilder pb = new PrimitiveBuilder(vm, defaultKeyspace, Jointab.newJointab(SqlParser.getBindVars(stmt)));
        pb.processSelect(stmt, null);
        pb.getBuilder().wireup(pb.getBuilder(), pb.getJointab());
        return pb.getBuilder().getPrimitiveEngine();
    }

    public static PrimitiveEngine buildUnionPlan(final SQLSelectStatement stmt, final VSchemaManager vm, final String defaultKeyspace) throws SQLException {
        SQLUnionQuery unionQuery = (SQLUnionQuery) stmt.getSelect().getQuery();
        PrimitiveBuilder pb = new PrimitiveBuilder(vm, defaultKeyspace, Jointab.newJointab(SqlParser.getBindVars(stmt)));
        pb.processUnion(unionQuery, null);
        pb.getBuilder().wireup(pb.getBuilder(), pb.getJointab());
        return pb.getBuilder().getPrimitiveEngine();
    }

    public static Builder unionRouteMerge(final Builder left, final Builder right, final SQLUnionQuery.UnionSelect unionQuery) throws SQLException {
        if (!(left instanceof RoutePlan)) {
            throw new SQLFeatureNotSupportedException("unsupported: SELECT of UNION is non-trivial");
        }
        RoutePlan lroute = (RoutePlan) left;

        if (!(right instanceof RoutePlan)) {
            throw new SQLFeatureNotSupportedException("unsupported: SELECT of UNION is non-trivial");
        }
        RoutePlan rroute = (RoutePlan) right;

        if (!lroute.mergeUnion(rroute)) {
            throw new SQLFeatureNotSupportedException("unsupported: UNION cannot be executed as a single route");
        }

        if (lroute.getSelect() instanceof SQLUnionQuery) {
            lroute.setSelect(((SQLUnionQuery) lroute.getSelect()).addUnion(unionQuery));
            return lroute;
        } else {
            // 当左表是dual时，并无任何可以作为路由条件字段，因此需要左右routeplan颠倒，但是SQL不颠倒
            if (isDualTable(lroute)) {
                rroute.setRedirect(null);
                lroute.setRedirect(rroute);
                rroute.setSelect(new SQLUnionQuery(lroute.getSelect(), unionQuery.getOperator(), unionQuery.getSelectQuery()));
                return rroute;
            } else {
                lroute.setSelect(new SQLUnionQuery(lroute.getSelect(), unionQuery.getOperator(), unionQuery.getSelectQuery()));
                return lroute;
            }
        }
    }

    public static boolean isDualTable(final Builder builder) {
        if (!(builder instanceof RoutePlan)) {
            return false;
        }

        RoutePlan routePlan = (RoutePlan) builder;
        return routePlan.getRouteEngine().getRouteOpcode() == Engine.RouteOpcode.SelectReference
            && routePlan.getRouteEngine().getTableName().equalsIgnoreCase("dual");
    }

    /**
     * @param query
     * @param vm
     * @return
     * @throws SQLException
     */
    public static PrimitiveEngine handleDualSelects(SQLSelectQuery query, VSchemaManager vm, String defaultKeyspace) throws SQLException {
        if (!(query instanceof MySqlSelectQueryBlock)) {
            throw new SQLFeatureNotSupportedException("unsupported sql statement: " + SQLUtils.toMySqlString(query, SQLUtils.NOT_FORMAT_OPTION).trim());
        }

        SQLSelectQueryBlock sel = (SQLSelectQueryBlock) query;

        if (!isOnlyDual(sel)) {
            return null;
        }

        List<EvalEngine.Expr> exprs = new ArrayList<>(sel.getSelectList().size());
        List<String> cols = new ArrayList<>(sel.getSelectList().size());
        for (int i = 0; i < sel.getSelectList().size(); i++) {
            SQLSelectItem selItem = sel.getSelectList().get(i);
            try {
                EvalEngine.Expr expr = SqlParser.convert(selItem.getExpr());
                exprs.add(expr);
                cols.add(selItem.getAlias());
                if (StringUtils.isEmpty(cols.get(i))) {
                    cols.set(i, selItem.getExpr().toString());
                }
            } catch (SQLException e) {
                LOGGER.error(e.getMessage(), e);
                return null;
            }
        }
        VKeyspace keyspace = new VKeyspace(defaultKeyspace, vm.getKeyspace(defaultKeyspace).getSharded());
        return new ProjectionEngine(cols, exprs, new SingleRowEngine(query, keyspace));
    }

    /**
     * @param selectQuery
     * @return
     */
    private static Boolean isOnlyDual(SQLSelectQueryBlock selectQuery) throws SQLException {
        if (selectQuery.getWhere() != null || selectQuery.getGroupBy() != null || selectQuery.getLimit() != null || selectQuery.getOrderBy() != null) {
            return false;
        }

        SQLTableSource from = selectQuery.getFrom();
        if (from != null && !(from instanceof SQLExprTableSource)) {
            return false;
        }
        SQLExprTableSource exprTableSource = (SQLExprTableSource) from;
        String tableName = TableNameUtils.getTableSimpleName(exprTableSource);

        return SQLUtils.nameEquals("dual", tableName) && StringUtil.isNullOrEmpty(exprTableSource.getSchema());
    }

    /**
     * @param selectItemList
     * @return
     */
    public static Boolean selectItemsHasAggregates(List<SQLSelectItem> selectItemList) {
        //暂不支持groupy concat
        for (SQLSelectItem selectItem : selectItemList) {
            SQLExpr selectItemExpr = selectItem.getExpr();
//            if (selectItemExpr instanceof SQLAggregateExpr) {
//                String methodName = ((SQLAggregateExpr) selectItemExpr).getMethodName().toLowerCase();
//                if (SqlParser.AGGREGATES.get(methodName)) {
//                    return true;
//                }
//            }
            CheckNodeTypesVisitor visitor = new CheckNodeTypesVisitor(Sets.newHashSet(CheckNodeTypesVisitor.CheckNodeType.AGGREGATE));
            selectItemExpr.accept(visitor);
            if (visitor.getCheckResult()) {
                return true;
            }
        }
        return false;
    }

    /**
     * ResultFromNumber returns the result column index based on the column
     * order expression.
     *
     * @param rcs
     * @param val
     * @return
     * @throws SQLException
     */
    public static Integer resultFromNumber(List<ResultColumn> rcs, SQLLiteralExpr val) throws SQLException {
        if (!(val instanceof SQLIntegerExpr)) {
            throw new SQLException("column number is not an int");
        }
        int num = 0;
        try {
            num = Integer.parseInt(String.valueOf(((SQLIntegerExpr) val).getNumber()));
        } catch (NumberFormatException e) {
            throw new SQLException("error parsing column number: " + val);
        }
        if (num < 1 || num > rcs.size()) {
            throw new SQLException("column number out of range: " + num);
        }
        return num - 1;
    }

    /**
     * newJoin makes a new join using the two planBuilder. ajoin can be nil
     * if the join is on a ',' operator. lpb will contain the resulting join.
     * rpb will be discarded.
     *
     * @param lpb
     * @param rpb
     * @param joinTableSource
     */
    public static void newJoin(PrimitiveBuilder lpb, PrimitiveBuilder rpb, SQLJoinTableSource joinTableSource) throws SQLException {
        // This function converts ON clauses to WHERE clauses. The WHERE clause
        // scope can see all tables, whereas the ON clause can only see the
        // participants of the JOIN. However, since the ON clause doesn't allow
        // external references, and the FROM clause doesn't allow duplicates,
        // it's safe to perform this conversion and still expect the same behavior.

        Engine.JoinOpcode opcode = Engine.JoinOpcode.NormalJoin;
        if (joinTableSource != null) {
            if (SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN.equals(joinTableSource.getJoinType())) {
                opcode = Engine.JoinOpcode.LeftJoin;

                // For left joins, we have to push the ON clause into the RHS.
                // We do this before creating the join primitive.
                // However, variables of LHS need to be visible. To allow this,
                // we mark the LHS symtab as outer scope to the RHS, just like
                // a subquery. This make the RHS treat the LHS symbols as external.
                // This will prevent constructs from escaping out of the rpb scope.
                // At this point, the LHS symtab also contains symbols of the RHS.
                // But the RHS will hide those, as intended.
                rpb.getSymtab().setOuter(lpb.getSymtab());
                rpb.pushFilter(joinTableSource.getCondition(), SqlParser.WHERE_STR);
            } else if (joinTableSource.getUsing() != null && !joinTableSource.getUsing().isEmpty()) {
                throw new SQLFeatureNotSupportedException("unsupported: join with USING(column_list) clause");
            }
        }
        lpb.setBuilder(new JoinPlan(
            new HashMap<>(16, 1),
            lpb.getBuilder(),
            rpb.getBuilder(),
            new JoinEngine(
                opcode,
                new HashMap<>(16, 1)
            )
        ));
        lpb.getBuilder().reorder(0);
        if (joinTableSource == null || Engine.JoinOpcode.LeftJoin.equals(opcode)) {
            return;
        }
        lpb.pushFilter(joinTableSource.getCondition(), SqlParser.WHERE_STR);
    }

    public static NewSubqueryPlanResponse newSubqueryPlan(String alias, Builder bldr) throws SQLException {
        SubqueryPlan sq = new SubqueryPlan(bldr);

        // Create a 'table' that represents the subquery.
        Symtab.Table t = new Symtab.Table(new SQLExprTableSource(new SQLIdentifierExpr(alias)), sq);

        // Create column symbols based on the result column names.
        for (ResultColumn rc : bldr.resultColumns()) {
            if (t.getColumns().containsKey(rc.getAlias().toLowerCase())) {
                throw new SQLException("duplicate column names in subquery: " + rc.getAlias());
            }
            t.addColumn(rc.getAlias(), new Column(sq));
        }
        t.setIsAuthoritative(true);
        Symtab st = new Symtab();
        // AddTable will not fail because symtab is empty.
        st.addTable(t);

        return new NewSubqueryPlanResponse(sq, st);
    }

    /**
     * breaks up the Expr into AND-separated conditions
     * and appends them to filters, which can be shuffled and recombined
     * as needed.
     *
     * @param filters
     * @param node
     * @return
     */
    public static List<SQLExpr> splitAndExpression(List<SQLExpr> filters, SQLExpr node) {
        if (node == null) {
            return filters;
        }
        if (filters == null) {
            filters = new ArrayList<>();
        }
        if (node instanceof SQLBinaryOpExpr) {
            if (((SQLBinaryOpExpr) node).getOperator().equals(SQLBinaryOperator.BooleanAnd)) {
                filters = splitAndExpression(filters, ((SQLBinaryOpExpr) node).getLeft());
                return splitAndExpression(filters, ((SQLBinaryOpExpr) node).getRight());
            }
        }
        filters.add(node);
        return filters;
    }

    // removeKeyspaceFromColName removes the Qualifier.Qualifier on all ColNames in the expression tree
    public static SQLExpr removeKeyspaceFromColName(SQLExpr expr) {
        VtRemoveDbNameVisitor visitor = new VtRemoveDbNameVisitor();
        expr.accept(visitor);
        return expr;
    }

    /**
     * reorderBySubquery reorders the filters by pushing subqueries
     * to the end. This allows the non-subquery filters to be
     * pushed first because they can potentially improve the routing
     * plan, which can later allow a filter containing a subquery
     * to successfully merge with the corresponding route.
     *
     * @param filters
     */
    public static void reorderBySubquery(List<SQLExpr> filters) {
        int max = filters.size();
        for (int i = 0; i < max; i++) {
            if (!hasSubquery(filters.get(i))) {
                continue;
            }
            SQLExpr saved = filters.get(i);
            for (int j = i; j < filters.size() - 1; j++) {
                filters.set(j, filters.get(j + 1));
            }
            filters.set(filters.size() - 1, saved);
            max--;
        }
    }

    public static Boolean hasSubquery(SQLObject node) {
        VtHasSubqueryVisitor visitor = new VtHasSubqueryVisitor();
        node.accept(visitor);
        return visitor.getHasSubquery();
    }

    public static Boolean valEqual(SQLExpr a, SQLExpr b) {
        if (a instanceof SQLName && b instanceof SQLName) {
            return ((SQLName) a).getMetadata().equals(((SQLName) b).getMetadata());
        } else if (a instanceof SQLVariantRefExpr) {
            if (!(b instanceof SQLVariantRefExpr)) {
                return false;
            }
            return a.equals(b);
        } else if (a instanceof SQLLiteralExpr) {
            if (!(b instanceof SQLLiteralExpr)) {
                return false;
            }
            if (a instanceof SQLTextLiteralExpr) {
                if (b instanceof SQLTextLiteralExpr) {
                    return a.equals(b);
                } else if (b instanceof SQLHexExpr) {
                    return hexEquals((SQLHexExpr) b, (SQLTextLiteralExpr) a);
                }
            } else if (a instanceof SQLHexExpr) {
                return hexEquals((SQLHexExpr) a, (SQLLiteralExpr) b);
            } else if (a instanceof SQLIntegerExpr) {
                if (b instanceof SQLIntegerExpr) {
                    return a.equals(b);
                }
            }
        }
        return false;
    }

    private static Boolean hexEquals(SQLHexExpr a, SQLLiteralExpr b) {
        byte[] v = a.toBytes();
        if (b instanceof SQLTextLiteralExpr) {
            return Bytes.equal(v, ((SQLTextLiteralExpr) b).getText().getBytes());
        } else if (b instanceof SQLHexExpr) {
            return Bytes.equal(v, ((SQLHexExpr) b).toBytes());
        }
        return false;
    }

    public static List<LogicTable> getLogicTables(String keyspaceName, SQLObject sql) throws SQLException {
        List<LogicTable> logicTableList = new ArrayList<>();
        List<String> ltbNames = getTables(sql);
        for (String ltbName : ltbNames) {
            LogicTable ltb = VitessDataSource.getLogicTable(keyspaceName, ltbName);
            if (ltb == null) {
                throw new SQLException("Cannot find corresponding logic table, table name: " + ltbName);
            }
            logicTableList.add(ltb);
        }
        if (logicTableList.isEmpty()) {
            throw new SQLException("Cannot find any logic table");
        }
        return logicTableList;
    }

    /**
     * @param sql
     * @throws SQLException
     */
    public static List<String> getTables(SQLObject sql) throws SQLException {
        if (sql instanceof MySqlSelectQueryBlock) {
            SQLTableSource tableExpr = ((MySqlSelectQueryBlock) sql).getFrom();
            return getTablesFromTableExpr(tableExpr);
        } else if (sql instanceof SQLUnionQuery) {
            return getTablesFromUnionQuery((SQLUnionQuery) sql);
        } else if (sql instanceof SQLDeleteStatement) {
            SQLTableSource from = ((SQLDeleteStatement) sql).getFrom();
            if (from == null) {
                from = ((SQLDeleteStatement) sql).getTableSource();
            }
            return getTablesFromTableExpr(from);
        } else if (sql instanceof SQLUpdateStatement) {
            SQLTableSource from = ((SQLUpdateStatement) sql).getFrom();
            if (from == null) {
                from = ((SQLUpdateStatement) sql).getTableSource();
            }
            return getTablesFromTableExpr(from);
        } else if (sql instanceof MySqlInsertReplaceStatement) {
            SQLExprTableSource tableSource = ((MySqlInsertReplaceStatement) sql).getTableSource();
            return getTablesFromTableExpr(tableSource);
        }
        throw new SQLException("ERROR: unexpected sql expression type: " + SQLUtils.toMySqlString(sql, SQLUtils.NOT_FORMAT_OPTION).trim());
    }

    /**
     * @param tableExpr
     * @return
     * @throws SQLException
     */
    private static List<String> getTablesFromTableExpr(SQLTableSource tableExpr) throws SQLException {
        if (tableExpr instanceof SQLExprTableSource) {
            String tableName = TableNameUtils.getTableSimpleName(((SQLExprTableSource) tableExpr));
            return new ArrayList<String>() {{
                add(tableName);
            }};
        } else if (tableExpr instanceof SQLJoinTableSource) {
            return getTablesFromJoinTable((SQLJoinTableSource) tableExpr);
        } else if (tableExpr instanceof SQLSubqueryTableSource || tableExpr instanceof SQLUnionQueryTableSource) {
            return getTablesFromSubqueryTable(tableExpr);
        }
        throw new SQLException("BUG: unexpected table expression type: " + SQLUtils.toMySqlString(tableExpr, SQLUtils.NOT_FORMAT_OPTION).trim());
    }

    /**
     * @param joinTableSource
     * @return
     * @throws SQLException
     */
    private static List<String> getTablesFromJoinTable(SQLJoinTableSource joinTableSource) throws SQLException {
        List<String> leftTables = getTablesFromTableExpr(joinTableSource.getLeft());
        List<String> rightTables = getTablesFromTableExpr(joinTableSource.getRight());
        leftTables.addAll(rightTables);
        return leftTables;
    }

    /**
     * @param tableSource
     * @return
     * @throws SQLException
     */
    private static List<String> getTablesFromSubqueryTable(SQLTableSource tableSource) throws SQLException {
        if (tableSource instanceof SQLSubqueryTableSource) {
            SQLSelect select = ((SQLSubqueryTableSource) tableSource).getSelect();
            SQLSelectQuery selectQuery = select.getQuery();
            return getTables(selectQuery);
        } else if (tableSource instanceof SQLUnionQueryTableSource) {
            SQLUnionQuery unionQuery = ((SQLUnionQueryTableSource) tableSource).getUnion();
            return getTablesFromUnionQuery(unionQuery);
        }
        throw new SQLException("BUG: unexpected SELECT type: " + SQLUtils.toMySqlString(tableSource.getParent(), SQLUtils.NOT_FORMAT_OPTION).trim());
    }

    /**
     * @param unionQuery
     * @return
     * @throws SQLException
     */
    private static List<String> getTablesFromUnionQuery(final SQLUnionQuery unionQuery) throws SQLException {
        List<String> leftTable = getTables(unionQuery.getLeft());
        List<String> rightTable = getTables(unionQuery.getRight());
        leftTable.addAll(rightTable);
        return leftTable;
    }

    /**
     * getMatch returns the matched value if there is an equality
     * constraint on the specified column that can be used to
     * decide on a route.
     *
     * @param node
     * @param col
     * @return
     * @throws SQLException
     */
    public static MatchResult getMatch(final SQLExpr node, final String col) throws SQLException {
        List<SQLExpr> filters = splitAndExpression(null, node);
        VtPlanValue planValue;
        for (SQLExpr filter : filters) {
            if (filter instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) filter;
                if (!nameMatch(binaryOpExpr.getLeft(), col)) {
                    continue;
                }
                if (binaryOpExpr.getOperator() != SQLBinaryOperator.Equality) {
                    continue;
                }
                if (!SqlParser.isValue(binaryOpExpr.getRight())) {
                    continue;
                }
                planValue = SqlParser.newPlanValue(binaryOpExpr.getRight());
            } else if (filter instanceof SQLInListExpr) {
                SQLInListExpr listExpr = (SQLInListExpr) filter;
                if (!nameMatch(listExpr.getExpr(), col)) {
                    continue;
                }
                if (!SqlParser.isSimpleTuple(listExpr.getTargetList())) {
                    continue;
                }
                planValue = SqlParser.newPlanValue(listExpr.getTargetList());
            } else {
                continue;
            }
            return new MatchResult(planValue, true);
        }
        return new MatchResult(null, false);
    }

    private static boolean nameMatch(final SQLExpr node, final String col) {
        if (node instanceof SQLName) {
            String colName = ((SQLName) node).getSimpleName();
            return colName.equalsIgnoreCase(col);
        }
        return false;
    }

    public static MySqlSelectQueryBlock getFirstSelect(SQLSelectQuery selStmt) throws SQLException {
        if (selStmt == null) {
            return null;
        }
        if (selStmt instanceof MySqlSelectQueryBlock) {
            return (MySqlSelectQueryBlock) selStmt;
        } else if (selStmt instanceof SQLUnionQuery) {
            SQLUnionQuery sqlUnionQuery = (SQLUnionQuery) selStmt;
            return getFirstSelect(sqlUnionQuery.getLeft());
        } else {
            throw new SQLException("[BUG]: unknown type for SelectStatement");
        }
    }

    @Getter
    @AllArgsConstructor
    public static class NewSubqueryPlanResponse {
        private final SubqueryPlan subqueryPlan;

        private final Symtab symtab;
    }

    @Getter
    @AllArgsConstructor
    public static class MatchResult {
        private final VtPlanValue pv;

        private final boolean ok;
    }
}
