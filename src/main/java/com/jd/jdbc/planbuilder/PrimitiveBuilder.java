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

import com.google.protobuf.UnknownFieldSet;
import com.jd.jdbc.VSchemaManager;
import com.jd.jdbc.engine.Engine;
import static com.jd.jdbc.engine.Engine.PulloutOpcode.PulloutExists;
import static com.jd.jdbc.engine.Engine.PulloutOpcode.PulloutIn;
import static com.jd.jdbc.engine.Engine.PulloutOpcode.PulloutNotIn;
import static com.jd.jdbc.engine.Engine.PulloutOpcode.PulloutValue;
import static com.jd.jdbc.engine.Engine.RouteOpcode.SelectDBA;
import static com.jd.jdbc.engine.Engine.RouteOpcode.SelectReference;
import com.jd.jdbc.engine.OrderedAggregateEngine;
import com.jd.jdbc.engine.RouteEngine;
import com.jd.jdbc.engine.table.TableRouteEngine;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.key.Bytes;
import com.jd.jdbc.planbuilder.Builder.PushSelectResponse;
import com.jd.jdbc.planbuilder.PlanBuilder.NewSubqueryPlanResponse;
import com.jd.jdbc.planbuilder.Symtab.Table;
import com.jd.jdbc.planbuilder.tableplan.TableRoutePlan;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.SqlParser;
import static com.jd.jdbc.sqlparser.SqlParser.GroupByExpr.ColName;
import static com.jd.jdbc.sqlparser.SqlParser.GroupByExpr.Literal;
import static com.jd.jdbc.sqlparser.SqlParser.HAVING_STR;
import static com.jd.jdbc.sqlparser.SqlParser.SelectExpr.AliasedExpr;
import static com.jd.jdbc.sqlparser.SqlParser.SelectExpr.Nextval;
import static com.jd.jdbc.sqlparser.SqlParser.SelectExpr.StarExpr;
import static com.jd.jdbc.sqlparser.SqlParser.WHERE_STR;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLLimit;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.SQLSetQuantifier;
import com.jd.jdbc.sqlparser.ast.expr.SQLAllColumnExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import static com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator.BooleanAnd;
import static com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator.BooleanOr;
import static com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator.Equality;
import com.jd.jdbc.sqlparser.ast.expr.SQLExistsExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLInSubQueryExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntegerExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefListExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLJoinTableSource;
import static com.jd.jdbc.sqlparser.ast.statement.SQLJoinTableSource.JoinType;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelect;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectOrderByItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSubqueryTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionOperator;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQueryTableSource;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtFindOriginVisitor;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtFindOriginVisitor.SubqueryInfo;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRewriteTableSchemaVisitor;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import static com.jd.jdbc.sqlparser.utils.JdbcConstants.MYSQL;
import com.jd.jdbc.sqlparser.utils.TableNameUtils;
import com.jd.jdbc.sqltypes.VtPlanValue;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.tindexes.LogicTable;
import com.jd.jdbc.tindexes.TableIndex;
import com.jd.jdbc.vindexes.VKeyspace;
import static com.jd.jdbc.vindexes.VschemaConstant.CODE_PINNED_TABLE;
import static com.jd.jdbc.vindexes.VschemaConstant.TYPE_PINNED_TABLE;
import static com.jd.jdbc.vindexes.VschemaConstant.TYPE_REFERENCE;
import static com.jd.jdbc.vindexes.VschemaConstant.TYPE_SEQUENCE;
import com.jd.jdbc.vindexes.hash.Binary;
import com.jd.jdbc.vindexes.hash.BinaryHash;
import com.jd.jdbc.vindexes.hash.Hash;
import com.jd.jdbc.vitess.VitessDataSource;
import io.netty.util.internal.StringUtil;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import vschema.Vschema;

@Data
public class PrimitiveBuilder {
    private static final Log log = LogFactory.getLog(PrimitiveBuilder.class);

    private final VSchemaManager vm;

    private final String defaultKeyspace;

    private Builder builder;

    private Symtab symtab;

    private Jointab jointab;

    public PrimitiveBuilder(VSchemaManager vm, String defaultKeyspace) {
        this.vm = vm;
        this.defaultKeyspace = defaultKeyspace;
    }

    public PrimitiveBuilder(VSchemaManager vm, String defaultKeyspace, Jointab jt) {
        this(vm, defaultKeyspace);
        this.jointab = jt;
    }

    /**
     * @param sel
     * @param outer
     * @throws Exception
     */
    public void processSelect(final SQLSelectStatement sel, final Symtab outer) throws SQLException {
        SQLSelectQuery query = sel.getSelect().getQuery();
        if (query instanceof MySqlSelectQueryBlock) {
            MySqlSelectQueryBlock selectQueryBlock = (MySqlSelectQueryBlock) query;
            SQLTableSource tableSource = selectQueryBlock.getFrom();
            this.processTableSource(tableSource);
            // Set the outer symtab after processing of FROM clause.
            // This is because correlation is not allowed there.
            this.symtab.setOuter(outer);
            SQLExpr where = selectQueryBlock.getWhere();
            if (where != null) {
                this.pushFilter(where, WHERE_STR);
            }
            this.checkAggregates(selectQueryBlock);
            this.pushSelectExprs(selectQueryBlock);
            SQLSelectGroupByClause groupBy = selectQueryBlock.getGroupBy();
            if (groupBy != null && groupBy.getHaving() != null) {
                this.pushFilter(groupBy.getHaving(), HAVING_STR);
            }
            this.pushOrderBy(selectQueryBlock.getOrderBy());
            this.pushLimit(selectQueryBlock.getLimit());
            this.builder.pushMisc(selectQueryBlock);
        } else {
            throw new SQLException("unsupported sql statement: " + SQLUtils.toMySqlString(sel, SQLUtils.NOT_FORMAT_OPTION).trim());
        }
    }

    private String baseProcess(final SQLExprTableSource stmt, final String tableName, String keyspaceName) {
        final String schema = stmt.getSchema();
        if (schema != null && !schema.isEmpty()) {
            keyspaceName = schema;
        }

        String vindex = null;
        try {
            vindex = vm.getVindex(keyspaceName, tableName);
        } catch (Exception e) {
            log.error("get vindex fail, keyspace " + keyspaceName + ", table " + tableName, e);
        }
        return vindex;
    }

    /**
     * pushFilter identifies the target route for the specified bool expr,
     * pushes it down, and updates the route info if the new constraint improves
     * the primitive. This function can push to a WHERE or HAVING clause.
     *
     * @param where
     * @param whereType
     * @throws SQLException
     */
    public void pushFilter(SQLExpr where, String whereType) throws SQLException {
        List<SQLExpr> filters = PlanBuilder.splitAndExpression(new ArrayList<>(), where);
        PlanBuilder.reorderBySubquery(filters);
        for (SQLExpr filter : filters) {
            FindOriginResponse findOriginResponse = this.findOrigin(filter);
            List<PulloutSubqueryPlan> pullouts = findOriginResponse.getPulloutSubqueryPlanList();
            Builder origin = findOriginResponse.getBuilder();
            SQLExpr expr = findOriginResponse.getPushExpr();
            if (origin instanceof RoutePlan) {
                RouteEngine routeEngine = ((RoutePlan) origin).getRouteEngine();
                if (SelectDBA.equals(routeEngine.getRouteOpcode())) {
                    VtRewriteTableSchemaVisitor visitor = new VtRewriteTableSchemaVisitor();
                    expr.accept(visitor);
                    SQLException exception = visitor.getException();
                    if (exception != null) {
                        throw exception;
                    }
                    List<EvalEngine.Expr> tableNameExpressionList = visitor.getTableNameExpressionList();
                    if (tableNameExpressionList != null && !tableNameExpressionList.isEmpty()) {
                        routeEngine.getSysTableKeyspaceExpr().addAll(tableNameExpressionList);
                    }
                }
            }
            // The returned expression may be complex. Resplit before pushing.
            for (SQLExpr subExpr : PlanBuilder.splitAndExpression(new ArrayList<>(), expr)) {
                this.builder.pushFilter(this, subExpr, whereType, origin);
            }
            this.addPullouts(pullouts);
        }
    }

    private void addPullouts(List<PulloutSubqueryPlan> pulloutSubqueryPlanList) {
        for (PulloutSubqueryPlan pulloutSubqueryPlan : pulloutSubqueryPlanList) {
            pulloutSubqueryPlan.setUnderlying(this.builder);
            this.builder = pulloutSubqueryPlan;
            this.builder.reorder(0);
        }
    }

    /**
     * identifies the target route for the
     * select expressions and pushes them down.
     *
     * @param sel
     * @throws SQLException
     */
    private void pushSelectExprs(MySqlSelectQueryBlock sel) throws SQLException {
        List<ResultColumn> resultColumnList = this.pushSelectRoutes(sel.getSelectList());
        this.symtab.setResultColumns(resultColumnList);
        this.pushGroupBy(sel);
    }

    /**
     * is a convenience function that pushes all the select
     * expressions and returns the list of resultColumns generated for it.
     *
     * @param selectItemList
     * @return
     * @throws SQLException
     */
    private List<ResultColumn> pushSelectRoutes(List<SQLSelectItem> selectItemList) throws SQLException {
        List<ResultColumn> resultColumnList = new ArrayList<>(selectItemList.size());
        for (SQLSelectItem selectItem : selectItemList) {
            SqlParser.SelectExpr selectExpr = SqlParser.SelectExpr.type(selectItem);
            if (AliasedExpr.equals(selectExpr)) {
                FindOriginResponse findOriginResponse = this.findOrigin(selectItem.getExpr());
                List<PulloutSubqueryPlan> pulloutSubqueryPlanList = findOriginResponse.getPulloutSubqueryPlanList();
                SQLExpr expr = findOriginResponse.getPushExpr();
                Builder origin = findOriginResponse.getBuilder();
                selectItem.setExpr(expr);
                PushSelectResponse pushSelectResponse = this.builder.pushSelect(this, selectItem, origin);
                resultColumnList.add(pushSelectResponse.getResultColumn());
                this.addPullouts(pulloutSubqueryPlanList);
            } else if (StarExpr.equals(selectExpr)) {
                ExpandStarResponse expandStarResponse = this.expandStar(resultColumnList, selectItem);
                resultColumnList = expandStarResponse.getOutrcs();
                Boolean expanded = expandStarResponse.getExpanded();
                if (expanded) {
                    continue;
                }
                // We'll allow select * for simple routes.
                if (!(this.builder instanceof AbstractRoutePlan)) {
                    throw new SQLException("unsupported: '*' expression in cross-shard query");
                }
                // Validate keyspace reference if any.
                if (!(selectItem.getExpr() instanceof SQLAllColumnExpr)) {
                    String tableName = ((SQLPropertyExpr) selectItem.getExpr()).getOwnernName();
                    if (selectItem.getExpr() instanceof SQLPropertyExpr) {
                        if (((SQLPropertyExpr) selectItem.getExpr()).getOwner() instanceof SQLPropertyExpr) {
                            tableName = ((SQLPropertyExpr) ((SQLPropertyExpr) selectItem.getExpr()).getOwner()).getName();
                        }
                    }
                    if (tableName != null) {
                        this.symtab.findTable(tableName.toLowerCase());
                    }
                }
                resultColumnList.add(((AbstractRoutePlan) this.builder).pushAnonymous(selectItem));
            } else if (Nextval.equals(selectExpr)) {

            } else {
                throw new SQLException("BUG: unexpected select expression type: " + selectItem);
            }
        }
        return resultColumnList;
    }

    /**
     * expandStar expands a StarExpr and pushes the expanded
     * expressions down if the tables have authoritative column lists.
     * If not, it returns false.
     * This function breaks the abstraction a bit: it directly sets the
     * the Metadata for newly created expressions. In all other cases,
     * the Metadata is set through a symtab Find.
     *
     * @param inrcs
     * @param expr
     * @return
     * @throws SQLException
     */
    private ExpandStarResponse expandStar(List<ResultColumn> inrcs, SQLSelectItem expr) throws SQLException {
        List<Table> tables = this.symtab.allTables();
        if (tables == null || tables.isEmpty()) {
            // no table metadata available.
            return new ExpandStarResponse(inrcs, false);
        }
        if (expr.getExpr() instanceof SQLAllColumnExpr) {
            for (Table t : tables) {
                // All tables must have authoritative column lists.
                if (!t.getIsAuthoritative()) {
                    return new ExpandStarResponse(inrcs, false);
                }
            }
            boolean singleTable = tables.size() == 1;
            for (Table t : tables) {
                for (String col : t.getColumnNames()) {
                    if (singleTable) {
                        // If there's only one table, we use unqualified column names.
                        expr = new SQLSelectItem(new SQLIdentifierExpr(col, t.getColumns().get(col.toLowerCase())));
                    } else {
                        // If a and b have id as their column, then
                        // select * from a join b should result in
                        // select a.id as id, b.id as id from a join b.
                        expr = new SQLSelectItem(
                            new SQLPropertyExpr(t.getTableAlias(), col, t.getColumns().get(col.toLowerCase())),
                            col);
                    }
                    PushSelectResponse pushSelectResponse = this.builder.pushSelect(this, expr, t.getOrigin());
                    inrcs.add(pushSelectResponse.getResultColumn());
                }
            }
            return new ExpandStarResponse(inrcs, true);
        }

        // Expression qualified with table name.
        String tableName = ((SQLPropertyExpr) expr.getExpr()).getOwnernName();
        if (expr.getExpr() instanceof SQLPropertyExpr) {
            if (((SQLPropertyExpr) expr.getExpr()).getOwner() instanceof SQLPropertyExpr) {
                tableName = ((SQLPropertyExpr) ((SQLPropertyExpr) expr.getExpr()).getOwner()).getName();
            }
        }
        Table t = this.symtab.findTable(tableName);
        if (!t.getIsAuthoritative()) {
            return new ExpandStarResponse(inrcs, false);
        }
        for (String col : t.getColumnNames()) {
            expr = new SQLSelectItem(new SQLPropertyExpr(((SQLPropertyExpr) expr.getExpr()).getOwnernName(), col, t.getColumns().get(col.toLowerCase())));
            PushSelectResponse pushSelectResponse = this.builder.pushSelect(this, expr, t.getOrigin());
            inrcs.add(pushSelectResponse.getResultColumn());
        }
        return new ExpandStarResponse(inrcs, true);
    }

    /**
     * pushGroupBy processes the group by clause. It resolves all symbols
     * and ensures that there are no subqueries.
     *
     * @param sel
     * @throws SQLException
     */
    private void pushGroupBy(MySqlSelectQueryBlock sel) throws SQLException {
        if (sel.getDistionOption() == SQLSetQuantifier.DISTINCT) {
            this.builder.makeDistinct();
        }
        SQLSelectGroupByClause groupBy = sel.getGroupBy();
        if (groupBy != null && groupBy.getItems() != null && !groupBy.getItems().isEmpty()) {
            this.symtab.resolveSymbols(groupBy.getItems());
        }
        this.builder.pushGroupBy(groupBy);
    }

    /**
     * pushOrderBy pushes the order by clause into the primitives.
     * It resolves all symbols and ensures that there are no subqueries.
     *
     * @param orderBy
     * @throws SQLException
     */
    private void pushOrderBy(SQLOrderBy orderBy) throws SQLException {
        if (orderBy != null && orderBy.getItems() != null) {
            List<SQLExpr> nodeList = new ArrayList<>();
            for (SQLSelectOrderByItem selectOrderByItem : orderBy.getItems()) {
                nodeList.add(selectOrderByItem.getExpr());
            }
            this.symtab.resolveSymbols(nodeList);
        }
        this.builder = this.builder.pushOrderBy(orderBy);
        this.builder.reorder(0);
    }

    /**
     * @param limit
     * @throws Exception
     */
    private void pushLimit(SQLLimit limit) throws SQLException {
        if (limit == null) {
            return;
        }
        if (this.builder instanceof RoutePlan) {
            RoutePlan routePlan = (RoutePlan) this.builder;
            if (routePlan.isSingleShard()) {
                routePlan.setLimit(limit);
                return;
            }
        }
        if (limit.getParent() instanceof SQLUnionQuery) {
            throw new SQLFeatureNotSupportedException("Incorrect usage of UNION and LIMIT - add parens to disambiguate your query");
        }
        LimitPlan lb = new LimitPlan(this.builder);
        try {
            lb.setLimit(limit);
        } catch (SQLException e) {
            throw e;
        }
        this.builder = lb;
        this.builder.reorder(0);
    }

    /**
     * findOrigin identifies the right-most origin referenced by expr. In situations where
     * the expression references columns from multiple origins, the expression will be
     * pushed to the right-most origin, and the executor will use the results of
     * the previous origins to feed the necessary values to the primitives on the right.
     * <p>
     * If the expression contains a subquery, the right-most origin identification
     * also follows the same rules of a normal expression. This is achieved by
     * looking at the Externs field of its symbol table that contains the list of
     * external references.
     * <p>
     * Once the target origin is identified, we have to verify that the subquery's
     * route can be merged with it. If it cannot, we fail the query. This is because
     * we don't have the ability to wire up subqueries through expression evaluation
     * primitives. Consequently, if the plan for a subquery comes out as a Join,
     * we can immediately error out.
     * <p>
     * Since findOrigin can itself be called from within a subquery, it has to assume
     * that some of the external references may actually be pointing to an outer
     * query. The isLocal response from the symtab is used to make sure that we
     * only analyze symbols that point to the current symtab.
     * <p>
     * If an expression has no references to the current query, then the left-most
     * origin is chosen as the default.
     *
     * @param expr
     * @return
     * @throws SQLException
     */
    private FindOriginResponse findOrigin(SQLExpr expr) throws SQLException {
        // highestOrigin tracks the highest origin referenced by the expression.
        // Default is the First.
        Builder highestOrigin = this.builder.first();
        List<PulloutSubqueryPlan> pulloutSubqueryPlanList = new ArrayList<>();
        List<SubqueryInfo> subqueryInfoList = new ArrayList<>();

        VtFindOriginVisitor vtFindOriginVisitor = new VtFindOriginVisitor(this, highestOrigin, subqueryInfoList);
        expr.accept(vtFindOriginVisitor);

        List<SQLException> exceptionList = vtFindOriginVisitor.getExceptionList();
        if (exceptionList != null && !exceptionList.isEmpty()) {
            throw exceptionList.get(0);
        }

        highestOrigin = vtFindOriginVisitor.getHighestOrigin();
        subqueryInfoList = vtFindOriginVisitor.getSubqueryInfoList();
        if (subqueryInfoList != null && !subqueryInfoList.isEmpty()) {
            if (this.builder.isSplitTablePlan()) {
                throw new SQLFeatureNotSupportedException("unsupport pollout subqeury on split table");
            }
        }

        Map<SQLSelect, SQLExpr> constructsMap = vtFindOriginVisitor.getConstructsMap();

        for (SubqueryInfo sqi : subqueryInfoList) {
            if (sqi.getBldr() instanceof RoutePlan) {
                RoutePlan subRoute = (RoutePlan) sqi.getBldr();
                if (highestOrigin instanceof RoutePlan) {
                    RoutePlan highestRoute = (RoutePlan) highestOrigin;
                    if (subRoute != null && highestRoute.mergeSubquery(this, subRoute)) {
                        continue;
                    }
                }
            }
            if (sqi.getOrigin() != null) {
                throw new SQLException("unsupported: cross-shard correlated subquery");
            }

            Jointab.GenerateSubqueryVarResponse generateSubqueryVarResponse = this.jointab.generateSubqueryVars();
            String sqName = generateSubqueryVarResponse.getSq();
            String hasValues = generateSubqueryVarResponse.getHasValues();
            if (!constructsMap.containsKey(sqi.getAst())) {
                // (subquery) -> :_sq
                expr = SqlParser.replaceSelect(expr, sqi.getAst(), new SQLVariantRefExpr(":" + sqName));
                pulloutSubqueryPlanList.add(new PulloutSubqueryPlan(PulloutValue, sqName, hasValues, sqi.getBldr()));
                continue;
            }
            SQLExpr construct = constructsMap.get(sqi.getAst());
            if (construct instanceof SQLInSubQueryExpr) {
                if (!((SQLInSubQueryExpr) construct).isNot()) {
                    // a in (subquery) -> (:__sq_has_values = 1 and (a in ::__sq))
                    SQLBinaryOpExpr left = new SQLBinaryOpExpr(new SQLVariantRefExpr(":" + hasValues), Equality, new SQLIntegerExpr(1), MYSQL);
                    SQLExpr right = SqlParser.replaceSelect(construct, sqi.getAst(), new SQLVariantRefListExpr("::" + sqName));
                    SQLBinaryOpExpr newExpr = new SQLBinaryOpExpr(left, BooleanAnd, right, MYSQL);
                    expr = SqlParser.replaceInSubQueryExpr((SQLInSubQueryExpr) expr, (SQLInSubQueryExpr) construct, newExpr);
                    pulloutSubqueryPlanList.add(new PulloutSubqueryPlan(PulloutIn, sqName, hasValues, sqi.getBldr()));
                } else {
                    // a not in (subquery) -> (:__sq_has_values = 0 or (a not in ::__sq))
                    SQLBinaryOpExpr left = new SQLBinaryOpExpr(new SQLVariantRefExpr(":" + hasValues), Equality, new SQLIntegerExpr(0), MYSQL);
                    SQLExpr right = SqlParser.replaceSelect(construct, sqi.getAst(), new SQLVariantRefListExpr("::" + sqName));
                    SQLBinaryOpExpr newExpr = new SQLBinaryOpExpr(left, BooleanOr, right, MYSQL);
                    expr = SqlParser.replaceInSubQueryExpr((SQLInSubQueryExpr) expr, (SQLInSubQueryExpr) construct, newExpr);
                    pulloutSubqueryPlanList.add(new PulloutSubqueryPlan(PulloutNotIn, sqName, hasValues, sqi.getBldr()));
                }
            } else if (construct instanceof SQLExistsExpr) {
                // exists (subquery) -> :__sq_has_values
                expr = SqlParser.replaceExistsExpr((SQLExistsExpr) expr, (SQLExistsExpr) construct, new SQLVariantRefExpr(":" + hasValues));
                pulloutSubqueryPlanList.add(new PulloutSubqueryPlan(PulloutExists, sqName, hasValues, sqi.getBldr()));
            }
        }
        return new FindOriginResponse(pulloutSubqueryPlanList, highestOrigin, expr);
    }

    /**
     * processDMLTable analyzes the FROM clause for DMLs and returns a route.
     *
     * @param tableSource
     * @return
     * @throws SQLException
     */
    public AbstractRoutePlan processDmlTable(SQLTableSource tableSource) throws SQLException {
        this.processTableSource(tableSource);
        if (!(this.builder instanceof AbstractRoutePlan)) {
            throw new SQLFeatureNotSupportedException("unsupported: multi-shard or vindex write statement");
        }
        return (AbstractRoutePlan) this.builder;
    }

    /**
     * @param tableExpr
     * @throws SQLException
     */
    private void processTableSource(SQLTableSource tableExpr) throws SQLException {
        if (tableExpr instanceof SQLExprTableSource) {
            processExprTable((SQLExprTableSource) tableExpr);
        } else if (tableExpr instanceof SQLJoinTableSource) {
            processJoinTable((SQLJoinTableSource) tableExpr);
        } else if (tableExpr instanceof SQLSubqueryTableSource || tableExpr instanceof SQLUnionQueryTableSource) {
            processSubqueryTable(tableExpr);
        } else {
            throw new SQLException("BUG: unexpected table expression type: " + SQLUtils.toMySqlString(tableExpr, SQLUtils.NOT_FORMAT_OPTION).trim());
        }
    }

    /**
     * @param exprTableSource
     * @throws SQLException
     */
    private void processExprTable(SQLExprTableSource exprTableSource) throws SQLException {
        buildTablePrimitive(exprTableSource);
    }

    private void processJoinTable(SQLJoinTableSource joinTableSource) throws SQLException {
        switch (joinTableSource.getJoinType()) {
            case JOIN:
            case COMMA:
            case STRAIGHT_JOIN:
            case INNER_JOIN:
            case LEFT_OUTER_JOIN:
                break;
            case RIGHT_OUTER_JOIN:
                this.convertToLeftJoin(joinTableSource);
                break;
            default:
                throw new SQLException("unsupported: " + joinTableSource.getJoinType().name);
        }
        this.processTableSource(joinTableSource.getLeft());
        PrimitiveBuilder rpb = new PrimitiveBuilder(this.vm, this.defaultKeyspace, this.jointab);
        rpb.processTableSource(joinTableSource.getRight());

        //TODO: remove and support join on split table
        if ((this.getBuilder().isSplitTablePlan()) || (rpb.getBuilder().isSplitTablePlan())) {
            throw new SQLFeatureNotSupportedException("unsupport join sql exoression on split table");
        }

        this.join(rpb, joinTableSource);
    }

    private void processSubqueryTable(SQLTableSource tableSource) throws SQLException {
        PrimitiveBuilder spb = new PrimitiveBuilder(this.vm, this.defaultKeyspace, this.jointab);
        if (tableSource instanceof SQLSubqueryTableSource) {
            SQLSelect select = ((SQLSubqueryTableSource) tableSource).getSelect();
            SQLSelectQuery selectQuery = select.getQuery();
            if (selectQuery instanceof MySqlSelectQueryBlock) {
                spb.processSelect(new SQLSelectStatement(select), null);
            }
        } else if (tableSource instanceof SQLUnionQueryTableSource) {
            SQLUnionQuery unionQuery = ((SQLUnionQueryTableSource) tableSource).getUnion();
            spb.processUnion(unionQuery, null);
        } else {
            throw new SQLException("BUG: unexpected SELECT type: " + SQLUtils.toMySqlString(tableSource.getParent(), SQLUtils.NOT_FORMAT_OPTION).trim());
        }

        if (!(spb.builder instanceof RoutePlan)) {
            NewSubqueryPlanResponse newSubqueryPlanResponse = PlanBuilder.newSubqueryPlan(tableSource.getAlias(), spb.builder);
            this.builder = newSubqueryPlanResponse.getSubqueryPlan();
            this.symtab = newSubqueryPlanResponse.getSymtab();
            this.builder.reorder(0);
            return;
        }
        RoutePlan subRoutePlan = (RoutePlan) spb.builder;

        // Since a route is more versatile than a subquery, we
        // build a route primitive that has the subquery in its
        // FROM clause. This allows for other constructs to be
        // later pushed into it.
        MySqlSelectQueryBlock subSelectQueryBlock = new MySqlSelectQueryBlock();
        subSelectQueryBlock.setFrom(tableSource);
        RoutePlan rb = new RoutePlan(subSelectQueryBlock);
        Symtab st = new Symtab(rb);
        rb.setTableSubstitutionList(subRoutePlan.getTableSubstitutionList());
        rb.setCondition(subRoutePlan.getCondition());
        rb.setRouteEngine(subRoutePlan.getRouteEngine());
        subRoutePlan.setRedirect(rb);

        // The subquery needs to be represented as a new logical table in the symtab.
        // The new route will inherit the routeOptions of the underlying subquery.
        // For this, we first build new vschema tables based on the columns returned
        // by the subquery, and re-expose possible vindexes. When added to the symtab,
        // a new set of column references will be generated against the new tables,
        // and those vindex maps will be returned. They have to replace the old vindex
        // maps of the inherited route options.
        Vschema.Table vschemaTable = Vschema.Table.newBuilder().build();
        for (ResultColumn rc : subRoutePlan.resultColumns()) {
            if (rc.getColumn().getVindex() == null) {
                continue;
            }
            // Check if a colvindex of the same name already exists.
            // Dups are not allowed in subqueries in this situation.
            for (Vschema.ColumnVindex colVindex : vschemaTable.getColumnVindexesList()) {
                if (colVindex.getColumnsList().get(0).equalsIgnoreCase(rc.getAlias())) {
                    throw new SQLException("duplicate column aliases: " + rc.getAlias());
                }
            }
            Vschema.ColumnVindex columnVindex = Vschema.ColumnVindex.newBuilder()
                .setName(new Hash().toString())
                .setColumn(rc.getAlias())
                .addAllColumns(new ArrayList<String>() {{
                    add(rc.getAlias());
                }}).build();
            vschemaTable = vschemaTable.toBuilder().addColumnVindexes(columnVindex).build();
        }
        st.addVSchemaTable(new SQLExprTableSource(new SQLIdentifierExpr(tableSource.getAlias())), vschemaTable, rb);
        this.builder = rb;
        this.symtab = st;
    }

    public void processUnion(SQLUnionQuery unionQuery, Symtab outer) throws SQLException {
        this.processPart(unionQuery.getFirstStatement(), outer, false);
        if (this.builder.isSplitTablePlan()) {
            throw new SQLFeatureNotSupportedException("doesn't support union sql on split table");
        }
        for (SQLUnionQuery.UnionSelect us : unionQuery.getUnionSelectList()) {
            PrimitiveBuilder rpb = new PrimitiveBuilder(this.vm, this.defaultKeyspace, this.jointab);
            rpb.processPart(us, outer, false);
            if (rpb.builder.isSplitTablePlan()) {
                throw new SQLFeatureNotSupportedException("doesn't support union sql on split table");
            }
            try {
                Builder builder = PlanBuilder.unionRouteMerge(this.getBuilder(), rpb.getBuilder(), us);
                this.setBuilder(builder);
            } catch (SQLException e) {
                if (!SQLUnionOperator.UNION_ALL.equals(us.getOperator())) {
                    throw e;
                }
                // we are merging between two routes - let's check if we can see so that we have the same amount of columns on both sides of the union
                List<ResultColumn> lhsCols = this.getBuilder().resultColumns();
                List<ResultColumn> rhsCols = rpb.getBuilder().resultColumns();
                if (!lhsCols.equals(rhsCols)) {
                    throw new SQLException("The used SELECT statements have a different number of columns");
                }
                this.setBuilder(new ConcatenatePlan(this.builder, rpb.builder));
            }
            this.symtab.setOuter(outer);
        }
        this.pushOrderBy(unionQuery.getOrderBy());
        this.pushLimit(unionQuery.getLimit());
    }

    private void processPart(SQLSelectQuery selectQuery, Symtab outer, Boolean hasParens) throws SQLException {
        SQLSelectQuery part;
        if (selectQuery instanceof SQLUnionQuery.UnionSelect) {
            part = ((SQLUnionQuery.UnionSelect) selectQuery).getSelectQuery();
        } else {
            part = selectQuery;
        }
        if (part instanceof SQLUnionQuery) {
            this.processUnion((SQLUnionQuery) part, outer);
            if (part.isBracket()) {
                // TODO: This is probably not a great idea. If we ended up with something other than a route, we'll lose the parens
                if (this.builder instanceof RoutePlan) {
                    ((RoutePlan) this.builder).getSelect().setBracket(true);
                }
            }
        } else if (part instanceof MySqlSelectQueryBlock) {
            if (part.isBracket()) {
                part.setBracket(false);
                this.processPart(part, outer, true);
                part.setBracket(true);
                // TODO: This is probably not a great idea. If we ended up with something other than a route, we'll lose the parens
                if (this.builder instanceof RoutePlan) {
                    ((RoutePlan) this.builder).getSelect().setBracket(true);
                }
                return;
            }
            if (((MySqlSelectQueryBlock) part).isCalcFoundRows()) {
                throw new SQLException("SQL_CALC_FOUND_ROWS not supported with union");
            }
            if (!hasParens) {
                this.checkOrderByAndLimit((MySqlSelectQueryBlock) part);
            }
            this.processSelect(new SQLSelectStatement(new SQLSelect(part)), outer);
        } else {
            throw new SQLException("BUG: unexpected SELECT type: " + SQLUtils.toMySqlString(part, SQLUtils.NOT_FORMAT_OPTION));
        }
    }

    private void checkOrderByAndLimit(MySqlSelectQueryBlock part) throws SQLException {
        if (part.getOrderBy() != null) {
            throw new SQLException("Incorrect usage of UNION and ORDER BY - add parens to disambiguate your query");
        }
        if (part.getLimit() != null) {
            throw new SQLException("Incorrect usage of UNION and LIMIT - add parens to disambiguate your query");
        }
    }

    private void convertToLeftJoin(SQLJoinTableSource joinTableSource) {
        SQLTableSource left = joinTableSource.getLeft();
        // If the LHS is a join, we have to parenthesize it.
        // Otherwise, it can be used as is.
        if (left instanceof SQLJoinTableSource) {
            left = new SQLJoinTableSource(
                ((SQLJoinTableSource) left).getLeft(),
                JoinType.JOIN,
                ((SQLJoinTableSource) left).getRight(),
                null);
        }
        joinTableSource.setLeft(joinTableSource.getRight());
        joinTableSource.setRight(left);
        joinTableSource.setJoinType(JoinType.LEFT_OUTER_JOIN);
    }

    private void join(PrimitiveBuilder rpb, SQLJoinTableSource joinTableSource) throws SQLException {
        // Merge the symbol tables. In the case of a left join, we have to
        // ideally create new symbols that originate from the join primitive.
        // However, this is not worth it for now, because the Push functions
        // verify that only valid constructs are passed through in case of left join.
        this.symtab.merge(rpb.symtab);

        if (!(this.builder instanceof RoutePlan) || !(rpb.builder instanceof RoutePlan)) {
            PlanBuilder.newJoin(this, rpb, joinTableSource);
            return;
        }

        RoutePlan lRoutePlan = (RoutePlan) this.builder;
        RoutePlan rRoutePlan = (RoutePlan) rpb.builder;
        // Try merging the routes.
        if (!lRoutePlan.joinCanMerge(this, rRoutePlan, joinTableSource)) {
            PlanBuilder.newJoin(this, rpb, joinTableSource);
            return;
        }

        if (SelectReference.equals(lRoutePlan.getRouteEngine().getRouteOpcode())) {
            // Swap the conditions & eroutes, and then merge.
            SQLExpr tempCondition = lRoutePlan.getCondition();
            lRoutePlan.setCondition(rRoutePlan.getCondition());
            rRoutePlan.setCondition(tempCondition);
            RouteEngine tempEngine = lRoutePlan.getRouteEngine();
            lRoutePlan.setRouteEngine(rRoutePlan.getRouteEngine());
            rRoutePlan.setRouteEngine(tempEngine);
        }
        lRoutePlan.getTableSubstitutionList().addAll(rRoutePlan.getTableSubstitutionList());
        rRoutePlan.setRedirect(lRoutePlan);

        // Merge the AST.
        MySqlSelectQueryBlock select = (MySqlSelectQueryBlock) lRoutePlan.getSelect();
        if (joinTableSource == null) {
            MySqlSelectQueryBlock rhsSelect = (MySqlSelectQueryBlock) rRoutePlan.getSelect();
            select.setFrom(new SQLJoinTableSource(select.getFrom(), JoinType.COMMA, rhsSelect.getFrom(), null));
        } else {
            select.setFrom(joinTableSource);
        }

        // Since the routes have merged, set st.singleRoute to point at
        // the merged route.
        this.symtab.setSingleRoute(lRoutePlan);
        if (joinTableSource == null) {
            return;
        }
        SQLExpr condition = joinTableSource.getCondition();
        if (condition == null) {
            return;
        }
        FindOriginResponse findOriginResponse = this.findOrigin(condition);
        List<PulloutSubqueryPlan> pulloutSubqueryPlanList = findOriginResponse.getPulloutSubqueryPlanList();
        SQLExpr expr = findOriginResponse.getPushExpr();
        joinTableSource.setCondition(expr);
        this.addPullouts(pulloutSubqueryPlanList);
        for (SQLExpr filter : PlanBuilder.splitAndExpression(new ArrayList<>(), condition)) {
            lRoutePlan.updatePlan(this, filter);
        }
    }

    /**
     * builds a primitive based on the table name.
     *
     * @param tableExpr
     * @throws SQLException
     */
    private void buildTablePrimitive(SQLExprTableSource tableExpr) throws SQLException {
        String tableName = TableNameUtils.getTableSimpleName(tableExpr);
        MySqlSelectQueryBlock select = new MySqlSelectQueryBlock();
        select.setFrom(tableExpr);

        Vschema.Keyspace ks = this.vm.getKeyspace(this.defaultKeyspace);
        VKeyspace keyspace = new VKeyspace(this.defaultKeyspace, ks.getSharded());

        if (RoutePlan.systemTable(tableExpr.getSchema())) {
            RoutePlan routeBuilder = new RoutePlan(select);
            Symtab symtab = new Symtab(routeBuilder);
            routeBuilder.setRouteEngine(new RouteEngine(SelectDBA, keyspace));
            this.builder = routeBuilder;
            this.symtab = symtab;
            symtab.addTable(new Table(tableExpr, routeBuilder));
            return;
        }

        LogicTable ltb = VitessDataSource.getLogicTable(this.defaultKeyspace, tableName);
        if (ltb != null) {
            String firstActualTableName = ltb.getActualTableList().get(0).getActualTableName();
            Vschema.Table vschemaTable = vm.getTable(this.defaultKeyspace, firstActualTableName);

            if (vschemaTable != null && !StringUtil.isNullOrEmpty(vschemaTable.getPinned())) {
                throw new SQLException("split-table query is not allowed on pinned table.");
            }

            TableRoutePlan tableRouteBuilder = new TableRoutePlan(select, this.vm);
            this.builder = tableRouteBuilder;
            Symtab symtab = new Symtab(tableRouteBuilder);
            this.symtab = symtab;
            symtab.addTSchemaTable(tableExpr, ltb, tableRouteBuilder);
            TableRouteEngine tableRouteEngine = new TableRouteEngine(Engine.TableRouteOpcode.SelectScatter, keyspace);
            tableRouteEngine.setTableName(tableName);
            tableRouteBuilder.setTableRouteEngine(tableRouteEngine);
            return;
        }

        Vschema.Table vschemaTable = vm.getTable(this.defaultKeyspace, tableName);
        if (vschemaTable == null) {

            if (ks.getSharded() && !tableName.equalsIgnoreCase("dual")) {
                throw new SQLSyntaxErrorException("table " + tableName + " not found");
            }
            if (tableName.equalsIgnoreCase("dual")) {
                vschemaTable = Vschema.Table.newBuilder()
                    .setType(TYPE_REFERENCE)
                    .addAllColumnVindexes(Collections.emptyList())
                    .addAllColumns(Collections.emptyList())
                    .setColumnListAuthoritative(false)
                    .setUnknownFields(UnknownFieldSet.newBuilder().build())
                    .build();
            } else {
                vschemaTable = Vschema.Table.newBuilder()
                    .setType(TYPE_PINNED_TABLE)
                    .addAllColumnVindexes(Collections.emptyList())
                    .addAllColumns(Collections.emptyList())
                    .setPinned(CODE_PINNED_TABLE)
                    .setColumnListAuthoritative(false)
                    .setUnknownFields(UnknownFieldSet.newBuilder().build())
                    .build();
            }
        }

        RoutePlan routeBuilder = new RoutePlan(select);
        this.builder = routeBuilder;
        Symtab symtab = new Symtab(routeBuilder);
        this.symtab = symtab;
        symtab.addVSchemaTable(tableExpr, vschemaTable, routeBuilder);

        RouteEngine routeEngine;
        if (TYPE_SEQUENCE.equalsIgnoreCase(vschemaTable.getType())) {
            routeEngine = new RouteEngine(Engine.RouteOpcode.SelectNext, keyspace);
            routeEngine.setVindex(new Binary());
            routeEngine.getVtPlanValueList().add(new VtPlanValue(VtValue.newVtValue(Query.Type.VARBINARY,
                Bytes.decodeToByteArray(vschemaTable.getPinned()))));
        } else if (TYPE_REFERENCE.equalsIgnoreCase(vschemaTable.getType())) {
            routeEngine = new RouteEngine(SelectReference, keyspace);
        } else if (!ks.getSharded()) {
            routeEngine = new RouteEngine(Engine.RouteOpcode.SelectUnsharded, keyspace);
        } else if (StringUtil.isNullOrEmpty(vschemaTable.getPinned())
            || (!StringUtil.isNullOrEmpty(vschemaTable.getPinned()) && SQLUtils.nameEquals("dual", tableName))) {
            routeEngine = new RouteEngine(Engine.RouteOpcode.SelectScatter, keyspace);
            routeEngine.setTargetDestination(null);
            routeEngine.setTargetTabletType(Topodata.TabletType.MASTER);
        } else {
            // Pinned tables have their keyspace ids already assigned.
            // Use the Binary vindex, which is the identity function
            // for keyspace id.
            routeEngine = new RouteEngine(Engine.RouteOpcode.SelectEqualUnique, keyspace, true, vschemaTable.getPinned());
            routeEngine.setVindex(new Binary());
            routeEngine.getVtPlanValueList().add(new VtPlanValue(VtValue.newVtValue(Query.Type.VARBINARY,
                Bytes.decodeToByteArray(vschemaTable.getPinned()))));
        }
        routeEngine.setTableName(tableName);
        routeBuilder.setRouteEngine(routeEngine);
    }

    /**
     * checkAggregates analyzes the select expression for aggregates. If it determines
     * that a primitive is needed to handle the aggregation, it builds an orderedAggregate
     * primitive and returns it. It returns a groupByHandler if there is aggregation it
     * can handle.
     *
     * @param sel
     * @return
     * @throws
     */
    private void checkAggregates(MySqlSelectQueryBlock sel) throws SQLException {
        Builder route = this.builder;

        if (route instanceof RoutePlan) {
            if (((RoutePlan) route).isSingleShard()) {
                return;
            }
        }

        // Check if we can allow aggregates.
        boolean hasAggregates;
        if (sel.getDistionOption() == SQLSetQuantifier.DISTINCT) {
            hasAggregates = true;
        } else {
            hasAggregates = PlanBuilder.selectItemsHasAggregates(sel.getSelectList());
        }

        SQLSelectGroupByClause groupBy = sel.getGroupBy();

        if (groupBy != null && !groupBy.getItems().isEmpty()) {
            hasAggregates = true;
        }

        if (!hasAggregates) {
            return;
        }

        // The query has aggregates. We can proceed only
        // if the underlying primitive is a route because
        // we need the ability to push down group by and
        // order by clauses.
        if (!(route instanceof AbstractRoutePlan)) {
            throw new SQLException("unsupported: cross-shard query with aggregates");
        }

        // If there is a distinct clause, we can check the select list
        // to see if it has a unique vindex reference. For example,
        // if the query was 'select distinct id, col from t' (with id
        // as a unique vindex), then the distinct operation can be
        // safely pushed down because the unique vindex guarantees
        // that each id can only be in a single shard. Without the
        // unique vindex property, the id could come from multiple
        // shards, which will require us to perform the grouping
        // at the vtgate level.
        if (sel.getDistionOption() == SQLSetQuantifier.DISTINCT) {
            for (SQLSelectItem selectItem : sel.getSelectList()) {

                SqlParser.SelectExpr selectExpr = SqlParser.SelectExpr.type(selectItem);
                if (AliasedExpr.equals(selectExpr)) {
                    SQLExpr sqlExpr = selectItem.getExpr();
                    if (this.builder instanceof TableRoutePlan) {
                        TableIndex tindex = this.symtab.getColumnTindex(sqlExpr, (TableRoutePlan) route);
                        if (tindex != null) {
                            return;
                        }
                    } else {
                        BinaryHash vindex = this.symtab.getColumnVindex(sqlExpr, (RoutePlan) route);
                        if (vindex != null && vindex.isUnique()) {
                            return;
                        }
                    }
                }
            }
        }

        // The group by clause could also reference a unique vindex. The above
        // example could itself have been written as
        // 'select id, col from t group by id, col', or a query could be like
        // 'select id, count(*) from t group by id'. In the above cases,
        // the grouping can be done at the shard level, which allows the entire query
        // to be pushed down. In order to perform this analysis, we're going to look
        // ahead at the group by clause to see if it references a unique vindex.
        if (this.groupByHasUniqueVindex(sel, (AbstractRoutePlan) route)) {
            return;
        }

        // We need an aggregator primitive.
        OrderedAggregateEngine eaggr = new OrderedAggregateEngine();
        this.builder = new OrderedAggregatePlan(route, eaggr, eaggr);
        this.builder.reorder(0);
    }

    /**
     * groupbyHasUniqueVindex looks ahead at the group by expression to see if
     * it references a unique vindex.
     * <p>
     * The vitess group by rules are different from MySQL because it's not possible
     * to match the MySQL behavior without knowing the schema. For example:
     * 'select id as val from t group by val' will have different interpretations
     * under MySQL depending on whether t has a val column or not.
     * In vitess, we always assume that 'val' references 'id'. This is achieved
     * by the symbol table resolving against the select list before searching
     * the tables.
     * <p>
     * In order to look ahead, we have to overcome the chicken-and-egg problem:
     * group by needs the select aliases to be built. Select aliases are built
     * on push-down. But push-down decision depends on whether group by expressions
     * reference a vindex.
     * To overcome this, the look-ahead has to perform a search that matches
     * the group by analyzer. The flow is similar to oa.PushGroupBy, except that
     * we don't search the ResultColumns because they're not created yet. Also,
     * error conditions are treated as no match for simplicity; They will be
     * subsequently caught downstream.
     *
     * @param sel
     * @param routeBuilder
     * @return
     */
    private Boolean groupByHasUniqueVindex(MySqlSelectQueryBlock sel, AbstractRoutePlan routeBuilder) throws SQLException {

        SQLSelectGroupByClause groupByClause = sel.getGroupBy();
        if (groupByClause == null) {
            return false;
        }
        List<SQLExpr> groupByItemList = groupByClause.getItems();

        for (SQLExpr groupByItem : groupByItemList) {
            SQLExpr matchedExpr = null;
            SqlParser.GroupByExpr columnExpr = SqlParser.GroupByExpr.type(groupByItem);
            if (ColName.equals(columnExpr)) {
                matchedExpr = findAlias(groupByItem, sel.getSelectList());
                if (matchedExpr == null) {
                    matchedExpr = groupByItem;
                }
            } else if (Literal.equals(columnExpr)) {
                if (!(groupByItem instanceof SQLIntegerExpr)) {
                    continue;
                }
                int num = ((SQLIntegerExpr) groupByItem).getNumber().intValue();
                if (num < 1 || num > sel.getSelectList().size()) {
                    continue;
                }
                SQLSelectItem item = sel.getSelectList().get(num - 1);
                SqlParser.SelectExpr selectExpr = SqlParser.SelectExpr.type(item);
                if (AliasedExpr.equals(selectExpr)) {
                    matchedExpr = item.getExpr();
                }
            } else {
                continue;
            }
            if (routeBuilder instanceof TableRoutePlan) {
                TableIndex tindex = this.symtab.getColumnTindex(matchedExpr, (TableRoutePlan) routeBuilder);
                if (tindex != null) {
                    return true;
                }
            } else {
                BinaryHash vindex = this.symtab.getColumnVindex(matchedExpr, (RoutePlan) routeBuilder);
                if (vindex != null && vindex.isUnique()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @param colName
     * @param selectItemList
     * @return
     */
    private SQLExpr findAlias(SQLExpr colName, List<SQLSelectItem> selectItemList) {
        // Qualified column names cannot match an (unqualified) alias.
        if (colName instanceof SQLPropertyExpr) {
            return null;
        }
        // See if this references an alias.
        for (SQLSelectItem selectItem : selectItemList) {
            SqlParser.SelectExpr selectExpr = SqlParser.SelectExpr.type(selectItem);
            if (AliasedExpr.equals(selectExpr)) {
                if (colName instanceof SQLIdentifierExpr) {
                    SQLExpr selectItemExpr = selectItem.getExpr();

                    String groupByName = ((SQLIdentifierExpr) colName).getName();
                    String selectItemAlias = selectItem.getAlias();
                    String selectItemName = null;
                    if (selectItemExpr instanceof SQLIdentifierExpr) {
                        selectItemName = ((SQLIdentifierExpr) selectItemExpr).getName();
                    } else if (selectItemExpr instanceof SQLPropertyExpr) {
                        selectItemName = ((SQLPropertyExpr) selectItemExpr).getName();
                    }
                    //是否需要忽略大小写
                    if (SQLUtils.nameEquals(groupByName, selectItemAlias)) {
                        return selectItemExpr;
                    }
                    if (SQLUtils.nameEquals(groupByName, selectItemName)) {
                        return selectItemExpr;
                    }
                }
            }
        }
        return null;
    }

    public Boolean finalizeUnshardedDmlSubqueries(SQLObject... nodes) {
        return Boolean.TRUE;
    }

    @Getter
    @AllArgsConstructor
    private static class FindOriginResponse {
        private final List<PulloutSubqueryPlan> pulloutSubqueryPlanList;

        private final Builder builder;

        private final SQLExpr pushExpr;
    }

    @Getter
    @AllArgsConstructor
    private static class ExpandStarResponse {
        private final List<ResultColumn> outrcs;

        private final Boolean expanded;
    }
}
