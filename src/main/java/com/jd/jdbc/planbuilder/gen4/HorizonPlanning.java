/*
Copyright 2023 JD Project Authors. Licensed under Apache-2.0.

Copyright 2022 The Vitess Authors.

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

package com.jd.jdbc.planbuilder.gen4;

import com.google.common.collect.Sets;
import com.jd.jdbc.common.tuple.ImmutablePair;
import com.jd.jdbc.common.tuple.ImmutableTriple;
import com.jd.jdbc.common.tuple.MutableTriple;
import com.jd.jdbc.common.tuple.Pair;
import com.jd.jdbc.common.tuple.Triple;
import com.jd.jdbc.common.util.CollectionUtils;
import com.jd.jdbc.context.PlanningContext;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.gen4.AbstractAggregateGen4;
import com.jd.jdbc.engine.gen4.GroupByParams;
import com.jd.jdbc.engine.gen4.MemorySortGen4Engine;
import com.jd.jdbc.engine.gen4.OrderByParamsGen4;
import com.jd.jdbc.planbuilder.MemorySortPlan;
import com.jd.jdbc.planbuilder.PlanBuilder;
import com.jd.jdbc.planbuilder.gen4.logical.DistinctGen4Plan;
import com.jd.jdbc.planbuilder.gen4.logical.FilterGen4Plan;
import com.jd.jdbc.planbuilder.gen4.logical.HashJoinPlan;
import com.jd.jdbc.planbuilder.gen4.logical.JoinGen4Plan;
import com.jd.jdbc.planbuilder.gen4.logical.LimitGen4Plan;
import com.jd.jdbc.planbuilder.gen4.logical.LogicalPlan;
import com.jd.jdbc.planbuilder.gen4.logical.MemorySortGen4Plan;
import com.jd.jdbc.planbuilder.gen4.logical.OrderedAggregateGen4Plan;
import com.jd.jdbc.planbuilder.gen4.logical.ProjectionGen4Plan;
import com.jd.jdbc.planbuilder.gen4.logical.RouteGen4Plan;
import com.jd.jdbc.planbuilder.gen4.logical.SimpleProjectionGen4Plan;
import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.planbuilder.semantics.TableInfo;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.planbuilder.semantics.Type;
import com.jd.jdbc.sqlparser.SqlParser;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.SQLOrderingSpecification;
import com.jd.jdbc.sqlparser.ast.SQLSetQuantifier;
import com.jd.jdbc.sqlparser.ast.expr.SQLAggregateExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLExprUtils;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntegerExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLMethodInvokeExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNullExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectOrderByItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQuery;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.CheckNodeTypesVisitor;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.RewriteHavingAggrWithOffsetVisitor;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRemoveDbNameExpectSystemDbVisitor;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRemoveDbNameInColumnVisitor;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqltypes.VtType;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import vschema.Vschema;

@Getter
@Setter
public class HorizonPlanning {

    private SQLSelectQuery sel;

    private QueryProjection qp;

    private boolean hasHaving;

    public HorizonPlanning(QueryProjection qp) {
        this.qp = qp;
    }

    public HorizonPlanning(SQLSelectQuery sel) {
        this.sel = sel;
    }

    public LogicalPlan planHorizon(PlanningContext ctx, LogicalPlan plan, boolean truncateColumns) throws SQLException {
        boolean isRoute = plan instanceof RouteGen4Plan;
        if (!isRoute && ctx.getSemTable().getNotSingleRouteErr() != null) {
            // If we got here, we don't have a single shard plan
            throw new SQLException(ctx.getSemTable().getNotSingleRouteErr());
        }

        if (isRoute && ((RouteGen4Plan) plan).isSingleShard()) {
            this.planSingleShardRoutePlan(this.sel, (RouteGen4Plan) plan);
            return plan;
        }

        // If the current plan is a simpleProjection, we want to rewrite derived expression.
        // In transformDerivedPlan (operator_transformers.go), derived tables that are not
        // a simple route are put behind a simpleProjection. In this simple projection,
        // every Route will represent the original derived table. Thus, pushing new expressions
        // to those Routes require us to rewrite them.
        // On the other hand, when a derived table is a simple Route, we do not put it under
        // a simpleProjection. We create a new Route that contains the derived table in the
        // FROM clause. Meaning that, when we push expressions to the select list of this
        // new Route, we do not want them to rewrite them.

        this.qp = new QueryProjection();
        this.qp.createQPFromSelect(this.sel);

        boolean needsOrdering = !this.qp.getOrderExprs().isEmpty();
        if (this.sel instanceof MySqlSelectQueryBlock) {
            SQLSelectGroupByClause groupBy = ((MySqlSelectQueryBlock) this.sel).getGroupBy();
            if (groupBy != null) {
                this.hasHaving = groupBy.getHaving() != null;
            }
        }

        boolean canShortcut = isRoute && !hasHaving && !needsOrdering;

        // If we still have a HAVING clause, it's because it could not be pushed to the WHERE,
        // so it probably has aggregations
        if (qp.needsAggregating() || hasHaving) {
            plan = this.planAggregations(ctx, plan);
            // if we already did sorting, we don't need to do it again
            needsOrdering = needsOrdering && !this.qp.isCanPushDownSorting();
        } else if (canShortcut) {
            planSingleShardRoutePlan(this.sel, (RouteGen4Plan) plan);
        } else {
            this.pushProjections(ctx, plan, this.qp.selectExprs);
        }

        // If we didn't already take care of ORDER BY during aggregation planning, we need to handle it now
        if (needsOrdering) {
            // this.planOr
            plan = this.planOrderBy(ctx, this.qp.orderExprs, plan);
        }

        plan = this.planDistinct(ctx, plan);

        if (!truncateColumns) {
            return plan;
        }
        plan = this.truncateColumnsIfNeeded(ctx, plan);

        return plan;
    }

    public static void planSingleShardRoutePlan(SQLSelectQuery sel, RouteGen4Plan rb) {
        stripDownQuery(sel, rb.select);
        VtRemoveDbNameExpectSystemDbVisitor visitor = new VtRemoveDbNameExpectSystemDbVisitor();
        rb.select.accept(visitor);
    }

    private static void stripDownQuery(SQLSelectQuery from, SQLSelectQuery to) {
        VtRemoveDbNameInColumnVisitor visitor = new VtRemoveDbNameInColumnVisitor();
        if (from instanceof MySqlSelectQueryBlock) {
            ((MySqlSelectQueryBlock) to).setGroupBy(((MySqlSelectQueryBlock) from).getGroupBy());
            ((MySqlSelectQueryBlock) to).setOrderBy(((MySqlSelectQueryBlock) from).getOrderBy());
            SQLOrderBy orderBy = ((MySqlSelectQueryBlock) to).getOrderBy();
            if (orderBy != null) {
                for (SQLSelectOrderByItem item : orderBy.getItems()) {
                    boolean isSpecialOrderBy = isSpecialOrderBy(item.getExpr());
                    if (item.getType() == null && !isSpecialOrderBy) {
                        item.setType(SQLOrderingSpecification.ASC);
                    }
                }
            }
            ((MySqlSelectQueryBlock) to).setHints(((MySqlSelectQueryBlock) from).getHints());
            ((MySqlSelectQueryBlock) to).getSelectList().addAll(((MySqlSelectQueryBlock) from).getSelectList());
            for (SQLSelectItem expr : ((MySqlSelectQueryBlock) to).getSelectList()) {
                expr.accept(visitor);
            }

        } else if (from instanceof SQLUnionQuery) {
            stripDownQuery(((SQLUnionQuery) from).getLeft(), ((SQLUnionQuery) to).getLeft());
            stripDownQuery(((SQLUnionQuery) from).getRight(), ((SQLUnionQuery) to).getRight());
        } else {

        }
    }

    private LogicalPlan planAggregations(PlanningContext ctx, LogicalPlan plan) throws SQLException {
        boolean isPushable = !this.isJoin(plan);
        List<QueryProjection.GroupBy> grouping = this.qp.getGrouping();
        boolean vindexOverlapWithGrouping = this.hasUniqueVindex(ctx.getSemTable(), grouping);
        if (isPushable && vindexOverlapWithGrouping) {
            // If we have a plan that we can push the group by and aggregation through, we don't need to do aggregation
            // at the vtgate level at all
            this.planAggregationWithoutOA(ctx, plan);
            LogicalPlan resultPlan = this.planOrderBy(ctx, this.qp.orderExprs, plan);
            LogicalPlan newPlan = this.planHaving(ctx, resultPlan);
            return newPlan;
        }

        return this.planAggrUsingOA(ctx, plan, grouping);
    }

    public static boolean isJoin(LogicalPlan plan) {
        if (plan instanceof JoinGen4Plan) {
            return true;
        }

        if (plan instanceof HashJoinPlan) {
            return true;
        }

        /*
          switch plan.(type) {
	case *joinGen4, *hashJoin:
		return true
	default:
		return false
	}
         */
        //TODO
        return false;
    }

    private boolean exprHasVindex(SemTable semTable, SQLExpr expr, boolean hasToBeUnique) {
        boolean isCol = expr instanceof SQLName;
        if (!isCol) {
            return false;
        }
        TableSet ts = semTable.recursiveDeps(expr);
        if (ts == null) {
            return false;
        }
        TableInfo tableInfo = semTable.tableInfoFor(ts);
        if (tableInfo == null) {
            return false;
        }
        Vschema.Table vschemaTable = tableInfo.getVindexTable();
        for (Vschema.ColumnVindex vindex : vschemaTable.getColumnVindexesList()) {
            // if len(vindex.Columns) > 1 || hasToBeUnique && !vindex.IsUnique() {
            if (vschemaTable.getColumnVindexesList().size() > 1 || hasToBeUnique && false) {
                return false;
            }
            if (Objects.equals(((SQLName) expr).getSimpleName(), vindex.getColumn())) {
                return true;
            }
        }
        return false;
    }

    private boolean exprHasUniqueVindex(SemTable semTable, SQLExpr expr) {
        return exprHasVindex(semTable, expr, true);
    }

    private boolean hasUniqueVindex(SemTable semTable, List<QueryProjection.GroupBy> groupByExprs) {
        for (QueryProjection.GroupBy groupByExpr : groupByExprs) {
            if (this.exprHasUniqueVindex(semTable, groupByExpr.weightStrExpr)) {
                return true;
            }
        }
        return false;
    }

    private LogicalPlan planAggrUsingOA(PlanningContext ctx, LogicalPlan plan, List<QueryProjection.GroupBy> grouping) throws SQLException {
        OrderedAggregateGen4Plan oa = new OrderedAggregateGen4Plan(new ArrayList<>(grouping.size()));
        List<QueryProjection.OrderBy> order;

        if (this.qp.isCanPushDownSorting()) {
            this.qp.alignGroupByAndOrderBy();
            // the grouping order might have changed, so we reload the grouping expressions
            grouping = this.qp.getGrouping();
            order = this.qp.orderExprs;
        } else {
            order = new ArrayList<>(grouping.size());
            for (QueryProjection.GroupBy expr : grouping) {
                order.add(expr.asOrderBy());
            }
        }

        // here we are building up the grouping keys for the OA,
        // but they are lacking the input offsets because we have yet to push the columns down
        for (QueryProjection.GroupBy expr : grouping) {
            GroupByParams groupByParams = new GroupByParams(expr.inner, true);
            oa.getGroupByKeys().add(groupByParams);
        }

        rewriterHaving();

        List<QueryProjection.Aggr> aggregationExprs = this.qp.aggregationExpressions();

        // If we have a distinct aggregating expression,
        // we handle it by pushing it down to the underlying input as a grouping column
        ImmutableTriple<List<QueryProjection.GroupBy>, List<Integer>, List<QueryProjection.Aggr>> distinctAggr = handleDistinctAggr(ctx, aggregationExprs);
        List<QueryProjection.GroupBy> distinctGroupBy = distinctAggr.getLeft();
        List<Integer> distinctOffsets = distinctAggr.getMiddle();
        List<QueryProjection.Aggr> aggrs = distinctAggr.getRight();

        if (distinctGroupBy.size() > 0) {
            grouping.addAll(distinctGroupBy);
            // all the distinct grouping aggregates use the same expression, so it should be OK to just add it once
            order.add(distinctGroupBy.get(0).asOrderBy());
            oa.setPreProcess(true);
        }

        PushAggregationResult pushAggregationResult = this.pushAggregation(ctx, plan, grouping, aggrs, false);
        if (!pushAggregationResult.isPushed()) {
            oa.setPreProcess(true);
            oa.setAggrOnEngine(true);
        }
        plan = pushAggregationResult.getOutput();

        LogicalPlan aggPlan = plan;
        ProjectionGen4Plan proj = null;
        boolean isRoute = plan instanceof RouteGen4Plan;
        boolean needsProj = !isRoute;
        if (needsProj) {
            aggPlan = proj;
        }
        List<AbstractAggregateGen4.AggregateParams> aggrParams = generateAggregateParams(aggrs, pushAggregationResult.getOutputAggrsOffset(), proj, pushAggregationResult.isPushed());
        if (proj != null) {

        }

        // Next we add the aggregation expressions and grouping offsets to the OA
        addColumnsToOA(ctx, oa, distinctGroupBy, aggrParams, distinctOffsets, pushAggregationResult.getGroupingOffsets(), aggregationExprs);

        aggPlan = planOrderBy(ctx, order, aggPlan);
        oa.setInput(aggPlan);

        return this.planHaving(ctx, oa);
    }

    /**
     * @param ctx
     * @param oa
     * @param distinctGroupBy  these are the group by expressions that where added because we have unique aggregations
     * @param aggrParams       these are the aggregate params we already have for non-distinct aggregations
     * @param distinctOffsets  distinctOffsets mark out where we need to use the distinctGroupBy offsets to create *engine.AggregateParams for the distinct aggregations
     * @param groupings        these are the offsets for the group by params
     * @param aggregationExprs aggregationExprs are all the original aggregation expressions the query requested
     */
    private void addColumnsToOA(PlanningContext ctx, OrderedAggregateGen4Plan oa, List<QueryProjection.GroupBy> distinctGroupBy, List<AbstractAggregateGen4.AggregateParams> aggrParams,
                                List<Integer> distinctOffsets,
                                List<Offsets> groupings, List<QueryProjection.Aggr> aggregationExprs) {
        if (distinctGroupBy.size() == 0) {
            oa.setAggregates(aggrParams);
        } else {
            int count = groupings.size() - distinctOffsets.size();
            int lastOffset = distinctOffsets.get(distinctOffsets.size() - 1);
            int distinctIdx = 0;
            for (int i = 0; i <= lastOffset || i <= aggrParams.size(); i++) {
                while (distinctIdx < distinctOffsets.size() && i == distinctOffsets.get(distinctIdx)) {
                    // we loop here since we could be dealing with multiple distinct aggregations after each other
                    Offsets groupOffset = groupings.get(count);
                    count++;
                    QueryProjection.Aggr aggrExpr = aggregationExprs.get(i);
                    List<SQLExpr> arguments = aggrExpr.getFunc().getArguments();
                    SQLExpr sqlExpr = CollectionUtils.isEmpty(arguments) ? null : arguments.get(0);
                    int colId = ctx.getSemTable().collationForExpr(sqlExpr);
                    AbstractAggregateGen4.AggregateParams addAggregate = new AbstractAggregateGen4.AggregateParams();
                    addAggregate.setCol(groupOffset.getCol());
                    addAggregate.setKeyCol(groupOffset.getCol());
                    addAggregate.setWAssigned(groupOffset.getWsCol() >= 0);
                    addAggregate.setWCol(groupOffset.getWsCol());
                    addAggregate.setOpcode(aggrExpr.getOpCode());
                    addAggregate.setAlias(aggrExpr.getAlias());
                    addAggregate.setOriginal(aggrExpr.getOriginal());
                    addAggregate.setCollationId(colId);

                    oa.getAggregates().add(addAggregate);

                    distinctIdx++;
                }
                if (i < aggrParams.size()) {
                    oa.getAggregates().add(aggrParams.get(i));
                }
            }
            // we have to remove the tail of the grouping offsets, so we only have the offsets for the GROUP BY in the query
            groupings = groupings.subList(0, groupings.size() - distinctOffsets.size());
        }
        for (int i = 0; i < groupings.size(); i++) {
            Offsets offsets = groupings.get(i);
            List<GroupByParams> groupByKeys = oa.getGroupByKeys();
            GroupByParams groupByParams = groupByKeys.get(i);
            groupByParams.setKeyCol(offsets.getCol());
            groupByParams.setWeightStringCol(offsets.getWsCol());
        }
    }

    private ImmutableTriple<List<QueryProjection.GroupBy>, List<Integer>, List<QueryProjection.Aggr>> handleDistinctAggr(PlanningContext ctx, List<QueryProjection.Aggr> exprs) throws SQLException {
        SQLExpr distinctExpr = null;
        List<QueryProjection.GroupBy> distincts = new ArrayList<>();
        List<Integer> offsets = new ArrayList<>();
        List<QueryProjection.Aggr> aggrs = new ArrayList<>();

        for (int i = 0; i < exprs.size(); i++) {
            QueryProjection.Aggr expr = exprs.get(i);
            if (!expr.getDistinct()) {
                aggrs.add(expr);
                continue;
            }
            List<SQLExpr> arguments = expr.getFunc().getArguments();
            SQLExpr sqlExpr = CollectionUtils.isEmpty(arguments) ? null : arguments.get(0);
            SQLExpr[] innerAndInnerWS = this.getQp().getSimplifiedExpr(sqlExpr);
            if (exprHasVindex(ctx.getSemTable(), innerAndInnerWS[1], false)) {
                aggrs.add(expr);
                continue;
            }
            if (distinctExpr == null) {
                distinctExpr = innerAndInnerWS[1];
            } else {
                if (!SQLExprUtils.equals(distinctExpr, innerAndInnerWS[1])) {
                    throw new SQLException("unsupported: only one distinct aggregation allowed in a select: " + expr.getOriginal());
                }
            }
            distincts.add(new QueryProjection.GroupBy(innerAndInnerWS[0], innerAndInnerWS[1], expr.getIndex(), null));
            offsets.add(i);
        }
        return new ImmutableTriple<>(distincts, offsets, aggrs);
    }

    private List<AbstractAggregateGen4.AggregateParams> generateAggregateParams(List<QueryProjection.Aggr> aggrs, List<List<Offsets>> aggrParamOffsets, ProjectionGen4Plan proj, boolean pushed) {
        List<AbstractAggregateGen4.AggregateParams> aggrParams = new ArrayList<>(aggrs.size());
        for (int idx = 0; idx < aggrParamOffsets.size(); idx++) {
            List<Offsets> paramOffset = aggrParamOffsets.get(idx);
            QueryProjection.Aggr aggr = aggrs.get(idx);
            Integer incomingOffset = paramOffset.get(0).getCol();
            Integer offset = null;
            if (proj != null) {

            } else {
                offset = incomingOffset;
            }
            Engine.AggregateOpcodeG4 opCode = Engine.AggregateOpcodeG4.AggregateSum;
            switch (aggr.getOpCode()) {
                case AggregateMin:
                case AggregateMax:
                case AggregateRandom:
                    opCode = aggr.getOpCode();
                    break;
                case AggregateCount:
                case AggregateCountDistinct:
                case AggregateSumDistinct:
                case AggregateSum:
//                case engine.AggregateCountStar
                    if (!pushed) {
                        opCode = aggr.getOpCode();
                    }
                    break;
                default:
            }
            AbstractAggregateGen4.AggregateParams aggregateParams = new AbstractAggregateGen4.AggregateParams(opCode, offset, aggr.getAlias(), aggr.getOriginal().getExpr(), aggr.getOriginal());
            aggregateParams.setOrigOpcode(aggr.getOpCode());
            aggrParams.add(idx, aggregateParams);
        }
        return aggrParams;
    }

    /**
     * pushAggregation pushes grouping and aggregation as far down in the tree as possible
     * the output `outputAggrsOffset` needs a little explaining: this is the offsets for aggregation - remember
     * that aggregation can be broken down into multiple expressions that are later combined.
     * this is why this output is a slice of slices
     *
     * @param ctx
     * @param plan
     * @param grouping
     * @param aggregations
     * @param ignoreOutputOrder
     * @return
     * @throws SQLException
     */
    private PushAggregationResult pushAggregation(PlanningContext ctx, LogicalPlan plan, List<QueryProjection.GroupBy> grouping, List<QueryProjection.Aggr> aggregations,
                                                  boolean ignoreOutputOrder) throws SQLException {
        if (plan instanceof RouteGen4Plan) {
            PushAggregationResult pushAggregationResult = new PushAggregationResult();
            pushAggregationResult.setPushed(true);
            pushAggregationResult.setOutput(plan);
            Pair<List<Offsets>, List<List<Offsets>>> aReturn = pushAggrOnRoute(ctx, (RouteGen4Plan) plan, aggregations, grouping, ignoreOutputOrder);
            pushAggregationResult.setGroupingOffsets(aReturn.getLeft());
            pushAggregationResult.setOutputAggrsOffset(aReturn.getRight());
            return pushAggregationResult;
        }
        if (plan instanceof JoinGen4Plan) {
            PushAggregationResult pushAggregationResult = new PushAggregationResult();
            pushAggregationResult.setPushed(true);
            pushAggregationResult.setOutput(plan);
            Pair<List<Offsets>, List<List<Offsets>>> aReturn = pushAggrOnJoin(ctx, (JoinGen4Plan) plan, grouping, aggregations);
            pushAggregationResult.setGroupingOffsets(aReturn.getLeft());
            pushAggregationResult.setOutputAggrsOffset(aReturn.getRight());
            return pushAggregationResult;
        }
        // TODO Semi join

        // TODO Simple Projection

        if (plan instanceof LimitGen4Plan) {
            // if we are seeing a limit, it's because we are building on top of a derived table.
            PushAggregationResult pushAggregationResult = new PushAggregationResult();
            pushAggregationResult.setPushed(true);
            pushAggregationResult.setOutput(plan);

            List<Offsets> groupingOffsets = new ArrayList<>(grouping.size());
            List<List<Offsets>> outputAggrsOffset = new ArrayList<>();

            for (QueryProjection.GroupBy grp : grouping) {
                Pair<Integer, Integer> result = this.wrapAndPushExpr(ctx, grp.getInner(), grp.getWeightStrExpr(), ((LimitGen4Plan) plan).getInput());
                int offset = result.getLeft();
                int wOffset = result.getRight();
                groupingOffsets.add(new Offsets(offset, wOffset));
            }

            for (QueryProjection.Aggr aggr : aggregations) {
                int offset = 0;
                if (aggr.getOriginal().getExpr() instanceof SQLAggregateExpr) {
                    SQLAggregateExpr aggrExpr = (SQLAggregateExpr) aggr.getOriginal().getExpr();
                    Engine.AggregateOpcodeG4 opcode = AbstractAggregateGen4.SUPPORTED_AGGREGATES.get(aggrExpr.getMethodName().toLowerCase());
                    if (opcode == Engine.AggregateOpcodeG4.AggregateCountStar) { // countstar count(*) ??
                        //TODO
                    } else {
                        if (aggrExpr.getArguments().size() != 1) {
                            throw new SQLException("[BUG]: unexpected expression: " + aggrExpr.toString());
                        }

                        Pair<Integer, Integer> ret = ProjectionPushing.pushProjection(ctx, new SQLSelectItem(aggrExpr.getArguments().get(0)), ((LimitGen4Plan) plan).getInput(), true, true, false);
                        offset = ret.getLeft();
                    }
                    Offsets[] offsets = {new Offsets(offset, -1)};
                    outputAggrsOffset.add(Arrays.asList(offsets));
                } else {
                    throw new SQLException("[BUG] unexpected expression:" + aggr.getOriginal().toString());
                }
            }

            pushAggregationResult.setGroupingOffsets(groupingOffsets);
            pushAggregationResult.setOutputAggrsOffset(outputAggrsOffset);
            return pushAggregationResult;
        }
        throw new SQLFeatureNotSupportedException("using aggregation on top of a plan is not yet supported");
    }

    private Pair<List<Offsets>, List<List<Offsets>>> pushAggrOnRoute(PlanningContext ctx, RouteGen4Plan plan, List<QueryProjection.Aggr> aggregations, List<QueryProjection.GroupBy> grouping,
                                                                     boolean ignoreOutputOrder) throws SQLException {
        boolean columnOrderMatters = !ignoreOutputOrder;
        if (!(plan.getSelect() instanceof MySqlSelectQueryBlock)) {
            throw new SQLFeatureNotSupportedException("can't plan aggregation on union");
        }
        MySqlSelectQueryBlock sel = (MySqlSelectQueryBlock) plan.getSelect();

        List<Integer> groupingCols = new ArrayList<>();
        List<List<Offsets>> aggregation = new ArrayList<>();
        AggregationPushing.Func reorg = null;
        if (columnOrderMatters) {
            // During this first run, we push the projections for the normal columns (not the weigh_string ones, that is)
            // in the order that the user asked for it
            // sortOffsets also returns a reorgFunc,
            // that can be used to rearrange the produced outputs to the original order
            Triple<List<QueryProjection.GroupBy>, AggregationPushing.Func, SortedIterator> sortOffsetsResult = sortOffsets(grouping, aggregations);
            reorg = sortOffsetsResult.getMiddle();
            grouping = sortOffsetsResult.getLeft();
            Pair<List<List<Offsets>>, List<Integer>> orderReturn = pushAggrsAndGroupingInOrder(ctx, plan, sortOffsetsResult.getRight(), sel, aggregation, groupingCols);
            aggregation = orderReturn.getLeft();
            groupingCols = orderReturn.getRight();
        } else {
            // if we haven't already pushed the aggregations, now is the time
            for (QueryProjection.Aggr aggr : aggregations) {
                Offsets param = addAggregationToSelect(sel, aggr);
                aggregation.add(Collections.singletonList(param));
            }
        }

        List<Offsets> groupingOffsets = new ArrayList<>(grouping.size());
        for (int idx = 0; idx < grouping.size(); idx++) {
            QueryProjection.GroupBy expr = grouping.get(idx);
            addGroupBy(sel, expr.getInner());
            Offsets pos;
            if (ignoreOutputOrder) {
                // we have not yet pushed anything, so we need to push the expression first
                Integer col = addExpressionToRoute(ctx, plan, new SQLSelectItem(expr.getInner()), true);
                pos = newOffset(col);
            } else {
                pos = newOffset(groupingCols.get(idx));
            }

            if (expr.getWeightStrExpr() != null && ctx.getSemTable().needsWeightString(expr.getInner())) {
                SQLExpr wsExpr = this.weightStringFor(expr.getWeightStrExpr());

                Integer wsCol = addExpressionToRoute(ctx, plan, new SQLSelectItem(wsExpr), true);
                pos.setWsCol(wsCol);
                addGroupBy(sel, wsExpr);
            }
            groupingOffsets.add(pos);
        }
        Pair<List<Offsets>, List<List<Offsets>>> pushAggrsAndGroupingResult = reorg.passThrough(groupingOffsets, aggregation);
        return pushAggrsAndGroupingResult;
    }

    /**
     * We push down aggregations using the logic from the paper Orthogonal Optimization of Subqueries and Aggregation, by
     * Cesar A. Galindo-Legaria and Milind M. Joshi from Microsoft Corp.
     * <p>
     * It explains how one can split an aggregation into local aggregates that depend on only one side of the join.
     * The local aggregates can then be gathered together to produce the global
     * group by/aggregate query that the user asked for.
     * <p>
     * In Vitess, this is particularly useful because it allows us to push aggregation down to the routes, even when
     * we have to join the results at the vtgate level. Instead of doing all the grouping and aggregation at the
     * vtgate level, we can offload most of the work to MySQL, and at the vtgate just summarize the results.
     *
     * @param ctx
     * @param join
     * @param grouping
     * @param aggregations
     * @return
     */
    private Pair<List<Offsets>, List<List<Offsets>>> pushAggrOnJoin(PlanningContext ctx, JoinGen4Plan join, List<QueryProjection.GroupBy> grouping, List<QueryProjection.Aggr> aggregations)
        throws SQLException {
        Pair<List<QueryProjection.Aggr>, List<QueryProjection.Aggr>> splitRes = AggregationPushing.splitAggregationsToLeftAndRight(ctx, aggregations, join);

        List<QueryProjection.Aggr> lhsAggr = splitRes.getLeft();
        List<QueryProjection.Aggr> rhsAggr = splitRes.getRight();


        // We need to group by the columns used in the join condition.
        // If we don't, the LHS will not be able to return the column, and it can't be used to send down to the RHS
        List<QueryProjection.GroupBy> lhsCols = this.createGroupingsForColumns(join.getLHSColumns());

        // Here we split the grouping depending on if they should with the LHS or RHS of the query
        // This is done by using the semantic table and checking dependencies
        Triple<List<QueryProjection.GroupBy>, List<QueryProjection.GroupBy>, List<Integer>> tripRet = AggregationPushing.splitGroupingsToLeftAndRight(ctx, join, grouping, lhsCols);

        List<QueryProjection.GroupBy> lhsGrouping = tripRet.getLeft();
        List<QueryProjection.GroupBy> rhsGrouping = tripRet.getMiddle();
        List<Integer> groupingOffsets = tripRet.getRight();


        // If the rhs has no grouping column then a count(*) will return 0 from the query and will get mapped to the record from left hand side.
        // This is an incorrect behaviour as the join condition has not matched, so we add a literal 1 to the select query and also group by on it.
        // So that only if join condition matches the records will be mapped and returned.
        if (rhsGrouping.size() == 0 && rhsAggr.size() != 0) {
            SQLIntegerExpr l = new SQLIntegerExpr(1);
            SQLSelectItem aExpr = new SQLSelectItem(l);
            Pair<Integer, Integer> ret = ProjectionPushing.pushProjection(ctx, aExpr, join.getRight(), true, true, false);
            int offset = ret.getLeft();
            l.setNumber(offset + 1);
            rhsGrouping.add(new QueryProjection.GroupBy(l));
        }

        // Next we push the aggregations to both sides

        PushAggregationResult leftResult = this.filteredPushAggregation(ctx, join.getLeft(), lhsGrouping, lhsAggr, true);
        PushAggregationResult rightResult = this.filteredPushAggregation(ctx, join.getRight(), rhsGrouping, rhsAggr, true);

        join.setLeft(leftResult.getOutput());
        join.setRight(rightResult.getOutput());

        // Next, we have to pass through the grouping values through the join and the projection we add on top
        // We added new groupings to the LHS because of the join condition, so we don't want to pass through everything,
        // just the groupings that are used by operators on top of this current one

        int wsOutputGrpOffset = groupingOffsets.size() + join.getCols().size();
        List<Offsets> outputGroupings = new ArrayList<>(groupingOffsets.size());

        List<Integer> wsOffsets = new ArrayList<>();

        for (int groupBy : groupingOffsets) {
            Offsets offset = null;
            int fac = 1;
            if (groupBy < 0) {
                offset = leftResult.getGroupingOffsets().get(-groupBy - 1);
                fac = -1;
            } else {
                offset = rightResult.getGroupingOffsets().get(groupBy - 1);
            }
            Offsets outputGrouping = newOffset(join.getCols().size());
            join.getCols().add(offset.getCol() * fac);

            if (offset.getWsCol() > -1) {
                // we add the weight_string calls at the end of the join columns
                outputGrouping.setWsCol(wsOutputGrpOffset + wsOffsets.size());
                wsOffsets.add(offset.getWsCol() * fac);
            }
            outputGroupings.add(outputGrouping);
        }
        join.getCols().addAll(wsOffsets);


        List<List<Offsets>> outputAggrOffsets = new ArrayList<>(aggregations.size());
        for (int idx = 0; idx < aggregations.size(); idx++) {
            List<Offsets> l = leftResult.getOutputAggrsOffset().get(idx);
            List<Offsets> r = rightResult.getOutputAggrsOffset().get(idx);

            List<Offsets> offSlice = new ArrayList<>();

            for (Offsets off : l) {
                offSlice.add(newOffset(join.getCols().size()));
                join.getCols().add(-(off.getCol() + 1));
            }

            for (Offsets off : r) {
                offSlice.add(newOffset(join.getCols().size()));
                join.getCols().add(off.getCol() + 1);
            }

            outputAggrOffsets.add(offSlice);
        }
        return Pair.of(outputGroupings, outputAggrOffsets);
    }

    /**
     * this method takes a slice of aggregations that can have missing spots in the form of `nil`,
     * and pushes the non-empty values down.
     * during aggregation planning, it's important to know which of
     * the incoming aggregations correspond to what is sent to the LHS and RHS.
     * Some aggregations only need to be sent to one of the sides of the join, and in that case,
     * the other side will have a nil in this offset of the aggregations
     *
     * @param ctx
     * @param plan
     * @param grouping
     * @param aggregations
     * @param ignoreOutputOrder
     * @return
     */
    private PushAggregationResult filteredPushAggregation(
        PlanningContext ctx, LogicalPlan plan, List<QueryProjection.GroupBy> grouping, List<QueryProjection.Aggr> aggregations, Boolean ignoreOutputOrder
    ) throws SQLException {
        List<Boolean> used = new ArrayList<>(aggregations.size());

        List<QueryProjection.Aggr> aggrs = new ArrayList<>(aggregations.size());

        for (int idx = 0; idx < aggregations.size(); idx++) {
            if (aggregations.get(idx) != null) {
                used.add(true);
                aggrs.add(aggregations.get(idx));
            } else {
                used.add(false);
            }
        }

        PushAggregationResult result = this.pushAggregation(ctx, plan, grouping, aggrs, ignoreOutputOrder);

        PushAggregationResult ret = new PushAggregationResult();
        ret.setOutput(result.getOutput());
        ret.setPushed(result.isPushed());
        ret.setGroupingOffsets(result.getGroupingOffsets());
        ret.setOutputAggrsOffset(new ArrayList<>());

        int idx = 0;
        for (Boolean b : used) {
            if (!b) {
                ret.getOutputAggrsOffset().add(null);
                continue;
            }
            ret.getOutputAggrsOffset().add(result.getOutputAggrsOffset().get(idx));
            idx++;
        }
        return ret;
    }

    private void addGroupBy(MySqlSelectQueryBlock sel, SQLExpr inner) {
        if (sel.getGroupBy() == null) {
            SQLSelectGroupByClause newGroupBy = new SQLSelectGroupByClause();
            sel.setGroupBy(newGroupBy);
        }
        for (SQLExpr item : sel.getGroupBy().getItems()) {
            if (Objects.equals(item, inner)) {
                return;
            }
        }
        sel.getGroupBy().addItem(inner);
    }

    private Pair<List<List<Offsets>>, List<Integer>> pushAggrsAndGroupingInOrder(PlanningContext ctx, RouteGen4Plan plan, SortedIterator sortedIterator, MySqlSelectQueryBlock sel,
                                                                                 List<List<Offsets>> vtgateAggregation,
                                                                                 List<Integer> groupingCols) throws SQLException {
        while (sortedIterator.hasNext()) {
            SortedIterator current = sortedIterator.next();
            QueryProjection.Aggr aggregation = current.getValueA();
            QueryProjection.GroupBy groupBy = current.getValueGB();
            if (aggregation != null) {
                Offsets param = addAggregationToSelect(sel, aggregation);
                vtgateAggregation.add(Collections.singletonList(param));
                continue;
            }
            if (groupBy != null) {
                boolean reuseCol = groupBy.getInnerIndex() == null;
                Integer col = addExpressionToRoute(ctx, plan, groupBy.asAliasedExpr(), reuseCol);
                groupingCols.add(col);
            }
        }
        return new ImmutablePair<>(vtgateAggregation, groupingCols);
    }

    private Integer addExpressionToRoute(PlanningContext ctx, RouteGen4Plan rb, SQLSelectItem asAliasedExpr, boolean reuseCol) throws SQLException {
        if (reuseCol) {
            int offset = checkIfAlreadyExists(asAliasedExpr, rb.getSelect(), ctx.getSemTable());
            if (offset != -1) {
                return offset;
            }
        }
//        expr.Expr = sqlparser.RemoveKeyspaceFromColName(expr.Expr)
//        PlanBuilder.removeKeyspaceFromColName(asAliasedExpr.getExpr());
        if (!(rb.getSelect() instanceof MySqlSelectQueryBlock)) {
            throw new SQLFeatureNotSupportedException("unsupported: pushing projection");
        }
        if (ctx.isRewriteDerivedExpr()) {
            // if we are trying to push a projection that belongs to a DerivedTable
            // we rewrite that expression, so it matches the column name used inside
            // that derived table.
        }
        MySqlSelectQueryBlock sel = (MySqlSelectQueryBlock) rb.getSelect();
        Integer offset = sel.getSelectList().size();
        sel.getSelectList().add(asAliasedExpr);
        return offset;
    }

    /**
     * addAggregationToSelect adds the aggregation to the SELECT statement and returns the AggregateParams to be used outside
     *
     * @param sel
     * @param aggregation
     * @return
     */
    private Offsets addAggregationToSelect(MySqlSelectQueryBlock sel, QueryProjection.Aggr aggregation) {
        // TODO: removing duplicated aggregation expression should also be done at the join level
        for (int i = 0; i < sel.getSelectList().size(); i++) {
            SQLSelectItem selectItem = sel.getSelectList().get(i);
            SqlParser.SelectExpr selectExpr = SqlParser.SelectExpr.type(selectItem);
            if (!Objects.equals(SqlParser.SelectExpr.AliasedExpr, selectExpr)) {
                continue;
            }

            if (Objects.equals(selectItem.getExpr(), aggregation.getOriginal().getExpr())) {
                return newOffset(i);
            }
        }
        sel.addSelectItem(aggregation.getOriginal());
        return newOffset(sel.getSelectList().size() - 1);
    }

    private Offsets newOffset(int col) {
        return new Offsets(col, -1);
    }

    private List<QueryProjection.GroupBy> createGroupingsForColumns(List<SQLName> columns) {
        List<QueryProjection.GroupBy> lhsGrouping = new ArrayList<>();
        for (SQLName lhsColumn : columns) {
            SQLExpr[] ret = this.qp.getSimplifiedExpr(lhsColumn);
            lhsGrouping.add(new QueryProjection.GroupBy(
                ret[0],
                ret[1],
                0,
                null
            ));
        }
        return lhsGrouping;
    }

    private Triple<List<QueryProjection.GroupBy>, AggregationPushing.Func, SortedIterator> sortOffsets(List<QueryProjection.GroupBy> grouping, List<QueryProjection.Aggr> aggregations) {
        List<QueryProjection.GroupBy> originalGrouping = new ArrayList<>(grouping);
        List<QueryProjection.Aggr> originalAggr = new ArrayList<>(aggregations);
        qp.sortAggregations(aggregations);
        qp.sortGrouping(grouping);

        AggregationPushing.Func reorg = (groupByOffsets, aggrOffsets) -> {
            List<Offsets> orderedGroupingOffsets = new ArrayList<>(originalGrouping.size());
            for (QueryProjection.GroupBy og : originalGrouping) {
                for (int i = 0; i < grouping.size(); i++) {
                    QueryProjection.GroupBy g = grouping.get(i);
                    if (og.getInner() == g.getInner()) {
                        orderedGroupingOffsets.add(groupByOffsets.get(i));
                    }
                }
            }
            List<List<Offsets>> orderedAggrs = new ArrayList<>(originalGrouping.size());
            for (QueryProjection.Aggr og : originalAggr) {
                for (int i = 0; i < aggregations.size(); i++) {
                    QueryProjection.Aggr g = aggregations.get(i);
                    if (og.getOriginal() == g.getOriginal()) {
                        orderedAggrs.add(aggrOffsets.get(i));
                    }
                }
            }
            return new ImmutablePair(orderedGroupingOffsets, orderedAggrs);
        };

        SortedIterator sortedIterator = new SortedIterator(grouping, aggregations);
        return new ImmutableTriple<>(grouping, reorg, sortedIterator);
    }

    private void planAggregationWithoutOA(PlanningContext ctx, LogicalPlan plan) throws SQLException {
        for (QueryProjection.SelectExpr expr : this.qp.getSelectExprs()) {
            SQLSelectItem selectItem = expr.getAliasedExpr();
            ProjectionPushing.pushProjection(ctx, selectItem, plan, true, false, false);
        }
        for (QueryProjection.GroupBy expr : this.qp.getGrouping()) {
            // since all the grouping will be done at the mysql level,
            // we know that we won't need any weight_string() calls
            this.planGroupByGen4(ctx, expr, plan /*weighString*/, false);
        }
    }

    private void pushProjections(PlanningContext ctx, LogicalPlan plan, List<QueryProjection.SelectExpr> selectExprs) throws SQLException {
        for (QueryProjection.SelectExpr e : selectExprs) {
            ProjectionPushing.pushProjection(ctx, e.getAliasedExpr(), plan, true, false, false);
        }
    }

    public LogicalPlan planOrderBy(PlanningContext ctx, List<QueryProjection.OrderBy> orderExprs, LogicalPlan plan) throws SQLException {
        if (plan instanceof RouteGen4Plan) {
            LogicalPlan newPlan = this.planOrderByForRoute(ctx, orderExprs, (RouteGen4Plan) plan, this.qp.hasStar);
            return newPlan;
        } else if (plan instanceof OrderedAggregateGen4Plan) {
            // remove ORDER BY NULL from the list of order by expressions since we will be doing the ordering on vtgate level so NULL is not useful
            List<QueryProjection.OrderBy> orderExprsWithoutNils = new ArrayList<>(orderExprs.size());
            for (QueryProjection.OrderBy expr : orderExprs) {
                if (expr.inner.getExpr() == null) {
                    continue;
                }
                orderExprsWithoutNils.add(expr);
            }
            orderExprs = orderExprsWithoutNils;
            CheckNodeTypesVisitor visitor = new CheckNodeTypesVisitor(Sets.newHashSet(CheckNodeTypesVisitor.CheckNodeType.AGGREGATE));
            for (QueryProjection.OrderBy expr : orderExprs) {
                expr.weightStrExpr.accept(visitor);
                boolean hasAggr = visitor.getCheckResult();
                if (hasAggr) {
                    return createMemorySortPlanOnAggregation(ctx, (OrderedAggregateGen4Plan) plan, orderExprs);
                }
            }
            LogicalPlan newInput = planOrderBy(ctx, orderExprs, ((OrderedAggregateGen4Plan) plan).getInput());
            ((OrderedAggregateGen4Plan) plan).setInput(newInput);
            return plan;

        } else if (plan instanceof MemorySortPlan) {
            return plan;
        } else if (plan instanceof JoinGen4Plan) {
            return this.planOrderByForJoin(ctx, orderExprs, (JoinGen4Plan) plan);
        } else {
            throw new SQLException("ordering on complex query " + plan.toString());
        }
    }

    private LogicalPlan planHaving(PlanningContext ctx, LogicalPlan plan) throws SQLException {
        if (this.sel instanceof MySqlSelectQueryBlock) {
            MySqlSelectQueryBlock mySqlSelectQueryBlock = (MySqlSelectQueryBlock) this.sel;
            if (mySqlSelectQueryBlock.getGroupBy() != null && mySqlSelectQueryBlock.getGroupBy().getHaving() != null) {
                return pushHaving(ctx, mySqlSelectQueryBlock.getGroupBy().getHaving(), plan);
            }
        }
        return plan;
    }

    private LogicalPlan pushHaving(PlanningContext ctx, SQLExpr expr, LogicalPlan plan) throws SQLException {
        if (plan instanceof RouteGen4Plan) {
            SQLSelectQuery selectQuery = ((RouteGen4Plan) plan).getSelect();
            MySqlSelectQueryBlock planSelect = PlanBuilder.getFirstSelect(selectQuery);
            planSelect.getGroupBy().addHaving(expr);
            return plan;
        } else if (plan instanceof OrderedAggregateGen4Plan) {
            return new FilterGen4Plan(ctx, plan, expr);
        } else {
            throw new SQLException("[BUG] unreachable filtering: " + plan.toString());
        }
    }

    private void rewriterHaving() {
        if (!this.hasHaving) {
            return;
        }
        if (this.sel instanceof MySqlSelectQueryBlock) {
            SQLExpr havingExpr = ((MySqlSelectQueryBlock) this.sel).getGroupBy().getHaving();
            RewriteHavingAggrWithOffsetVisitor visitor = new RewriteHavingAggrWithOffsetVisitor(this.getQp());
            havingExpr.accept(visitor);
        }
    }

    private LogicalPlan planOrderByForRoute(PlanningContext ctx, List<QueryProjection.OrderBy> orderExprs, RouteGen4Plan plan, boolean hasStar) throws SQLException {
        for (QueryProjection.OrderBy order : orderExprs) {
            boolean isSpecialOrderBy = isSpecialOrderBy(order.inner.getExpr());
            if (order.inner.getType() == null && !isSpecialOrderBy) {
                order.inner.setType(SQLOrderingSpecification.ASC);
            }

            // AddOrder
            if (plan.select instanceof MySqlSelectQueryBlock) {
                MySqlSelectQueryBlock selectQueryBlock = (MySqlSelectQueryBlock) plan.select;
                if (selectQueryBlock.getOrderBy() == null) {
                    selectQueryBlock.setOrderBy(new SQLOrderBy());
                }
                selectQueryBlock.getOrderBy().addItem(order.inner);
            } else if (plan.select instanceof SQLUnionQuery) {
                SQLUnionQuery selectUnionQuery = (SQLUnionQuery) plan.select;
                if (selectUnionQuery.getOrderBy() == null) {
                    SQLOrderBy sqlOrderBy = new SQLOrderBy();
                    selectUnionQuery.setOrderBy(sqlOrderBy);
                }
                selectUnionQuery.getOrderBy().addItem(order.inner);
            }

            if (isSpecialOrderBy) {
                continue;
            }
            SQLExpr wsExpr = null;
            if (ctx.getSemTable().needsWeightString(order.inner.getExpr())) {
                wsExpr = order.weightStrExpr;
            }
            Pair<Integer, Integer> wsResult = this.wrapAndPushExpr(ctx, order.inner.getExpr(), wsExpr, plan);
            plan.eroute.getOrderBy().add(new OrderByParamsGen4(wsResult.getLeft(), order.inner.getType() == SQLOrderingSpecification.DESC, wsResult.getRight(), null));
        }
        return plan;
    }

    private static boolean isSpecialOrderBy(SQLExpr expr) {
        if (expr instanceof SQLNullExpr) {
            return true;
        }
        if (expr instanceof SQLMethodInvokeExpr) {
            String methodName = ((SQLMethodInvokeExpr) expr).getMethodName();
            return "rand".equalsIgnoreCase(methodName);
        }
        return false;
    }

    /**
     * wrapAndPushExpr pushes the expression and weighted_string function to the plan using semantics.SemTable
     * It returns (expr offset, weight_string offset, error)
     *
     * @param ctx
     * @param expr
     * @param weightStrExpr
     * @param plan
     * @return
     */
    private Pair<Integer, Integer> wrapAndPushExpr(PlanningContext ctx, SQLExpr expr, SQLExpr weightStrExpr, LogicalPlan plan) throws SQLException {
        Pair<Integer, Integer> result = ProjectionPushing.pushProjection(ctx, new SQLSelectItem(expr), plan, true, true, false);
        if (weightStrExpr == null) {
            //result.setValue(-1);
            return Pair.of(result.getLeft(), -1);
        }

        if (!(expr instanceof SQLName)) {
            if (expr instanceof SQLMethodInvokeExpr) {
                String methodName = ((SQLMethodInvokeExpr) expr).getMethodName();
                if ("cast".equalsIgnoreCase(methodName) || "convert".equalsIgnoreCase(methodName)) {
                    expr = ((SQLMethodInvokeExpr) expr).getParameters().get(0);
                }
            }
            if (!(expr instanceof SQLName)) {
                throw new SQLException("unsupported: in scatter query: complex order by expression: " + expr.toString());
            }
        }

        Type qt = ctx.getSemTable().typeFor(expr);
        boolean wsNeeded = true;
        if (qt != null && VtType.isNumber(qt.getType())) {
            wsNeeded = false;
        }
        // int weightStringOffset = -1;
        if (wsNeeded) {
            SQLExpr aliasedExpr = this.weightStringFor(weightStrExpr);
            Pair<Integer, Integer> wsret = ProjectionPushing.pushProjection(ctx, new SQLSelectItem(aliasedExpr), plan, true, true, false);
            //result.setValue(wsret.getLeft());
            return Pair.of(result.getLeft(), wsret.getLeft());
        }
        return result;
    }

    private LogicalPlan planOrderByForJoin(PlanningContext ctx, List<QueryProjection.OrderBy> orderExprs, JoinGen4Plan plan) throws SQLException {
        if (orderExprs.size() == 1 && isSpecialOrderBy(orderExprs.get(0).inner.getExpr())) {
            LogicalPlan lhs = this.planOrderBy(ctx, orderExprs, plan.getLeft());
            LogicalPlan rhs = this.planOrderBy(ctx, orderExprs, plan.getRight());

            plan.setLeft(lhs);
            plan.setRight(rhs);
            return plan;
        }
        // We can only push down sorting on the LHS of the join.
        // If the order is on the RHS, we need to do the sorting on the vtgate
        if (orderExprsDependsOnTableSet(orderExprs, ctx.getSemTable(), plan.getLeft().containsTables())) {
            LogicalPlan newLeft = this.planOrderBy(ctx, orderExprs, plan.getLeft());
            plan.setLeft(newLeft);
            return plan;
        }
        LogicalPlan sortPlan = this.createMemorySortPlan(ctx, plan, orderExprs, true);
        return sortPlan;
    }

    private static Boolean orderExprsDependsOnTableSet(List<QueryProjection.OrderBy> orderExprs, SemTable semTable, TableSet ts) {
        for (QueryProjection.OrderBy expr : orderExprs) {
            TableSet exprDependencies = semTable.recursiveDeps(expr.inner.getExpr());
            if (!exprDependencies.isSolvedBy(ts)) {
                return false;
            }
        }
        return true;
    }

    private LogicalPlan planDistinct(PlanningContext ctx, LogicalPlan plan) throws SQLException {
        if (!this.qp.needsDistinct()) {
            return plan;
        }
        if (plan instanceof RouteGen4Plan) {
            // we always make the underlying query distinct,
            // and then we might also add a distinct operator on top if it is needed
            SQLSelectQuery sel = ((RouteGen4Plan) plan).select;
            ((MySqlSelectQueryBlock) sel).setDistionOption(SQLSetQuantifier.DISTINCT);

            if (((RouteGen4Plan) plan).isSingleShard() || this.selectHasUniqueVindex(ctx.getSemTable(), this.qp.getSelectExprs())) {
                return plan;
            }

            return this.addDistinct(ctx, plan);
        } else if (plan instanceof OrderedAggregateGen4Plan) {
            return this.planDistinctOA(ctx.getSemTable(), (OrderedAggregateGen4Plan) plan);
        } else if (plan instanceof DistinctGen4Plan) {
            return plan;
        } else {
            throw new SQLException("unknown plan type for DISTINCT ", plan.toString());
        }
    }

    private LogicalPlan planDistinctOA(SemTable semTable, OrderedAggregateGen4Plan currPlan) throws SQLException {
        OrderedAggregateGen4Plan oa = new OrderedAggregateGen4Plan();
        oa.setInput(currPlan);
        for (QueryProjection.SelectExpr sExpr : this.qp.selectExprs) {
            SQLExpr expr = sExpr.getExpr();
            boolean found = false;
            for (GroupByParams grpParam : currPlan.getGroupByKeys()) {
                if (SQLExprUtils.equals(expr, grpParam.getExpr())) {
                    found = true;
                    oa.getGroupByKeys().add(grpParam);
                }
            }
            if (found) {
                continue;
            }
            for (AbstractAggregateGen4.AggregateParams aggrParam : currPlan.getAggregates()) {
                if (SQLExprUtils.equals(expr, aggrParam.getExpr())) {
                    found = true;
                    oa.getGroupByKeys().add(new GroupByParams(aggrParam.getCol(), -1));
                    break;
                }
            }
            if (!found) {
                throw new SQLException("[BUG] unable to plan distinct query as the column is not projected: %s" + sExpr.getCol());
            }
        }
        return oa;
    }

    private LogicalPlan createMemorySortPlan(PlanningContext ctx, LogicalPlan plan, List<QueryProjection.OrderBy> orderExprs, boolean useWeightStr) throws SQLException {
        MemorySortGen4Engine primitive = new MemorySortGen4Engine();
        MemorySortGen4Plan memorySortPlan = new MemorySortGen4Plan();
        memorySortPlan.setInput(plan);
        memorySortPlan.setTruncater(primitive);
        memorySortPlan.setEMemorySort(primitive);
        for (QueryProjection.OrderBy orderBy : orderExprs) {
            SQLExpr wsExpr = orderBy.weightStrExpr;
            if (!useWeightStr) {
                wsExpr = null;
            }
            Pair<Integer, Integer> offset = wrapAndPushExpr(ctx, orderBy.inner.getExpr(), wsExpr, plan);

            boolean isDesc = orderBy.inner.getType() == SQLOrderingSpecification.DESC;
            memorySortPlan.getEMemorySort().getOrderByParams().add(new OrderByParamsGen4(offset.getLeft(), isDesc, offset.getRight(), offset.getLeft(), null));
        }
        return memorySortPlan;
    }

    private LogicalPlan createMemorySortPlanOnAggregation(PlanningContext ctx, OrderedAggregateGen4Plan plan, List<QueryProjection.OrderBy> orderExprs) throws SQLException {
        MemorySortGen4Engine primitive = new MemorySortGen4Engine();
        MemorySortGen4Plan memorySortPlan = new MemorySortGen4Plan();
        memorySortPlan.setInput(plan);
        memorySortPlan.setTruncater(primitive);
        memorySortPlan.setEMemorySort(primitive);

        for (QueryProjection.OrderBy orderBy : orderExprs) {
            MutableTriple<Integer, Integer, Boolean> offset = findExprInOrderedAggr(plan, orderBy);
            if (!offset.getRight()) {
                throw new SQLException("expected to find the order by expression in orderedAggregate. Expression: " + orderBy.toString());
            }
            boolean isDesc = orderBy.inner.getType() == SQLOrderingSpecification.DESC;
            memorySortPlan.getEMemorySort().getOrderByParams().add(new OrderByParamsGen4(offset.getLeft(), isDesc, offset.getMiddle(), offset.getLeft(), null));
        }
        return memorySortPlan;
    }

    private MutableTriple<Integer, Integer, Boolean> findExprInOrderedAggr(OrderedAggregateGen4Plan plan, QueryProjection.OrderBy orderBy) {
        int keyCol = 0;
        int weightStringCol = 0;
        boolean found = false;

        for (GroupByParams key : plan.getGroupByKeys()) {
            if (SQLExprUtils.equals(orderBy.getWeightStrExpr(), key.getExpr()) || SQLExprUtils.equals(orderBy.getInner().getExpr(), key.getExpr())) {
                keyCol = key.getKeyCol();
                weightStringCol = key.getWeightStringCol();
                found = true;
                return new MutableTriple<>(keyCol, weightStringCol, found);
            }
        }

        for (AbstractAggregateGen4.AggregateParams aggregate : plan.getAggregates()) {
            if (SQLExprUtils.equals(orderBy.getWeightStrExpr(), aggregate.getOriginal().getExpr()) || SQLExprUtils.equals(orderBy.getInner().getExpr(), aggregate.getOriginal().getExpr())) {
                keyCol = aggregate.getCol();
                weightStringCol = -1;
                found = true;
                return new MutableTriple<>(keyCol, weightStringCol, found);
            }
        }
        return new MutableTriple<>(keyCol, weightStringCol, found);
    }

    private void planGroupByGen4(PlanningContext ctx, QueryProjection.GroupBy groupExpr, LogicalPlan plan, boolean wsAdded) throws SQLException {
        if (plan instanceof RouteGen4Plan) {
            SQLSelectGroupByClause groupByClause = ((MySqlSelectQueryBlock) ((RouteGen4Plan) plan).select).getGroupBy();
            if (groupByClause == null) {
                groupByClause = new SQLSelectGroupByClause();
                ((MySqlSelectQueryBlock) ((RouteGen4Plan) plan).select).setGroupBy(groupByClause);
            }
            groupByClause.addItem(groupExpr.inner);
            // If a weight_string function is added to the select list,
            // then we need to add that to the group by clause otherwise the query will fail on mysql with full_group_by error
            // as the weight_string function might not be functionally dependent on the group by.
            if (wsAdded) {
                groupByClause.addItem(weightStringFor(groupExpr.weightStrExpr));
            }
        } else {
            throw new SQLException("unsupported: group by on: " + plan.toString());
        }
    }

    private boolean selectHasUniqueVindex(SemTable semTable, List<QueryProjection.SelectExpr> selectExprs) throws SQLException {
        for (QueryProjection.SelectExpr expr : selectExprs) {
            SQLSelectItem exp = expr.getAliasedExpr();
            if (exp == null) {
                // TODO: handle star expression error
                return false;
            }
            if (this.exprHasUniqueVindex(semTable, exp.getExpr())) {
                return true;
            }
        }
        return false;
    }

    private LogicalPlan addDistinct(PlanningContext ctx, LogicalPlan plan) throws SQLException {
        ArrayList<QueryProjection.OrderBy> orderExprs = new ArrayList<>();
        ArrayList<GroupByParams> groupByKeys = new ArrayList<>();
        for (int index = 0; index < this.getQp().getSelectExprs().size(); index++) {
            QueryProjection.SelectExpr sExpr = this.getQp().getSelectExprs().get(index);
            if (isAmbiguousOrderBy(index, sExpr.getAliasedExpr().getExpr(), this.getQp().getSelectExprs())) {
                throw new SQLException("generating order by clause: ambiguous symbol reference: " + sExpr.getAliasedExpr().toString());
            }
            SQLExpr inner;
            if (sExpr.col.getAlias() == null || sExpr.col.getAlias().isEmpty()) {
                inner = sExpr.getAliasedExpr().getExpr();
            } else {
                // If we have an alias, we need to use the alias and not the original expression
                // to make sure dependencies work correctly,
                // we simply copy the dependencies of the original expression here
                inner = new SQLIdentifierExpr(sExpr.col.getAlias());
                ctx.getSemTable().copyDependencies(sExpr.col.getExpr(), inner);
            }

            GroupByParams groupByParams = new GroupByParams();
            groupByParams.setKeyCol(index);
            groupByParams.setCollationID(ctx.getSemTable().collationForExpr(inner));
            groupByParams.setExpr(inner);
            Pair<Integer, Integer> offset = wrapAndPushExpr(ctx, sExpr.getAliasedExpr().getExpr(), sExpr.getAliasedExpr().getExpr(), plan);
            int weightOffset = offset.getRight();
            groupByParams.setWeightStringCol(weightOffset);

            groupByKeys.add(groupByParams);

            QueryProjection.OrderBy orderByExpr = new QueryProjection.OrderBy(new SQLSelectOrderByItem(inner), sExpr.getAliasedExpr().getExpr());
            orderExprs.add(orderByExpr);

        }
        LogicalPlan innerPlan = planOrderBy(ctx, orderExprs, plan);
        OrderedAggregateGen4Plan orderedAggregateGen4Plan = new OrderedAggregateGen4Plan();
        orderedAggregateGen4Plan.setInput(innerPlan);
        orderedAggregateGen4Plan.setGroupByKeys(groupByKeys);

        return orderedAggregateGen4Plan;
    }

    private LogicalPlan truncateColumnsIfNeeded(PlanningContext ctx, LogicalPlan plan) throws SQLException {
        if (plan.outputColumns().size() == this.qp.getColumnCount()) {
            return plan;
        }
        if (isJoin(plan)) {
            // since this is a join, we can safely add extra columns and not need to truncate them
            return plan;
        }
        if (plan instanceof RouteGen4Plan) {
            ((RouteGen4Plan) plan).eroute.setTruncateColumnCount(this.qp.getColumnCount());
        } else if (plan instanceof OrderedAggregateGen4Plan) {
            ((OrderedAggregateGen4Plan) plan).setTruncateColumnCount(this.qp.getColumnCount());
        } else if (plan instanceof MemorySortGen4Plan) {
            ((MemorySortGen4Plan) plan).getTruncater().setTruncateColumnCount(this.getQp().getColumnCount());
        } else {
            plan = new SimpleProjectionGen4Plan(plan);
            List<QueryProjection.SelectExpr> exprs = this.getQp().getSelectExprs().subList(0, this.getQp().getColumnCount());
            pushProjections(ctx, plan, exprs);
        }
        return plan;
    }

    public static int checkIfAlreadyExists(SQLSelectItem expr, SQLSelectQuery node, SemTable semTable) {
        TableSet exprDep = semTable.recursiveDeps(expr.getExpr());
        // Here to find if the expr already exists in the SelectStatement, we have 3 cases
        // input is a Select -> In this case we want to search in the select
        // input is a Union -> In this case we want to search in the First Select of the Union
        // input is a Parenthesised Select -> In this case we want to search in the select
        // all these three cases are handled by the call to GetFirstSelect.
        boolean isExprCol = false;
        SQLName exprCol = null;
        if (expr.getExpr() instanceof SQLName) {
            isExprCol = true;
            exprCol = (SQLName) expr.getExpr();
        }

        if (node instanceof MySqlSelectQueryBlock) {
            MySqlSelectQueryBlock selectQueryBlock = (MySqlSelectQueryBlock) node;

            // first pass - search for aliased expressions
            for (int i = 0; i < selectQueryBlock.getSelectList().size(); i++) {
                if (!isExprCol) {
                    break;
                }

                SQLSelectItem selectItem = selectQueryBlock.getSelectList().get(i);
                SqlParser.SelectExpr selectExpr = SqlParser.SelectExpr.type(selectItem);
                if (SqlParser.SelectExpr.AliasedExpr.equals(selectExpr)) {
                    if (StringUtils.isNotEmpty(selectItem.getAlias()) && selectItem.getAlias().equals(((SQLName) expr.getExpr()).getSimpleName())) {
                        return i;
                    }
                }
            }

            // next pass - we are searching the actual expressions and not the aliases
            for (int i = 0; i < selectQueryBlock.getSelectList().size(); i++) {
                SQLSelectItem selectItem = selectQueryBlock.getSelectList().get(i);
                SqlParser.SelectExpr selectExpr = SqlParser.SelectExpr.type(selectItem);
                if (!SqlParser.SelectExpr.AliasedExpr.equals(selectExpr)) {
                    continue;
                }

                boolean isSelectExprCol = false;
                SQLName selectExprCol = null;
                if (selectItem.getExpr() instanceof SQLName) {
                    isSelectExprCol = true;
                    selectExprCol = (SQLName) selectItem.getExpr();
                }

                TableSet selectExprDep = semTable.recursiveDeps(selectItem.getExpr());

                // Check that the two expressions have the same dependencies
                if (!selectExprDep.equals(exprDep)) {
                    continue;
                }

                if (isSelectExprCol && isExprCol && exprCol.getSimpleName().equals(selectExprCol.getSimpleName())) {
                    // the expressions are ColName, we compare their name
                    return i;
                }

                if (SQLExprUtils.equals(selectItem.getExpr(), expr.getExpr())) {
                    // the expressions are not ColName, so we just compare the expressions
                    return i;
                }
            }
        }
        return -1;
    }

    public static boolean isAmbiguousOrderBy(int index, SQLExpr col, List<QueryProjection.SelectExpr> exprs) {
        return false;
    }

    public static SQLExpr weightStringFor(SQLExpr params) {
        SQLExpr expr = new SQLMethodInvokeExpr("weight_string", null, params);
        return expr;
    }

    @NoArgsConstructor
    @Getter
    @Setter
    static class PushAggregationResult {
        private LogicalPlan output;

        private List<Offsets> groupingOffsets;

        private List<List<Offsets>> outputAggrsOffset;

        private boolean pushed;
    }
}
