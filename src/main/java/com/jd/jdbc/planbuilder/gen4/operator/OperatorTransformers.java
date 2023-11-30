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

package com.jd.jdbc.planbuilder.gen4.operator;

import com.jd.jdbc.context.PlanningContext;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.gen4.CheckCol;
import com.jd.jdbc.engine.gen4.RouteGen4Engine;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.planbuilder.gen4.Gen4Planner;
import com.jd.jdbc.planbuilder.gen4.HorizonPlanning;
import com.jd.jdbc.planbuilder.gen4.logical.ConcatenateGen4Plan;
import com.jd.jdbc.planbuilder.gen4.logical.DistinctGen4Plan;
import com.jd.jdbc.planbuilder.gen4.logical.FilterGen4Plan;
import com.jd.jdbc.planbuilder.gen4.logical.JoinGen4Plan;
import com.jd.jdbc.planbuilder.gen4.logical.LogicalPlan;
import com.jd.jdbc.planbuilder.gen4.logical.RouteGen4Plan;
import com.jd.jdbc.planbuilder.gen4.operator.physical.ApplyJoin;
import com.jd.jdbc.planbuilder.gen4.operator.physical.Filter;
import com.jd.jdbc.planbuilder.gen4.operator.physical.PhysicalOperator;
import com.jd.jdbc.planbuilder.gen4.operator.physical.Route;
import com.jd.jdbc.planbuilder.gen4.operator.physical.RoutePlanning;
import com.jd.jdbc.planbuilder.gen4.operator.physical.Table;
import com.jd.jdbc.planbuilder.gen4.operator.physical.Union;
import com.jd.jdbc.planbuilder.semantics.Scoper;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLSetQuantifier;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator;
import com.jd.jdbc.sqlparser.ast.expr.SQLInListExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLInSubQueryExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefListExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionOperator;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQuery;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.vindexes.SingleColumn;
import com.jd.jdbc.vindexes.VKeyspace;
import com.jd.jdbc.vindexes.Vindex;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class OperatorTransformers {
    public static LogicalPlan transformToLogicalPlan(PlanningContext ctx, PhysicalOperator op, boolean isRoot) throws SQLException {
        if (op instanceof Route) {
            return transformRoutePlan(ctx, (Route) op);
        }
        if (op instanceof ApplyJoin) {
            return transformApplyJoinPlan(ctx, (ApplyJoin) op);
        }
        if (op instanceof Union) {
            return transformUnionPlan(ctx, (Union) op, isRoot);
        }
        if (op instanceof Filter) {
            LogicalPlan plan = transformToLogicalPlan(ctx, ((Filter) op).getSource(), false);
            SQLExpr ast = SQLBinaryOpExpr.combine(((Filter) op).getPredicates(), SQLBinaryOperator.BooleanAnd);
            return new FilterGen4Plan(ctx, plan, ast, true);
        }
        return null;
    }

    private static LogicalPlan transformUnionPlan(PlanningContext ctx, Union op, boolean isRoot) throws SQLException {
        List<LogicalPlan> sources;
        if (op.isDistinct()) {
            sources = transformAndMerge(ctx, op);
            for (LogicalPlan source : sources) {
                pushDistinct(source);
            }
        } else {
            sources = transformAndMergeInOrder(ctx, op);
        }

        LogicalPlan result;
        if (sources.size() == 1) {
            LogicalPlan src = sources.get(0);
            if (src instanceof RouteGen4Plan && ((RouteGen4Plan) src).isSingleShard()) {
                // if we have a single shard route, we don't need to do anything to make it distinct
                // TODO
                // rb.Select.SetLimit(op.limit)
                // rb.Select.SetOrderBy(op.ordering)
                return src;
            }
            result = src;
        } else {
            if (op.getOrderBy() != null) {
                throw new SQLFeatureNotSupportedException("can't do ORDER BY on top of UNION");
            }
            result = new ConcatenateGen4Plan(sources);
        }

        if (op.isDistinct()) {
            List<Integer> colls = getCollationsFor(ctx, op);
            List<CheckCol> checkCols = getCheckColsForUnion(ctx, result, colls);
            return new DistinctGen4Plan(result, checkCols, isRoot);
        }
        return result;
    }

    private static List<Integer> getCollationsFor(PlanningContext ctx, Union n) throws SQLException {
        // TODO: coerce selects' select expressions' collations
        List<Integer> colls = new ArrayList<>();
        SQLSelectQuery select1 = n.getSelectStmts().get(0);
        if (select1 instanceof MySqlSelectQueryBlock) {
            for (SQLSelectItem item : ((MySqlSelectQueryBlock) select1).getSelectList()) {
                colls.add(0);
            }
        } else {
            throw new SQLException("Unknown expectations");
        }
        return colls;
    }

    private static List<CheckCol> getCheckColsForUnion(PlanningContext ctx, LogicalPlan result, List<Integer> colls) throws SQLException {
        List<CheckCol> checkCols = new ArrayList<>(colls.size());
        for (int i = 0; i < colls.size(); i++) {
            CheckCol checkCol = new CheckCol(i, colls.get(i));
            if (colls.get(i) != 0) {
                checkCols.add(checkCol);
                continue;
            }
            // We might need a weight string - let's push one
            // `might` because we just don't know what type we are dealing with.
            // If we encounter a numerical value, we don't need any weight_string values
            Integer newOffset = pushWeightStringForDistinct(ctx, result, i);
            checkCol.setWsCol(newOffset);
            checkCols.add(checkCol);
        }
        return checkCols;
    }

    private static List<LogicalPlan> transformAndMerge(PlanningContext ctx, Union op) throws SQLException {
        List<LogicalPlan> sources = new ArrayList<>();
        for (int i = 0; i < op.getSources().size(); i++) {
            // first we go over all the operator inputs and turn them into logical plans,
            // including horizon planning
            PhysicalOperator source = op.getSources().get(i);
            LogicalPlan plan = createLogicalPlan(ctx, source, op.getSelectStmts().get(i));
            sources.add(plan);
        }

        // next we'll go over all the plans from and check if any two can be merged. if they can, they are merged,
        // and we continue checking for pairs of plans that can be merged into a single route
        int idx = 0;
        while (idx < sources.size()) {
            Set<Integer> keep = new HashSet<>();
            LogicalPlan srcA = sources.get(idx);
            boolean merged = false;
            for (int j = 0; j < sources.size(); j++) {
                if (j <= idx) {
                    continue;
                }
                LogicalPlan srcB = sources.get(j);
                LogicalPlan newPlan = mergeUnionLogicalPlans(ctx, srcA, srcB);
                if (newPlan != null) {
                    sources.set(idx, newPlan);
                    srcA = newPlan;
                    merged = true;
                } else {
                    keep.add(j);
                }
            }
            if (!merged) {
                return sources;
            }
            List<LogicalPlan> phase = new ArrayList<>();
            for (int i = 0; i < sources.size(); i++) {
                if (keep.contains(i) || i <= idx) {
                    phase.add(sources.get(i));
                }
            }
            idx++;
            sources = phase;
        }
        return sources;
    }

    private static List<LogicalPlan> transformAndMergeInOrder(PlanningContext ctx, Union op) throws SQLException {
        List<LogicalPlan> sources = new ArrayList<>();
        // We go over all the input operators and turn them into logical plans
        for (int i = 0; i < op.getSources().size(); i++) {
            PhysicalOperator source = op.getSources().get(i);
            LogicalPlan plan = createLogicalPlan(ctx, source, op.getSelectStmts().get(i));
            if (i == 0) {
                sources.add(plan);
                continue;
            }
            // next we check if the last plan we produced can be merged with this new plan
            LogicalPlan last = sources.get(sources.size() - 1);
            LogicalPlan newPlan = mergeUnionLogicalPlans(ctx, last, plan);
            if (newPlan != null) {
                // if we could merge them, let's replace the last plan with this new merged one
                sources.set(sources.size() - 1, newPlan);
                continue;
            }
            // else we just add the new plan to the end of list
            sources.add(plan);
        }
        return sources;
    }

    private static void pushDistinct(LogicalPlan plan) {
        if (plan instanceof RouteGen4Plan) {
            SQLSelectQuery selectQuery = ((RouteGen4Plan) plan).getSelect();
            if (selectQuery instanceof MySqlSelectQueryBlock) {
                ((MySqlSelectQueryBlock) selectQuery).setDistionOption(SQLSetQuantifier.DISTINCT);
            } else if (selectQuery instanceof SQLUnionQuery) {
                ((SQLUnionQuery) selectQuery).setOperator(SQLUnionOperator.UNION);
            }
        } else if (plan instanceof ConcatenateGen4Plan) {
            List<LogicalPlan> sources = ((ConcatenateGen4Plan) plan).getSources();
            for (LogicalPlan source : sources) {
                pushDistinct(source);
            }
        }
    }

    private static Integer pushWeightStringForDistinct(PlanningContext ctx, LogicalPlan plan, int offset) throws SQLException {
        int newOffset = 0;
        if (plan instanceof RouteGen4Plan) {
            RouteGen4Plan routePlan = (RouteGen4Plan) plan;
            List<MySqlSelectQueryBlock> allSelects = Scoper.getAllSelects(routePlan.getSelect());
            for (MySqlSelectQueryBlock sel : allSelects) {
                SQLSelectItem item = sel.getSelectList().get(offset);
                SQLSelectItem expr = getWeightStringForSelectExpr(item);
                int getWsIdx = HorizonPlanning.checkIfAlreadyExists(expr, sel, ctx.getSemTable());
                if (getWsIdx != -1) {
                    return getWsIdx;
                }
                sel.getSelectList().add(expr);
                newOffset = sel.getSelectList().size() - 1;
            }
            // we leave the responsibility of truncating to distinct
            routePlan.getEroute().setTruncateColumnCount(0);
        } else if (plan instanceof ConcatenateGen4Plan) {
            ConcatenateGen4Plan node = (ConcatenateGen4Plan) plan;
            for (LogicalPlan source : node.getSources()) {
                newOffset = pushWeightStringForDistinct(ctx, source, offset);
            }
            node.getNoNeedToTypeCheck().add(newOffset);
        } else if (plan instanceof JoinGen4Plan) {
            throw new SQLException("todo: pushWeightStringForDistinct on JoinGen4Plan");
        } else {
            throw new SQLException("bug: not supported pushWeightStringForDistinct on" + plan.getClass());
        }
        return newOffset;
    }

    private static SQLSelectItem getWeightStringForSelectExpr(SQLSelectItem item) throws SQLException {
        return new SQLSelectItem(HorizonPlanning.weightStringFor(item.getExpr()));

    }

    private static LogicalPlan mergeUnionLogicalPlans(PlanningContext ctx, LogicalPlan left, LogicalPlan right) {
        if (!(left instanceof RouteGen4Plan)) {
            return null;
        }
        if (!(right instanceof RouteGen4Plan)) {
            return null;
        }
        RouteGen4Plan lroute = (RouteGen4Plan) left;
        RouteGen4Plan rroute = (RouteGen4Plan) right;
        if (canMergeUnionPlans(ctx, lroute, rroute)) {
            SQLUnionQuery sqlUnionQuery = new SQLUnionQuery(lroute.getSelect(), SQLUnionOperator.UNION, rroute.getSelect());
            lroute.setSelect(sqlUnionQuery);
            return mergeSystemTableInformation(lroute, rroute);
        }
        return null;
    }

    /**
     * mergeSystemTableInformation copies over information from the second route to the first and appends to it
     * @param a
     * @param b
     * @return
     */
    private static LogicalPlan mergeSystemTableInformation(RouteGen4Plan a, RouteGen4Plan b) {
        // safe to append system table schema and system table names, since either the routing will match or either side would be throwing an error
        // during run-time which we want to preserve. For example outer side has User in sys table schema and inner side has User and Main in sys table schema
        // Inner might end up throwing an error at runtime, but if it doesn't then it is safe to merge.

        return a;
    }

    private static boolean canMergeUnionPlans(PlanningContext ctx, RouteGen4Plan a, RouteGen4Plan b) {
        // this method should be close to tryMerge below. it does the same thing, but on logicalPlans instead of queryTrees
        switch (a.getEroute().getRoutingParameters().getRouteOpcode()) {
            case SelectUnsharded:
            case SelectReference:
                return a.getEroute().getRoutingParameters().getRouteOpcode() == b.getEroute().getRoutingParameters().getRouteOpcode();
            case SelectDBA:
                return canSelectDBAMerge(a, b);
            case SelectEqualUnique:
                // Check if they target the same shard.
                if (b.getEroute().getRoutingParameters().getRouteOpcode() == Engine.RouteOpcode.SelectEqualUnique
                    && a.getEroute().getRoutingParameters().getVindex() == b.getEroute().getRoutingParameters().getVindex()
                    && a.getCondition() != null && b.getCondition() != null
                    && gen4ValuesEqual(ctx, a.getCondition(), b.getCondition())
                ) {
                    return true;
                }
                return false;
            case SelectScatter:
                return b.getEroute().getRoutingParameters().getRouteOpcode() == Engine.RouteOpcode.SelectScatter;
            case SelectNext:
                return false;
            default:
                return false;
        }
    }

    private static boolean canSelectDBAMerge(RouteGen4Plan a, RouteGen4Plan b) {
        return false;
    }

    private static boolean gen4ValuesEqual(PlanningContext ctx, SQLExpr condition, SQLExpr condition1) {
        return false;
    }

    private static LogicalPlan createLogicalPlan(PlanningContext ctx, PhysicalOperator source, SQLSelectQuery selStmt) throws SQLException {
        LogicalPlan plan = transformToLogicalPlan(ctx, source, false);
        if (selStmt != null) {
            plan = Gen4Planner.planHorizon(ctx, plan, selStmt, true);
        }
        return plan;
    }

    private static LogicalPlan transformRoutePlan(PlanningContext ctx, Route op) throws SQLException {

        OperatorTransformers ot = new OperatorTransformers();
        String[] tableNames = ot.getAllTableNames(op);

        Vindex vindex = null;
        List<EvalEngine.Expr> values = new ArrayList<>();
        if (op.selectedVindex() != null) {
            vindex = op.getSelected().getFoundVindex();
            values = op.getSelected().getValues();
        }

        SQLExpr condition = getVindexPredicate(ctx, op);

        SQLSelectQuery sel = OperatorToQuery.toSQL(ctx, op.getSource());

        replaceSubQuery(ctx, sel);

        VKeyspace keyspace = new VKeyspace(ctx.getVschema().getDefaultKeyspace(), op.getKeyspace().getSharded());

        RouteGen4Engine eroute = new RouteGen4Engine(op.getRouterOpCode(), keyspace);
        eroute.setTableName(String.join(",", tableNames));
        eroute.getRoutingParameters().setVindex((SingleColumn) vindex);
        eroute.getRoutingParameters().setValues(values);
        eroute.setSelectQuery(sel);
        eroute.getRoutingParameters().getSystableTableSchema().addAll(op.getSysTableTableSchema());

        return new RouteGen4Plan(eroute, sel, op.tableID(), condition);
    }

    private static LogicalPlan transformApplyJoinPlan(PlanningContext ctx, ApplyJoin n) throws SQLException {
        LogicalPlan lhs = transformToLogicalPlan(ctx, n.getLHS(), false);
        LogicalPlan rhs = transformToLogicalPlan(ctx, n.getRHS(), false);

        Engine.JoinOpcode opCode = Engine.JoinOpcode.NormalJoin;
        if (n.getLeftJoin()) {
            opCode = Engine.JoinOpcode.LeftJoin;
        }

        return new JoinGen4Plan(
            lhs,
            rhs,
            opCode,
            n.getColumns(),
            n.getVars(),
            n.getLhsColumns()
        );
    }

    class GetOpAllTableNameFunc implements RoutePlanning.VisitOperatorFunc {
        public Set<String> tableNameSet;

        public GetOpAllTableNameFunc() {
            tableNameSet = new TreeSet<String>();
        }

        public String[] getAllTableNames() {
            String ret[] = new String[tableNameSet.size()];
            ret = tableNameSet.toArray(ret);
            Arrays.sort(ret);
            return ret;
        }

        @Override
        public boolean doFunc(PhysicalOperator op) {
            if (op instanceof Table) {
                if (((Table) op).getQTable().isInfSchema()) {
                    this.tableNameSet.add(((Table) op).getQTable().getTable().getDbAndTableName());
                } else {
                    this.tableNameSet.add(((Table) op).getQTable().getTable().getName());
                }
            }
            return true;
        }
    }

    private String[] getAllTableNames(Route op) throws SQLException {
        GetOpAllTableNameFunc func = new GetOpAllTableNameFunc();
        RoutePlanning.visitOperator(op, func);
        return func.getAllTableNames();
    }

    private static SQLExpr getVindexPredicate(PlanningContext ctx, Route op) {
        SQLExpr condition = null;
        if (op.getSelected() != null) {
            if (op.getSelected().getValueExprs().size() > 0) {
                condition = op.getSelected().getValueExprs().get(0);
            }
            // TODO SQLInSubQueryExpr SQLInListExpr
            // 不支持多列vindex
            for (SQLExpr predicate : op.getSelected().getPredicates()) {
                if (predicate instanceof SQLInListExpr) { // col in (xx,yy,zz)
                    SQLExpr leftExpr = ((SQLInListExpr) predicate).getExpr();
                    if (leftExpr instanceof SQLName) {
                        ((SQLInListExpr) predicate).setTargetList(new ArrayList<SQLExpr>() {{
                            add(new SQLVariantRefListExpr("::" + Engine.LIST_VAR_NAME));
                        }});
                    }
                } else if (predicate instanceof SQLInSubQueryExpr) { // col in (select xxx from xxx)
                    SQLExpr leftExpr = ((SQLInSubQueryExpr) predicate).getExpr();
                    if (leftExpr instanceof SQLName) {
                        // TODO
                        // SQLSelect subq = ((SQLInSubQueryExpr)predicate).getSubQuery();
                    }
                }
            }
        }
        return condition;
    }

    private static void replaceSubQuery(PlanningContext ctx, SQLSelectQuery sql) {

    }

}
