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

package com.jd.jdbc.planbuilder.gen4.operator.physical;

import com.jd.jdbc.common.tuple.Pair;
import com.jd.jdbc.common.tuple.Triple;
import com.jd.jdbc.common.util.CollectionUtils;
import com.jd.jdbc.context.PlanningContext;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.planbuilder.PlanBuilder;
import com.jd.jdbc.planbuilder.gen4.IntroducesTable;
import com.jd.jdbc.planbuilder.gen4.QueryTable;
import com.jd.jdbc.planbuilder.gen4.operator.Operator;
import com.jd.jdbc.planbuilder.gen4.operator.OperatorFuncs;
import com.jd.jdbc.planbuilder.gen4.operator.logical.Concatenate;
import com.jd.jdbc.planbuilder.gen4.operator.logical.Derived;
import com.jd.jdbc.planbuilder.gen4.operator.logical.Filter;
import com.jd.jdbc.planbuilder.gen4.operator.logical.Join;
import com.jd.jdbc.planbuilder.gen4.operator.logical.LogicalOperator;
import com.jd.jdbc.planbuilder.gen4.operator.logical.QueryGraph;
import com.jd.jdbc.planbuilder.gen4.operator.logical.Update;
import com.jd.jdbc.planbuilder.semantics.SchemaInformation;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator;
import com.jd.jdbc.sqlparser.ast.expr.SQLExprUtils;
import com.jd.jdbc.sqlparser.ast.expr.SQLInListExpr;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRewriteTableSchemaVisitor;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.vindexes.VKeyspace;
import com.jd.jdbc.vindexes.VschemaConstant;
import static com.jd.jdbc.vindexes.VschemaConstant.TYPE_REFERENCE;
import com.jd.jdbc.vindexes.hash.Binary;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import vschema.Vschema;

public class RoutePlanning {

    public static PhysicalOperator createPhysicalOperator(PlanningContext ctx, LogicalOperator logicalOperator) throws SQLException {
        if (logicalOperator instanceof QueryGraph) {
            // case ctx.PlannerVersion == querypb.ExecuteOptions_Gen4Left2Right:
            // return leftToRightSolve(ctx, op)
            return greedySolve(ctx, (QueryGraph) logicalOperator);
        } else if (logicalOperator instanceof Join) {
            return optimizeJoin(ctx, (Join) logicalOperator);
        } else if (logicalOperator instanceof Concatenate) {
            return optimizeUnion(ctx, (Concatenate) logicalOperator);
        } else if (logicalOperator instanceof Filter) {
            return optimizeFilter(ctx, (Filter) logicalOperator);
        } else if (logicalOperator instanceof Update) {
            // todo
            throw new SQLFeatureNotSupportedException();
        } else {
            throw new SQLException("BUG: unexpected logicalOperator type: ", logicalOperator.toString());
        }
    }

    private static PhysicalOperator optimizeUnion(PlanningContext ctx, Concatenate op) throws SQLException {
        List<PhysicalOperator> sources = new ArrayList<>();
        for (LogicalOperator source : op.getSources()) {
            PhysicalOperator qt = createPhysicalOperator(ctx, source);
            sources.add(qt);
        }
        return new Union(sources, op.getSelectStmts(), op.isDistinct(), op.getOrderBy());
    }

    private static PhysicalOperator optimizeFilter(PlanningContext ctx, Filter op) throws SQLException {
        PhysicalOperator src = createPhysicalOperator(ctx, op.getSource());

        com.jd.jdbc.planbuilder.gen4.operator.physical.Filter filter = new com.jd.jdbc.planbuilder.gen4.operator.physical.Filter();

        filter.setPredicates(op.getPredicates());

        if (src instanceof Route) {
            filter.setSource(((Route) src).getSource());
            ((Route) src).setSource(filter);
            return src;
        }

        filter.setSource(src);
        return filter;

    }

    private static PhysicalOperator optimizeJoin(PlanningContext ctx, Join op) throws SQLException {
        PhysicalOperator lhs = createPhysicalOperator(ctx, op.getLHS());
        PhysicalOperator rhs = createPhysicalOperator(ctx, op.getRHS());

        List<SQLExpr> joinPredicates = PlanBuilder.splitAndExpression(new ArrayList<>(), op.getPredicate());
        return mergeOrJoin(ctx, lhs, rhs, joinPredicates, !op.getLeftJoin());
    }

    private static PhysicalOperator greedySolve(PlanningContext ctx, QueryGraph logicalOperator) throws SQLException {
        List<PhysicalOperator> routeOps = seedOperatorList(ctx, logicalOperator);
        OpCache planCache = new OpCache();

        return mergeRoutes(ctx, logicalOperator, routeOps, planCache, false);
    }

    /**
     * seedOperatorList returns a route for each table in the qg
     *
     * @param ctx
     * @param qg
     * @return
     * @throws SQLException
     */
    private static List<PhysicalOperator> seedOperatorList(PlanningContext ctx, QueryGraph qg) throws SQLException {
        int tablesSize = qg.getTables().size();
        List<PhysicalOperator> plans = new ArrayList<>(tablesSize);
        // we start by seeding the table with the single routes
        for (int i = 0; i < tablesSize; i++) {
            QueryTable table = qg.getTables().get(i);
            TableSet solves = ctx.getSemTable().tableSetFor(table.getAlias());
            Route plan = createRoute(ctx, table, solves);
            if (qg.getNoDeps() != null) {
                plan.setSource(new com.jd.jdbc.planbuilder.gen4.operator.physical.Filter(plan.getSource(), Collections.singletonList(qg.getNoDeps())));
            }
            plans.add(i, plan);
        }
        return plans;
    }

    private static Route createRoute(PlanningContext ctx, QueryTable table, TableSet solves) throws SQLException {
        if (table.isInfSchema()) {
            return createInfSchemaRoute(ctx, table);
        }
        SchemaInformation.SchemaInformationContext vschemaTable = ctx.getVschema().findTableOrVindex(table.getAlias());
        if (vschemaTable.getTable() == null) {
            throw new SQLException("table " + table.getTable().getName() + " not found");
        }
        Table tableSource = new Table();
        tableSource.setQTable(table);
        tableSource.setVTable(vschemaTable.getTable());
        Route plan = new Route();
        plan.setSource(tableSource);
        plan.setKeyspace(ctx.getKeyspace());

        for (Vschema.ColumnVindex colVindex : vschemaTable.getTable().getColumnVindexesList()) {
            VindexPlusPredicates vindexPlusPredicates = new VindexPlusPredicates();
            vindexPlusPredicates.setColVindex(colVindex);
            vindexPlusPredicates.setTableId(solves);
            plan.getVindexPreds().add(vindexPlusPredicates);
        }

        if (Objects.equals(VschemaConstant.TYPE_SEQUENCE, vschemaTable.getTable().getType())) {
            plan.setRouterOpCode(Engine.RouteOpcode.SelectNext);
        } else if (Objects.equals(VschemaConstant.TYPE_REFERENCE, vschemaTable.getTable().getType())) {
            plan.setRouterOpCode(Engine.RouteOpcode.SelectReference);
        } else if (!ctx.getKeyspace().getSharded()) {
            plan.setRouterOpCode(Engine.RouteOpcode.SelectUnsharded);
        } else if (StringUtils.isNotEmpty(vschemaTable.getTable().getPinned()) && Objects.equals(VschemaConstant.CODE_PINNED_TABLE, vschemaTable.getTable().getPinned())) {
            // Pinned tables have their keyspace ids already assigned.
            // Use the Binary vindex, which is the identity function
            // for keyspace id.
            plan.setRouterOpCode(Engine.RouteOpcode.SelectEqualUnique);

            Cost selectCost = new Cost();
            selectCost.setOpCode(Engine.RouteOpcode.SelectEqualUnique);

            VindexOption selectOption = new VindexOption();
            selectOption.setReady(true);
            // todo
            //selectOption.setValues(Lists.newArrayList(EvalEngine.newLiteralString(vschemaTable.getTable().getPinned().getBytes())));
            selectOption.setOpCode(Engine.RouteOpcode.SelectEqualUnique);
            selectOption.setFoundVindex(new Binary());
            selectOption.setCost(selectCost);

            plan.setSelected(selectOption);
        } else {
            plan.setRouterOpCode(Engine.RouteOpcode.SelectScatter);
        }

        for (SQLExpr predicate : table.getPredicates()) {
            plan.updateRoutingLogic(ctx, predicate);
        }

        if (plan.getRouterOpCode().equals(Engine.RouteOpcode.SelectScatter) && table.getPredicates().size() > 0) {
            // If we have a scatter query, it's worth spending a little extra time seeing if we can't improve it
            for (SQLExpr pred : table.getPredicates()) {
                List<SQLExpr> rewrittenList = tryRewriteOrToIn(pred);
                if (CollectionUtils.isEmpty(rewrittenList)) {
                    break;
                }
                for (SQLExpr rewritten : rewrittenList) {
                    plan.updateRoutingLogic(ctx, rewritten);
                }
            }
        }

        return plan;
    }

    private static Route createInfSchemaRoute(PlanningContext ctx, QueryTable table) throws SQLException {
        VKeyspace vKeyspace = ctx.getVschema().anyKeyspace();
        if (vKeyspace == null) {
            throw new SQLFeatureNotSupportedException();
        }

        Table src = new Table();
        src.setQTable(table);
        // src.setVTable(new Vschema.Table());

        Route route = new Route();
        route.setRouterOpCode(Engine.RouteOpcode.SelectDBA);
        route.setSource(src);
        route.setKeyspace(vKeyspace);

        for (SQLExpr sqlExpr : table.getPredicates()) {
            // todo
            VtRewriteTableSchemaVisitor visitor = new VtRewriteTableSchemaVisitor();
            sqlExpr.accept(visitor);
            SQLException exception = visitor.getException();
            if (exception != null) {
                throw exception;
            }
            List<EvalEngine.Expr> tableNameExpressionList = visitor.getTableNameExpressionList();
            if (tableNameExpressionList != null && !tableNameExpressionList.isEmpty()) {
                route.getSysTableTableSchema().addAll(tableNameExpressionList);
            }
        }

        return route;
    }

    public static List<SQLExpr> tryRewriteOrToIn(SQLExpr expr) {
        List<SQLExpr> sqlExprs = splitOrExpression(null, expr);
        if (CollectionUtils.isEmpty(sqlExprs)) {
            return null;
        }
        Map<SQLExpr, List<SQLExpr>> map = new HashMap<>();
        for (SQLExpr sqlExpr : sqlExprs) {
            if (sqlExpr instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) sqlExpr;
                if (binaryOpExpr.getLeft() instanceof SQLName && Objects.equals(SQLBinaryOperator.Equality, binaryOpExpr.getOperator())) {
                    map.computeIfAbsent(binaryOpExpr.getLeft(), key -> new ArrayList<>()).add(sqlExpr);
                } else if (binaryOpExpr.getRight() instanceof SQLName && Objects.equals(SQLBinaryOperator.Equality, binaryOpExpr.getOperator())) {
                    map.computeIfAbsent(binaryOpExpr.getRight(), key -> new ArrayList<>()).add(sqlExpr);
                }
            }
            if (sqlExpr instanceof SQLInListExpr) {
                SQLInListExpr inListExpr = (SQLInListExpr) sqlExpr;
                if (inListExpr.getExpr() instanceof SQLName) {
                    map.computeIfAbsent(inListExpr.getExpr(), key -> new ArrayList<>()).add(sqlExpr);
                }
            }
        }
        List<SQLExpr> sqlExprList = new ArrayList<>();
        for (Map.Entry<SQLExpr, List<SQLExpr>> entry : map.entrySet()) {
            if (entry.getValue().size() <= 1) {
                continue;
            }
            List<SQLExpr> tuple = new ArrayList<>();
            for (SQLExpr sqlExpr : entry.getValue()) {
                if (sqlExpr instanceof SQLBinaryOpExpr) {
                    SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) sqlExpr;
                    if (binaryOpExpr.getLeft() instanceof SQLName) {
                        tuple.add(binaryOpExpr.getRight());
                    } else if (binaryOpExpr.getRight() instanceof SQLName) {
                        tuple.add(binaryOpExpr.getLeft());
                    }
                }
                if (sqlExpr instanceof SQLInListExpr) {
                    SQLInListExpr inListExpr = (SQLInListExpr) sqlExpr;
                    if (inListExpr.getExpr() instanceof SQLName) {
                        tuple.addAll(inListExpr.getTargetList());
                    }
                }
            }
            SQLInListExpr inListExpr = new SQLInListExpr(entry.getKey());
            inListExpr.setTargetList(tuple);
            sqlExprList.add(inListExpr);
        }
        return sqlExprList;
    }

    private static List<SQLExpr> splitOrExpression(List<SQLExpr> filters, SQLExpr node) {
        if (node == null) {
            return filters;
        }
        if (filters == null) {
            filters = new ArrayList<>();
        }
        if (node instanceof SQLBinaryOpExpr) {
            if (((SQLBinaryOpExpr) node).getOperator().equals(SQLBinaryOperator.BooleanOr)) {
                filters = splitOrExpression(filters, ((SQLBinaryOpExpr) node).getLeft());
                return splitOrExpression(filters, ((SQLBinaryOpExpr) node).getRight());
            }
        }
        filters.add(node);
        return filters;
    }

    private static PhysicalOperator mergeRoutes(PlanningContext ctx, QueryGraph qg, List<PhysicalOperator> physicalOps, OpCache planCache, Boolean crossJoinsOK) throws SQLException {
        if (physicalOps.size() == 0) {
            return null;
        }

        while (physicalOps.size() > 1) {
            //throw new SQLException("BUG: logicalOperator length > 1");
            Triple<PhysicalOperator, Integer, Integer> ret = findBestJoin(ctx, qg, physicalOps, planCache, crossJoinsOK);
            // if we found a plan, we'll replace the two plans that were joined with the join plan created
            if (ret.getLeft() != null) {
                // we remove one plan, and replace the other
                int lIdx = ret.getMiddle();
                int rIdx = ret.getRight();

                if (rIdx > lIdx) {
                    physicalOps.remove(rIdx);
                    physicalOps.remove(lIdx);
                } else {
                    physicalOps.remove(lIdx);
                    physicalOps.remove(rIdx);
                }
                physicalOps.add(ret.getLeft());
            } else {
                if (crossJoinsOK) {
                    throw new SQLException("BUG: should not happen");
                }
                // we will only fail to find a join plan when there are only cross joins left
                // when that happens, we switch over to allow cross joins as well.
                // this way we prioritize joining physicalOps with predicates first
                crossJoinsOK = true;
            }
        }
        return physicalOps.get(0);
    }


    private static Triple<PhysicalOperator, Integer, Integer> findBestJoin(PlanningContext ctx, QueryGraph qg, List<PhysicalOperator> plans, OpCache planCache, Boolean crossJoinsOK)
        throws SQLException {
        PhysicalOperator bestPlan = null;
        int lIdx = 0;
        int rIdx = 0;
        for (int i = 0; i < plans.size(); i++) {
            PhysicalOperator lhs = plans.get(i);
            for (int j = 0; j < plans.size(); j++) {
                if (i == j) {
                    continue;
                }
                PhysicalOperator rhs = plans.get(j);
                List<SQLExpr> joinPredicates = qg.getPredicates(lhs.tableID(), rhs.tableID());

                if (joinPredicates.size() == 0 && !crossJoinsOK) {
                    // if there are no predicates joining the two tables,
                    // creating a join between them would produce a
                    // cartesian product, which is almost always a bad idea
                    continue;
                }
                PhysicalOperator plan = getJoinFor(ctx, planCache, lhs, rhs, joinPredicates);
                if (bestPlan == null || plan.cost() < bestPlan.cost()) {
                    bestPlan = plan;
                    // remember which plans we based on, so we can remove them later
                    lIdx = i;
                    rIdx = j;
                }

            }
        }
        return Triple.of(bestPlan, lIdx, rIdx);
    }

    private static PhysicalOperator getJoinFor(PlanningContext ctx, OpCache cm, PhysicalOperator lhs, PhysicalOperator rhs, List<SQLExpr> joinPredicates) throws SQLException {
        TableSetPair solves = new TableSetPair(lhs.tableID(), rhs.tableID());
        PhysicalOperator cachedPlan = cm.getOpCacheMap().get(solves);
        if (cachedPlan != null) {
            return cachedPlan;
        }

        PhysicalOperator join = mergeOrJoin(ctx, lhs, rhs, joinPredicates, true);
        cm.getOpCacheMap().put(solves, join);
        return join;
    }

    /**
     * requiresSwitchingSides will return true if any of the operators with the root from the given operator tree
     * is of the type that should not be on the RHS of a join
     *
     * @param ctx
     * @param op
     * @return
     */
    private static Boolean requiresSwitchingSides(PlanningContext ctx, PhysicalOperator op) throws SQLException {
        VisitDerived vd = new VisitDerived(ctx);
        visitOperator(op, vd);
        return vd.isRequired();
    }

    private static PhysicalOperator mergeOrJoin(PlanningContext ctx, PhysicalOperator lhs, PhysicalOperator rhs, List<SQLExpr> joinPredicates, boolean inner) throws SQLException {
        JoinMergerFunc merger = new JoinMergerFunc(joinPredicates, inner);

        PhysicalOperator newPlan = tryMerge(ctx, lhs, rhs, joinPredicates, merger);
        if (newPlan != null) {
            return newPlan;
        }

        if (joinPredicates.size() > 0) {
            if (requiresSwitchingSides(ctx, rhs)) {
                if (!inner) {
                    throw new SQLException("unsupported: LEFT JOIN not supported for derived tables");
                }
                if (requiresSwitchingSides(ctx, lhs)) {
                    throw new SQLException("unsupported: JOIN not supported between derived tables");
                }

                ApplyJoin join = new ApplyJoin(lhs.clone(), rhs.clone(), !inner);
                return pushJoinPredicates(ctx, joinPredicates, join);
            }
        }
        ApplyJoin join = new ApplyJoin(lhs.clone(), rhs.clone(), !inner);
        return pushJoinPredicates(ctx, joinPredicates, join);

    }

    public static Route createRouteOperatorForJoin(Route aRoute, Route bRoute, List<SQLExpr> joinPredicates, boolean inner) {

        // append system table names from both the routes.

        Route newRoute = new Route();
        newRoute.setRouterOpCode(aRoute.getRouterOpCode());
        newRoute.setKeyspace(aRoute.getKeyspace());

        List<VindexPlusPredicates> predicates = new ArrayList<>();
        predicates.addAll(aRoute.getVindexPreds());
        predicates.addAll(bRoute.getVindexPreds());
        newRoute.setVindexPreds(predicates);

        List<SQLExpr> seenPredicates = new ArrayList<>();
        seenPredicates.addAll(aRoute.getSeenPredicates());
        seenPredicates.addAll(bRoute.getSeenPredicates());
        newRoute.setSeenPredicates(seenPredicates);

        newRoute.getSysTableTableSchema().addAll(aRoute.getSysTableTableSchema());
        newRoute.getSysTableTableSchema().addAll(bRoute.getSysTableTableSchema());


        ApplyJoin source = new ApplyJoin(aRoute.getSource(), bRoute.getSource(), !inner, SQLBinaryOpExpr.combine(joinPredicates, SQLBinaryOperator.BooleanAnd));
        newRoute.setSource(source);

        // TODO  == ?
        if (aRoute.selectedVindex() == bRoute.selectedVindex()) {
            newRoute.setSelected(aRoute.getSelected());
        }

        return newRoute;
    }

    private static PhysicalOperator tryMerge(PlanningContext ctx, PhysicalOperator a, PhysicalOperator b, List<SQLExpr> joinPredicates, MergerFunc mergerFunc) throws SQLException {
        // operatorsToRoutes
        Route aRoute = null;
        if (a instanceof Route) {
            aRoute = (Route) a.clone();
        } else {
            return null;
        }

        Route bRoute = null;
        if (b instanceof Route) {
            bRoute = (Route) b.clone();
        } else {
            return null;
        }

        Boolean sameKeyspace = aRoute.getKeyspace().equals(bRoute.getKeyspace());
        if (sameKeyspace || (isDualTable(aRoute) || isDualTable(bRoute))) {
            // tryMergeReferenceTable
            Route tree = tryMergeReferenceTable(aRoute, bRoute, mergerFunc);
            if (tree != null) {
                return tree;
            }
        }

        switch (aRoute.getRouterOpCode()) {
            case SelectUnsharded:
            case SelectDBA:
                if (aRoute.getRouterOpCode() == bRoute.getRouterOpCode() && sameKeyspace) {
                    return mergerFunc.merger(aRoute, bRoute);
                }
                break;
            case SelectEqualUnique:
                // If the two routes fully match, they can be merged together.
                if (bRoute.getRouterOpCode() == Engine.RouteOpcode.SelectEqualUnique) {
                    boolean checkVindex = false;
                    if (aRoute.selectedVindex() != null && bRoute.selectedVindex() != null) {
                        // 我们仅仅比较vindex的类型是否一致
                        checkVindex = aRoute.selectedVindex().getClass() == bRoute.selectedVindex().getClass();
                    }
                    if (checkVindex && gen4ValuesEqual(ctx, aRoute.vindexExpressions(), bRoute.vindexExpressions())) {
                        return mergerFunc.merger(aRoute, bRoute);
                    }

                }
                // If the two routes don't match, fall through to the next case and see if we
                // can merge via join predicates instead.
                // fallthrough
            case SelectScatter:
            case SelectIN:
            case SelectNone:
                if (joinPredicates.isEmpty()) {
                    // If we are doing two Scatters, we have to make sure that the
                    // joins are on the correct vindex to allow them to be merged
                    // no join predicates - no vindex
                    return null;
                }
                if (!sameKeyspace) {
                    throw new SQLException("unsupported: cross-shard correlated subquery");
                }

                boolean canMerge = canMergeOnFilters(ctx, aRoute, bRoute, joinPredicates);
                if (!canMerge) {
                    return null;
                }

                Route r = mergerFunc.merger(aRoute, bRoute);

                // If we have a `None` route opcode, we want to keep it -
                // we only try to find a better Vindex for other route opcodes
                if (aRoute.getRouterOpCode() != Engine.RouteOpcode.SelectNone) {
                    r.pickBestAvailableVindex();
                }
                return r;
        }
        return null;
    }

    private static Boolean isDualTable(Route route) throws SQLException {
        List<Operator> sources = leaves(route);
        if (sources.size() > 1) {
            return false;
        }

        Operator src = sources.get(0);

        if (!(src instanceof Table)) {
            return false;
        }

        if (((Table) src).getVTable() == null) {
            return false;
        }

        if (((Table) src).getQTable() == null) {
            return false;
        }


        return TYPE_REFERENCE.equalsIgnoreCase(((Table) src).getVTable().getType()) && ((Table) src).getQTable().getTable().getName().equalsIgnoreCase("dual") &&
            ((Table) src).getQTable().getTable().getQualifier().isEmpty();

    }

    private static List<Operator> leaves(Operator op) throws SQLException {
        if (op instanceof QueryGraph || op instanceof Table) {
            return new ArrayList<>();
        }
        // logical
        if (op instanceof Derived) {
            return new ArrayList<>(Arrays.asList(((Derived) op).getInner()));
        }

        if (op instanceof Join) {
            return new ArrayList<>(Arrays.asList(((Join) op).getLHS(), ((Join) op).getRHS()));
        }

        // physical

        if (op instanceof ApplyJoin) {
            return new ArrayList<>(Arrays.asList(((ApplyJoin) op).getLHS(), ((ApplyJoin) op).getRHS()));
        }

        if (op instanceof com.jd.jdbc.planbuilder.gen4.operator.physical.Filter) {
            return new ArrayList<>(Arrays.asList(((com.jd.jdbc.planbuilder.gen4.operator.physical.Filter) op).getSource()));
        }

        if (op instanceof Route) {
            return new ArrayList<>(Arrays.asList(((Route) op).getSource()));
        }

        throw new SQLException("leaves unknown type " + op.getClass().toString());
    }

    private static Route tryMergeReferenceTable(Route aRoute, Route bRoute, MergerFunc mergerFunc) throws SQLException {
        Engine.RouteOpcode opCode;
        VindexOption vindex;
        VKeyspace ks;

        if (aRoute.getRouterOpCode() == Engine.RouteOpcode.SelectReference) {
            vindex = bRoute.getSelected();
            opCode = bRoute.getRouterOpCode();
            ks = bRoute.getKeyspace();
        } else if (bRoute.getRouterOpCode() == Engine.RouteOpcode.SelectReference) {
            vindex = aRoute.getSelected();
            opCode = aRoute.getRouterOpCode();
            ks = aRoute.getKeyspace();
        } else {
            return null;
        }

        Route r = mergerFunc.merger(aRoute, bRoute);

        r.setRouterOpCode(opCode);
        r.setSelected(vindex);
        r.setKeyspace(ks);
        return r;
    }

    private static Boolean gen4ValuesEqual(PlanningContext ctx, List<SQLExpr> a, List<SQLExpr> b) {
        if (a == null && b == null) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }

        if (a.size() != b.size()) {
            return false;
        }

        for (int i = 0; i < a.size(); i++) {
            SQLExpr aExpr = a.get(i);
            SQLExpr bExpr = b.get(i);
            if (!SQLExprUtils.equals(aExpr, bExpr)) {
                return false;
            }
            // ColName
            if (aExpr instanceof SQLName) {
                if (ctx.getSemTable().directDeps(aExpr) != ctx.getSemTable().directDeps(bExpr)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static PhysicalOperator pushJoinPredicates(PlanningContext ctx, List<SQLExpr> exprs, PhysicalOperator op) throws SQLException {
        if (exprs.isEmpty()) {
            return op;
        }

        if (op instanceof ApplyJoin) {
            return pushJoinPredicateOnJoin(ctx, exprs, (ApplyJoin) op);
        } else if (op instanceof Route) {
            return pushJoinPredicateOnRoute(ctx, exprs, (Route) op);
        } else if (op instanceof Table) {
            return OperatorFuncs.pushPredicate(ctx, SQLBinaryOpExpr.combine(exprs, SQLBinaryOperator.BooleanAnd), op);
        } else if (op instanceof Derived) {
            return pushJoinPredicateOnDerived(ctx, exprs, (Derived) op);
        } else if (op instanceof com.jd.jdbc.planbuilder.gen4.operator.physical.Filter) {
            ((com.jd.jdbc.planbuilder.gen4.operator.physical.Filter) op).getPredicates().addAll(exprs);
            return op;
        } else {
            throw new SQLException("unknown type pushJoinPredicates :" + op.getClass().getName());
        }

    }

    private static PhysicalOperator pushJoinPredicateOnJoin(PlanningContext ctx, List<SQLExpr> exprs, ApplyJoin inNode) throws SQLException {
        ApplyJoin node = (ApplyJoin) inNode.clone();
        List<SQLExpr> lhsPreds = new ArrayList<>();
        List<SQLExpr> rhsPreds = new ArrayList<>();
        List<String> lhsVarsName = new ArrayList<>();

        for (SQLExpr expr : exprs) {
            // We find the dependencies for the given expression and if they are solved entirely by one
            // side of the join tree, then we push the predicate there and do not break it into parts.
            // In case a predicate has no dependencies, then it is pushed to both sides so that we can filter
            // rows as early as possible making join cheaper on the vtgate level.

            TableSet depsForExpr = ctx.getSemTable().recursiveDeps(expr);
            boolean singleSodeDeps = false;
            TableSet lhsTables = node.getLHS().tableID();

            if (depsForExpr.isSolvedBy(lhsTables)) {
                lhsPreds.add(expr);
                singleSodeDeps = true;
            }
            if (depsForExpr.isSolvedBy(node.getRHS().tableID())) {
                rhsPreds.add(expr);
                singleSodeDeps = true;
            }

            if (singleSodeDeps) {
                continue;
            }

            Triple<List<String>, List<SQLName>, SQLExpr> ret = OperatorFuncs.breakExpressioninLHSandRHS(ctx, expr, lhsTables);
            node.getLhsColumns().addAll(ret.getMiddle());
            lhsVarsName.addAll(ret.getLeft());
            rhsPreds.add(ret.getRight());
        }

        if (!node.getLhsColumns().isEmpty() && !lhsVarsName.isEmpty()) {
            Pair<PhysicalOperator, List<Integer>> ret = OperatorFuncs.pushOutputColumns(ctx, node.getLHS(), node.getLhsColumns());
            node.setLHS(ret.getLeft());
            for (int i = 0; i < ret.getRight().size(); i++) {
                node.getVars().put(lhsVarsName.get(i), ret.getRight().get(i));
            }
        }

        PhysicalOperator lhsPlan = pushJoinPredicates(ctx, lhsPreds, node.getLHS());
        PhysicalOperator rhsPlan = pushJoinPredicates(ctx, rhsPreds, node.getRHS());

        node.setLHS(lhsPlan);
        node.setRHS(rhsPlan);

        if (node.getPredicate() != null) {
            exprs.add(node.getPredicate());
        }
        node.setPredicate(SQLBinaryOpExpr.combine(exprs, SQLBinaryOperator.BooleanAnd));
        return node;
    }

    private static PhysicalOperator pushJoinPredicateOnRoute(PlanningContext ctx, List<SQLExpr> exprs, Route op) throws SQLException {
        for (SQLExpr expr : exprs) {
            op.updateRoutingLogic(ctx, expr);
        }

        PhysicalOperator newSrc = pushJoinPredicates(ctx, exprs, op.getSource());
        op.setSource(newSrc);
        return op;
    }

    private static PhysicalOperator pushJoinPredicateOnDerived(PlanningContext ctx, List<SQLExpr> exprs, Derived node) {
        //TODO
        return (PhysicalOperator) node;
    }

    private static Boolean canMergeOnFilter(PlanningContext ctx, Route a, Route b, SQLExpr predicate) throws SQLException {
        if (!(predicate instanceof SQLBinaryOpExpr)) {
            return false;
        }
        if (((SQLBinaryOpExpr) predicate).getOperator() != SQLBinaryOperator.Equality) {
            return false;
        }

        SQLExpr left = ((SQLBinaryOpExpr) predicate).getLeft();
        SQLExpr right = ((SQLBinaryOpExpr) predicate).getRight();

        Vschema.ColumnVindex lVindex = findColumnVindex(ctx, a, left);
        if (lVindex == null) {
            // left right 交换
            SQLExpr tmp = left;
            left = right;
            right = tmp;
            lVindex = findColumnVindex(ctx, a, left);
        }
        Vschema.ColumnVindex rVindex = findColumnVindex(ctx, b, right);

        if (lVindex == null || rVindex == null) {
            return false;
        }
        return lVindex.getName().equalsIgnoreCase(rVindex.getName());
    }

    public static Vschema.ColumnVindex findColumnVindex(PlanningContext ctx, PhysicalOperator a, SQLExpr exp) throws SQLException {
        if (!(exp instanceof SQLName)) {
            return null;
        }
        exp = unwrapDerivedTables(ctx, exp);

        // for each equality expression that exp has with other column name, we check if it
        // can be solved by any table in our routeTree. If an equality expression can be solved,
        // we check if the equality expression and our table share the same vindex, if they do:
        // the method will return the associated vindexes.SingleColumn.

        List<SQLExpr> exprs = ctx.getSemTable().getExprAndEqualities(exp);
        for (SQLExpr expr : exprs) {
            if (!(expr instanceof SQLName)) {
                continue;
            }

            TableSet deps = ctx.getSemTable().recursiveDeps(expr);
            FindSingleColumn fsc = new FindSingleColumn(deps, ((SQLName) expr));
            visitOperator(a, fsc);
            if (fsc.getFindVindex() != null) {
                return fsc.getFindVindex();
            }
        }
        return null;
    }

    /**
     * unwrapDerivedTables we want to find the bottom layer of derived tables
     *
     * @return
     */
    private static SQLExpr unwrapDerivedTables(PlanningContext ctx, SQLExpr exp) {
        return exp;
    }

    private static Boolean canMergeOnFilters(PlanningContext ctx, Route a, Route b, List<SQLExpr> joinPredicates) throws SQLException {
        for (SQLExpr predicate : joinPredicates) {
            List<SQLExpr> filters = PlanBuilder.splitAndExpression(new ArrayList<>(), predicate);
            for (SQLExpr expr : filters) {
                if (canMergeOnFilter(ctx, a, b, expr)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static void visitOperator(PhysicalOperator op, VisitOperatorFunc func) throws SQLException {
        if (!func.doFunc(op)) {
            return;
        }
        if (op instanceof Table) {
            return;
        } else if (op instanceof Route) {
            visitOperator(((Route) op).getSource(), func);
        } else if (op instanceof com.jd.jdbc.planbuilder.gen4.operator.physical.Filter) {
            visitOperator(((com.jd.jdbc.planbuilder.gen4.operator.physical.Filter) op).getSource(), func);
        } else if (op instanceof ApplyJoin) {
            visitOperator(((ApplyJoin) op).getLHS(), func);
            visitOperator(((ApplyJoin) op).getRHS(), func);
        } else {
            throw new SQLException("unknown operator type while visiting " + op.getClass().getName());
        }
    }

    public interface VisitOperatorFunc {
        boolean doFunc(PhysicalOperator op);
    }

    public interface MergerFunc {
        Route merger(Route a, Route b) throws SQLException;
    }

    static class JoinMergerFunc implements MergerFunc {

        private List<SQLExpr> joinPredicates;

        private boolean inner;

        public JoinMergerFunc(List<SQLExpr> joinPredicates, boolean inner) {
            this.inner = inner;
            this.joinPredicates = joinPredicates;
        }

        @Override
        public Route merger(Route a, Route b) throws SQLException {
            return RoutePlanning.createRouteOperatorForJoin(a, b, this.joinPredicates, this.inner);
        }
    }

    @Getter
    static class FindSingleColumn implements VisitOperatorFunc {

        private TableSet deps;

        private SQLName col;

        private Vschema.ColumnVindex findVindex;

        public FindSingleColumn(TableSet deps, SQLName col) {
            this.deps = deps;
            this.col = col;
        }

        @Override
        public boolean doFunc(PhysicalOperator op) {
            if (!(op instanceof IntroducesTable)) {
                return true;
            }
            if (this.deps.isSolvedBy(((IntroducesTable) op).getQTable().getId())) {
                List<Vschema.ColumnVindex> vindexList = ((IntroducesTable) op).getVTable().getColumnVindexesList();
                for (Vschema.ColumnVindex vindex : vindexList) {
                    // Boolean isSingle =  SingleColumn;

                    if (vindex.getColumn().equalsIgnoreCase(this.col.getSimpleName())) {
                        this.findVindex = vindex;
                        return false;
                    }
                }
            }
            return false;
        }
    }

    @Getter
    static class VisitDerived implements VisitOperatorFunc {

        private boolean required = false;

        private PlanningContext ctx;

        public VisitDerived(PlanningContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public boolean doFunc(PhysicalOperator op) {

            if (op instanceof Derived) {
                if (((Derived) op).isMergeable(this.ctx)) {
                    this.required = true;
                    return false;
                }
            }
            return true;
        }
    }

}
