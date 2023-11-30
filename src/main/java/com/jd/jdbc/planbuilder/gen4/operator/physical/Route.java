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

import com.google.common.collect.Lists;
import com.jd.jdbc.context.PlanningContext;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLInListExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLListExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNullExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefListExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.vindexes.VKeyspace;
import com.jd.jdbc.vindexes.Vindex;
import com.jd.jdbc.vindexes.hash.BinaryHash;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import vschema.Vschema;

@Getter
@Setter
public class Route implements PhysicalOperator {
    private PhysicalOperator source;

    private Engine.RouteOpcode routerOpCode;

    private VKeyspace keyspace;

    // here we store the possible vindexes we can use so that when we add predicates to the plan,
    // we can quickly check if the new predicates enables any new vindex Options
    private List<VindexPlusPredicates> vindexPreds;

    // the best option available is stored here
    private VindexOption selected;

    // The following two fields are used when routing information_schema queries
    //SysTableTableSchema []evalengine.Expr
    // todo: SysTableTableName   map[string]evalengine.Expr

    private List<EvalEngine.Expr> sysTableTableSchema;

    // seenPredicates contains all the predicates that have had a chance to influence routing.
    // If we need to replan routing, we'll use this list
    private List<SQLExpr> seenPredicates;

    // targetDestination specifies an explicit target destination tablet type
    private Destination targetDestination;

    public Route() {
        this.vindexPreds = new ArrayList<>(10);
        this.seenPredicates = new ArrayList<>(8);
        this.sysTableTableSchema = new ArrayList<>();
    }

    public Route(Route r) {
        this.source = r.getSource();
        this.routerOpCode = r.getRouterOpCode();
        this.keyspace = r.getKeyspace();
        this.vindexPreds = r.vindexPreds;
        this.selected = r.getSelected();
        this.seenPredicates = r.seenPredicates;
        this.targetDestination = r.targetDestination;
        // this.sysTableTableSchema = new ArrayList<>(r.getSysTableTableSchema().size());
        this.sysTableTableSchema = r.getSysTableTableSchema();
    }

    @Override
    public TableSet tableID() {
        return this.getSource().tableID();
    }

    @Override
    public SQLSelectItem unsolvedPredicates(SemTable semTable) {
        return null;
    }

    @Override
    public void checkValid() throws SQLException {

    }

    @Override
    public Integer cost() {
        switch (routerOpCode) {
            case SelectDBA:
            case SelectNext:
            case SelectNone:
            case SelectReference:
            case SelectUnsharded:
                return 0;
            case SelectEqualUnique:
                return 1;
            case SelectEqual:
                return 5;
            case SelectIN:
                return 10;
            case SelectScatter:
                return 20;
            default:
                return 1;
        }
    }

    @Override
    public PhysicalOperator clone() {

        Route clone = new Route(this);

        clone.setSource(this.getSource().clone());
        List<VindexPlusPredicates> vindexPreds = new ArrayList<>(this.getVindexPreds().size());
        for (VindexPlusPredicates pred : this.getVindexPreds()) {
            vindexPreds.add(pred);
        }
        clone.setVindexPreds(vindexPreds);
        return clone;
    }

    public Vindex selectedVindex() {
        if (this.selected == null) {
            return null;
        }
        return this.selected.getFoundVindex();
    }

    public List<SQLExpr> vindexExpressions() {
        if (this.selected == null) {
            return null;
        }
        return this.selected.getValueExprs();
    }

    private boolean canImprove() {
        if (this.getRouterOpCode().equals(Engine.RouteOpcode.SelectNone)) {
            return false;
        }
        return true;
    }

    public void updateRoutingLogic(PlanningContext ctx, SQLExpr expr) throws SQLException {
        this.getSeenPredicates().add(expr);
        tryImprovingVindex(ctx, expr);
    }

    private void tryImprovingVindex(PlanningContext ctx, SQLExpr expr) throws SQLException {
        if (canImprove()) {
            boolean newVindexFound = searchForNewVindexes(ctx, expr);
            if (newVindexFound) {
                pickBestAvailableVindex();
            }
        }
    }

    private boolean searchForNewVindexes(PlanningContext ctx, SQLExpr predicate) throws SQLException {
        boolean newVindexFound = false;
        planComparisonRetrun planComparisonRetrun;
        if (predicate instanceof SQLBinaryOpExpr) {
            planComparisonRetrun = planComparison(ctx, (SQLBinaryOpExpr) predicate);
        } else if (predicate instanceof SQLInListExpr) {
            planComparisonRetrun = planComparison(ctx, (SQLInListExpr) predicate);
        } else {
            //throw new SQLFeatureNotSupportedException();
            return false;
        }

        if (planComparisonRetrun.exitEarly) {
            return false;
        }
        if (planComparisonRetrun.found) {
            newVindexFound = true;
        }
        return newVindexFound;
    }

    private planComparisonRetrun planComparison(PlanningContext ctx, SQLBinaryOpExpr binaryOpExpr) throws SQLException {
        switch (binaryOpExpr.getOperator()) {
            case Equality:
                boolean found = planEqualOp(ctx, binaryOpExpr);
                return new planComparisonRetrun(found, false);
            case Is:
                boolean foundis = planIsExpr(ctx, binaryOpExpr);
                return new planComparisonRetrun(foundis, false);
            default:
                return new planComparisonRetrun(false, false);
        }
    }

    private planComparisonRetrun planComparison(PlanningContext ctx, SQLInListExpr inListExpr) throws SQLException {
        if (inListExpr.isNot()) {
            // NOT IN is always a scatter, except when we can be sure it would return nothing
            if (isImpossibleNotIN(inListExpr)) {
                return new planComparisonRetrun(false, true);
            }
        } else {
            if (isImpossibleIN(inListExpr)) {
                return new planComparisonRetrun(false, true);
            }
            boolean found = planInOp(ctx, inListExpr);
            return new planComparisonRetrun(found, false);
        }
        return new planComparisonRetrun(false, false);
    }

    private boolean planEqualOp(PlanningContext ctx, SQLBinaryOpExpr binaryOpExpr) throws SQLException {
        if (binaryOpExpr.getLeft() instanceof SQLNullExpr || binaryOpExpr.getRight() instanceof SQLNullExpr) {
            // we are looking at ANDed predicates in the WHERE clause.
            // since we know that nothing returns true when compared to NULL,
            // so we can safely bail out here
            this.setSelectNoneOpcode();
            return false;
        }
        String columnName;

        SQLExpr vdValue = binaryOpExpr.getRight();
        if (!(binaryOpExpr.getLeft() instanceof SQLName)) {
            if (binaryOpExpr.getRight() instanceof SQLName) {
                // either the LHS or RHS have to be a column to be useful for the vindex
                vdValue = binaryOpExpr.getLeft();
                columnName = ((SQLName) binaryOpExpr.getRight()).getSimpleName();
            } else {
                return false;
            }
        } else {
            columnName = ((SQLName) binaryOpExpr.getLeft()).getSimpleName();
        }

        EvalEngine.Expr val = makeEvalEngineExpr(ctx, vdValue);

        return haveMatchingVindex(ctx, binaryOpExpr, vdValue, columnName, Collections.singletonList(val), Engine.RouteOpcode.SelectEqualUnique);
    }

    /**
     * makePlanValue transforms the given sqlparser.Expr into a sqltypes.PlanValue.
     * If the given sqlparser.Expr is an argument and can be found in the r.argToReplaceBySelect then the
     * method will stops and return nil values.
     * Otherwise, the method will try to apply makePlanValue for any equality the sqlparser.Expr n has.
     * The first PlanValue that is successfully produced will be returned.
     *
     * @param ctx
     * @param n
     * @return
     * @throws SQLException
     */
    private EvalEngine.Expr makeEvalEngineExpr(PlanningContext ctx, SQLExpr n) throws SQLException {
        List<SQLExpr> exprEqualList = ctx.getSemTable().getExprAndEqualities(n);
        for (SQLExpr expr : exprEqualList) {
            EvalEngine.Expr pv = EvalEngine.translate(expr, ctx.getSemTable());
            if (pv != null) {
                return pv;
            }
        }
        return null;
    }

    private EvalEngine.Expr makeInListEvalEngineExpr(PlanningContext ctx, List<SQLExpr> targetList) throws SQLException {
        List<SQLExpr> exprEqualList = ctx.getSemTable().getExprAndEqualities(targetList);
        EvalEngine.Expr pv = EvalEngine.translateEx(exprEqualList, ctx.getSemTable(),true);
        return pv;
    }

    private boolean isImpossibleIN(SQLInListExpr inListExpr) {
        List<SQLExpr> right = inListExpr.getTargetList();
        if ((right.size() == 1) && (right.get(0) instanceof SQLNullExpr)) {
            // WHERE col IN (null)
            this.setSelectNoneOpcode();
            return true;
        }
        return false;
    }

    private boolean planInOp(PlanningContext ctx, SQLInListExpr inListExpr) throws SQLException {
        SQLExpr left = inListExpr.getExpr();
        if (left instanceof SQLName) {
            for (VindexPlusPredicates v : this.getVindexPreds()) {
                if (!SQLUtils.nameEquals(v.getColVindex().getColumn(), ((SQLName) left).getSimpleName())) {
                    // 如果不是Vindex，提前返回
                    return false;
                }
            }

            EvalEngine.Expr pv = this.makeInListEvalEngineExpr(ctx, inListExpr.getTargetList());
            if (pv == null) {
                return false;
            }
            inListExpr.setTargetList(Collections.singletonList(new SQLVariantRefListExpr("::" + Engine.LIST_VAR_NAME)));
            return haveMatchingVindex(ctx, inListExpr, null, ((SQLName) left).getSimpleName(), Collections.singletonList(pv), Engine.RouteOpcode.SelectIN);
        }
        if (left instanceof SQLListExpr) {
            List<SQLExpr> right = inListExpr.getTargetList();
            return planCompositeInOpRecursive(ctx, inListExpr, (SQLListExpr) left, right, new ArrayList<>());
        }
        throw new SQLFeatureNotSupportedException();
    }

    private boolean planCompositeInOpRecursive(PlanningContext ctx, SQLInListExpr cmp, SQLListExpr left, List<SQLExpr> right, List<Integer> coordinates) throws SQLException {
        boolean foundVindex = false;
        int cindex = coordinates.size();
        coordinates.add(0);
        for (int i = 0; i < left.getItems().size(); i++) {
            SQLExpr expr = left.getItems().get(i);
            coordinates.set(cindex, i);
            if (expr instanceof SQLListExpr) {
                boolean ok = this.planCompositeInOpRecursive(ctx, cmp, (SQLListExpr) expr, right, coordinates);
                return ok || foundVindex;
            } else if (expr instanceof SQLName) {
                // check if left col is a vindex
                if (!this.hasVindex((SQLName) expr)) {
                    continue;
                }


                List<SQLExpr> rightVals = new ArrayList<>();

                for (SQLExpr currRight : right) {
                    if (currRight instanceof SQLListExpr) {
                        SQLExpr val = tupleAccess(currRight, coordinates);
                        if (val == null) {
                            return false;
                        }
                        rightVals.add(val);
                    } else {
                        return false;
                    }
                }

                EvalEngine.Expr evalEngineExpr = makeInListEvalEngineExpr(ctx, rightVals);
                if (evalEngineExpr == null) {
                    return false;
                }
                boolean newVindex = this.haveMatchingVindex(ctx, cmp, null, ((SQLName) expr).getSimpleName(), Collections.singletonList(evalEngineExpr), Engine.RouteOpcode.SelectIN);
                foundVindex = newVindex || foundVindex;
            }
        }
        return foundVindex;
    }

    private static SQLExpr tupleAccess(SQLExpr expr, List<Integer> coordinates) {
        SQLListExpr tuple = (SQLListExpr) expr;
        for (int idx : coordinates) {
            if (tuple == null || idx >= tuple.getItems().size()) {
                return null;
            }

            expr = tuple.getItems().get(idx);

            if (expr instanceof SQLListExpr) {
                tuple = (SQLListExpr) expr;
            } else {
                tuple = null;
            }
        }
        return expr;
    }


    private boolean isImpossibleNotIN(SQLInListExpr inListExpr) {
        List<SQLExpr> right = inListExpr.getTargetList();
        for (SQLExpr sqlExpr : right) {
            if (sqlExpr instanceof SQLNullExpr) {
                this.setSelectNoneOpcode();
                return true;
            }
        }
        return false;
    }

    private boolean planIsExpr(PlanningContext ctx, SQLBinaryOpExpr binaryOpExpr) throws SQLException {
        // we only handle IS NULL correct. IsExpr can contain other expressions as well
        if (!(binaryOpExpr.getRight() instanceof SQLNullExpr)) {
            return false;
        }
        if (!(binaryOpExpr.getLeft() instanceof SQLName)) {
            return false;
        }
        SQLNullExpr sqlNullExpr = new SQLNullExpr();
        EvalEngine.Expr val = makeEvalEngineExpr(ctx, sqlNullExpr);
        String columnName = ((SQLName) binaryOpExpr.getLeft()).getSimpleName();
        Engine.RouteOpcode routeOpcode = Engine.RouteOpcode.SelectScatter;
        for (VindexPlusPredicates v : this.getVindexPreds()) {
            if (SQLUtils.nameEquals(v.getColVindex().getColumn(), columnName)) {
                routeOpcode = Engine.RouteOpcode.SelectEqualUnique;
                break;
            }
        }
        return haveMatchingVindex(ctx, binaryOpExpr, sqlNullExpr, columnName, Collections.singletonList(val), routeOpcode);
    }

    private boolean hasVindex(SQLName column) {
        for (VindexPlusPredicates v : this.getVindexPreds()) {
            if (SQLUtils.nameEquals(v.getColVindex().getColumn(), column.getSimpleName())) {
                return true;
            }
        }
        return false;
    }

    private boolean haveMatchingVindex(PlanningContext ctx, SQLExpr node, SQLExpr valueExpr, String columnName, List<EvalEngine.Expr> vtPlanValues, Engine.RouteOpcode routeOpcode) {
        boolean newVindexFound = false;
        for (VindexPlusPredicates v : this.getVindexPreds()) {
//            if !ctx.SemTable.DirectDeps(column).IsSolvedBy(v.TableID) {
//                continue
//            }
            if (SQLUtils.nameEquals(v.getColVindex().getColumn(), columnName)) {
                // single column vindex - just add the option
                VindexOption vOption = new VindexOption();
                vOption.setValues(vtPlanValues);
                vOption.setValueExprs(Lists.newArrayList(valueExpr));
                vOption.setPredicates(Lists.newArrayList(node));
                vOption.setOpCode(routeOpcode);
                vOption.setFoundVindex(new BinaryHash());
                vOption.setCost(costFor(v.getColVindex(), routeOpcode));
                vOption.setReady(true);

                v.getOptions().add(vOption);
                newVindexFound = true;
            }
        }
        return newVindexFound;
    }

    private void setSelectNoneOpcode() {
        this.setRouterOpCode(Engine.RouteOpcode.SelectNone);
        // clear any chosen vindex as this query does not need to be sent down.
        this.selected = null;
    }

    // PickBestAvailableVindex goes over the available vindexes for this route and picks the best one available.
    public void pickBestAvailableVindex() {
        for (VindexPlusPredicates vindexPredicates : this.getVindexPreds()) {
            VindexOption option = vindexPredicates.bestOption();
            if (option != null) {
                this.selected = option;
                this.routerOpCode = option.getOpCode();
            }
        }
    }

    // costFor returns a cost struct to make route choices easier to compare
    private Cost costFor(Vschema.ColumnVindex colVindex, Engine.RouteOpcode opcode) {
        Cost cost = new Cost();
        switch (opcode) {
            case SelectUnsharded:
            case SelectNext:
            case SelectDBA:
            case SelectReference:
            case SelectNone:
            case SelectScatter:
                cost.setOpCode(opcode);
                return cost;
        }

        // foundVindex.Cost() always 1;
        // foundVindex.IsUnique() always true;
        cost.setVindexCost(1);
        cost.setUnique(true);

        cost.setOpCode(opcode);
        return cost;
    }

    @Getter
    @AllArgsConstructor
    static class planComparisonRetrun {
        private boolean found;

        private boolean exitEarly;
    }
}
