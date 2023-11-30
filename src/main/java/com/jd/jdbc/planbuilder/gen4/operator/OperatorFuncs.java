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

import com.jd.jdbc.common.tuple.Pair;
import com.jd.jdbc.common.tuple.Triple;
import com.jd.jdbc.context.PlanningContext;
import com.jd.jdbc.planbuilder.gen4.operator.physical.ApplyJoin;
import com.jd.jdbc.planbuilder.gen4.operator.physical.Filter;
import com.jd.jdbc.planbuilder.gen4.operator.physical.PhysicalOperator;
import com.jd.jdbc.planbuilder.gen4.operator.physical.Route;
import com.jd.jdbc.planbuilder.gen4.operator.physical.Table;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.SqlParser;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator;
import com.jd.jdbc.sqlparser.visitor.VtBreakExpressionInLHSandRHSVisitor;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OperatorFuncs {

    /**
     * PushPredicate is used to push predicates. It pushed it as far down as is possible in the tree.
     * If we encounter a join and the predicate depends on both sides of the join, the predicate will be split into two parts,
     * where data is fetched from the LHS of the join to be used in the evaluation on the RHS
     *
     * @param ctx
     * @param expr
     * @param op
     * @return
     */
    public static PhysicalOperator pushPredicate(PlanningContext ctx, SQLExpr expr, PhysicalOperator op) throws SQLException {
        if (op instanceof Route) {
            ((Route) op).updateRoutingLogic(ctx, expr);
            PhysicalOperator newSrc = pushPredicate(ctx, expr, ((Route) op).getSource());
            ((Route) op).setSource(newSrc);
            return op;
        }

        if (op instanceof ApplyJoin) {
            TableSet deps = ctx.getSemTable().recursiveDeps(expr);
            if (deps.isSolvedBy(((ApplyJoin) op).getLHS().tableID())) {
                PhysicalOperator newSrc = pushPredicate(ctx, expr, ((ApplyJoin) op).getLHS());
                ((ApplyJoin) op).setLHS(newSrc);
                return op;
            }

            if (deps.isSolvedBy(((ApplyJoin) op).getRHS().tableID())) {
                if (!((ApplyJoin) op).getLeftJoin()) {
                    PhysicalOperator newSrc = pushPredicate(ctx, expr, ((ApplyJoin) op).getRHS());
                    ((ApplyJoin) op).setRHS(newSrc);
                    return op;
                }

                // we are looking for predicates like `tbl.col = <>` or `<> = tbl.col`,
                // where tbl is on the rhs of the left outer join

                if (SqlParser.isComparison(expr)
                    && ((SQLBinaryOpExpr) expr).getOperator() != SQLBinaryOperator.LessThanOrEqualOrGreaterThan) {
                    SQLExpr left = ((SQLBinaryOpExpr) expr).getLeft();
                    SQLExpr right = ((SQLBinaryOpExpr) expr).getRight();
                    if (
                        (left instanceof SQLName)
                            && ctx.getSemTable().recursiveDeps(left).isSolvedBy(((ApplyJoin) op).getRHS().tableID())
                            || (right instanceof SQLName)
                            && ctx.getSemTable().recursiveDeps(right).isSolvedBy(((ApplyJoin) op).getRHS().tableID())
                    ) {
                        // When the predicate we are pushing is using information from an outer table, we can
                        // check whether the predicate is "null-intolerant" or not. Null-intolerant in this context means that
                        // the predicate will not return true if the table columns are null.
                        // Since an outer join is an inner join with the addition of all the rows from the left-hand side that
                        // matched no rows on the right-hand, if we are later going to remove all the rows where the right-hand
                        // side did not match, we might as well turn the join into an inner join.

                        // This is based on the paper "Canonical Abstraction for Outerjoin Optimization" by J Rao et al

                        ((ApplyJoin) op).setLeftJoin(false);
                        PhysicalOperator newSrc = pushPredicate(ctx, expr, ((ApplyJoin) op).getRHS());
                        ((ApplyJoin) op).setRHS(newSrc);
                        return op;
                    }
                }
                // finally, if we can't turn the outer join into an inner,
                // we need to filter after the join has been evaluated
                return new Filter(op, new ArrayList<>(Arrays.asList(expr)));
            }

            if (deps.isSolvedBy(op.tableID())) {
                // breakExpressionInLHSandRHS
                Triple<List<String>, List<SQLName>, SQLExpr> tripleRet = breakExpressioninLHSandRHS(ctx, expr, ((ApplyJoin) op).getLHS().tableID());

                Pair<PhysicalOperator, List<Integer>> pairRet = pushOutputColumns(ctx, ((ApplyJoin) op).getLHS(), tripleRet.getMiddle());

                ((ApplyJoin) op).setLHS(pairRet.getLeft());

                for (int i = 0; i < pairRet.getRight().size(); i++) {
                    String bvName = tripleRet.getLeft().get(i);
                    int idx = pairRet.getRight().get(i);
                    ((ApplyJoin) op).getVars().put(bvName, idx);
                }

                PhysicalOperator newSrc = pushPredicate(ctx, tripleRet.getRight(), ((ApplyJoin) op).getRHS());
                ((ApplyJoin) op).setRHS(newSrc);

                ((ApplyJoin) op).setPredicate(SQLBinaryOpExpr.and(((ApplyJoin) op).getPredicate(), expr));
                return op;
            }

            throw new SQLException("Cannot push predicate: " + expr.toString());
        }

        if (op instanceof Table) {
            return new Filter(op, new ArrayList<>(Arrays.asList(expr)));
        }

        if (op instanceof Filter) {
            ((Filter) op).getPredicates().add(expr);
            return op;
        }
        // TODO derived

        throw new SQLException("we cannot push predicates into " + op.getClass().getName());
    }

    /**
     * BreakExpressionInLHSandRHS takes an expression and
     * extracts the parts that are coming from one of the sides into `ColName`s that are needed
     *
     * @param ctx
     * @param expr
     * @param lhs
     * @return
     */
    public static Triple<List<String>, List<SQLName>, SQLExpr> breakExpressioninLHSandRHS(
        PlanningContext ctx, SQLExpr expr, TableSet lhs) throws SQLException {
        SQLExpr rewrittenExpr = expr.clone();

        VtBreakExpressionInLHSandRHSVisitor visitor = new VtBreakExpressionInLHSandRHSVisitor(lhs, ctx.getSemTable());
        rewrittenExpr.accept(visitor);
        if (visitor.getBErr()) {
            throw new SQLException(visitor.getErrMsg());
        }
        List<SQLExpr> joinPredicates = ctx.getJoinPredicates().get(expr);
        if (joinPredicates != null) {
            joinPredicates.add(rewrittenExpr);
        }
        return Triple.of(visitor.getBvNames(), visitor.getColumus(), rewrittenExpr);
    }

    /**
     * PushOutputColumns will push the columns to the table they originate from,
     * making sure that intermediate operators pass the data through
     *
     * @param ctx
     * @param op
     * @param columns
     * @return
     */
    public static Pair<PhysicalOperator, List<Integer>> pushOutputColumns(PlanningContext ctx, PhysicalOperator op, List<SQLName> columns) throws SQLException {
        if (op instanceof Route) {
            Pair<PhysicalOperator, List<Integer>> ret = pushOutputColumns(ctx, ((Route) op).getSource(), columns);
            ((Route) op).setSource(ret.getLeft());
            return Pair.of(op, ret.getRight());
        }

        if (op instanceof ApplyJoin) {
            List<Boolean> toTheLeft = new ArrayList<>();
            List<SQLName> lhs = new ArrayList<>();
            List<SQLName> rhs = new ArrayList<>();

            for (SQLName col : columns) {
                if (ctx.getSemTable().recursiveDeps(col).isSolvedBy(((ApplyJoin) op).getLHS().tableID())) {
                    lhs.add(col);
                    toTheLeft.add(true);
                } else {
                    rhs.add(col);
                    toTheLeft.add(false);
                }
            }
            Pair<PhysicalOperator, List<Integer>> retLeft = pushOutputColumns(ctx, ((ApplyJoin) op).getLHS(), lhs);
            ((ApplyJoin) op).setLHS(retLeft.getLeft());

            Pair<PhysicalOperator, List<Integer>> retRight = pushOutputColumns(ctx, ((ApplyJoin) op).getRHS(), rhs);
            ((ApplyJoin) op).setRHS(retRight.getLeft());

            List<Integer> outputColumns = new ArrayList<>(toTheLeft.size());

            int l = 0, r = 0;
            for (int i = 0; i < toTheLeft.size(); i++) {
                outputColumns.add(((ApplyJoin) op).getColumns().size());
                if (toTheLeft.get(i)) {
                    ((ApplyJoin) op).getColumns().add(0 - retLeft.getRight().get(l) - 1);
                    l++;
                } else {
                    ((ApplyJoin) op).getColumns().add(retRight.getRight().get(r) + 1);
                    r++;
                }
            }
            return Pair.of(op, outputColumns);
        }

        if (op instanceof Table) {
            List<Integer> offsets = new ArrayList<>();
            for (SQLName col : columns) {
                boolean exists = false;
                for (int idx = 0; idx < ((Table) op).getColumns().size(); idx++) {
                    if (SQLUtils.nameEquals(col, ((Table) op).getColumns().get(idx))) {
                        exists = true;
                        offsets.add(idx);
                    }
                }
                if (!exists) {
                    offsets.add(((Table) op).getColumns().size());
                    ((Table) op).getColumns().add(col);
                }
            }
            return Pair.of(op, offsets);
        }

        if (op instanceof Filter) {
            Pair<PhysicalOperator, List<Integer>> ret = pushOutputColumns(ctx, ((Filter) op).getSource(), columns);
            ((Filter) op).setSource(ret.getLeft());
            return Pair.of(op, ret.getRight());
        }

        //TODO vindex derived

        throw new SQLException("we cannot push output columns into" + op.getClass().getName());
    }
}
