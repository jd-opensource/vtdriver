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

import com.jd.jdbc.common.tuple.Pair;
import com.jd.jdbc.common.tuple.Triple;
import com.jd.jdbc.context.PlanningContext;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.gen4.AbstractAggregateGen4;
import com.jd.jdbc.engine.gen4.GroupByParams;
import com.jd.jdbc.planbuilder.gen4.logical.DistinctGen4Plan;
import com.jd.jdbc.planbuilder.gen4.logical.FilterGen4Plan;
import com.jd.jdbc.planbuilder.gen4.logical.JoinGen4Plan;
import com.jd.jdbc.planbuilder.gen4.logical.LimitGen4Plan;
import com.jd.jdbc.planbuilder.gen4.logical.LogicalPlan;
import com.jd.jdbc.planbuilder.gen4.logical.OrderedAggregateGen4Plan;
import com.jd.jdbc.planbuilder.gen4.logical.RouteGen4Plan;
import com.jd.jdbc.planbuilder.gen4.logical.SimpleProjectionGen4Plan;
import com.jd.jdbc.planbuilder.gen4.operator.OperatorFuncs;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.expr.SQLExprUtils;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRemoveDbNameVisitor;
import com.jd.jdbc.util.SelectItemUtil;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

public class ProjectionPushing {

    public static Pair<Integer, Integer> pushProjection(PlanningContext ctx, SQLSelectItem expr, LogicalPlan plan, boolean inner, boolean reuseCol, boolean hasAggregation) throws SQLException {
        if (plan instanceof RouteGen4Plan) {
            return addExpressionToRoute(ctx, (RouteGen4Plan) plan, expr, reuseCol);
        } else if (plan instanceof SimpleProjectionGen4Plan) {
            return pushProjectionIntoSimpleProj(ctx, expr, (SimpleProjectionGen4Plan) plan, inner, hasAggregation, reuseCol);
        } else if (plan instanceof FilterGen4Plan || plan instanceof LimitGen4Plan || plan instanceof DistinctGen4Plan) {
            LogicalPlan src = plan.inputs()[0];
            return pushProjection(ctx, expr, src, inner, reuseCol, hasAggregation);
        } else if (plan instanceof OrderedAggregateGen4Plan) {
            return pushProjectionIntoOA(ctx, expr, (OrderedAggregateGen4Plan) plan, inner, hasAggregation);
        } else if (plan instanceof JoinGen4Plan) {
            return pushProjectionIntoJoin(ctx, expr, (JoinGen4Plan) plan, reuseCol, inner, hasAggregation);
        } else {
            throw new SQLFeatureNotSupportedException();
        }
    }

    private static Pair<Integer, Integer> addExpressionToRoute(PlanningContext ctx, RouteGen4Plan rb, SQLSelectItem expr, boolean reuseCol) throws SQLException {
        if (reuseCol) {
            int getOffset = HorizonPlanning.checkIfAlreadyExists(expr, rb.getSelect(), ctx.getSemTable());
            if (getOffset != -1) {
                return Pair.of(getOffset, 0);
            }
        }
        // remove keyspace
        VtRemoveDbNameVisitor visitor = new VtRemoveDbNameVisitor();
        expr.accept(visitor);
        if (!(rb.select instanceof MySqlSelectQueryBlock)) {
            // error
            throw new SQLException("unsupported: pushing projection " + expr.toString());
        }

        if (ctx.isRewriteDerivedExpr()) {

        }

        int offset = ((MySqlSelectQueryBlock) rb.select).getSelectList().size();
        ((MySqlSelectQueryBlock) rb.select).addSelectItem(expr);

        return Pair.of(offset, 1);
    }

    private static Pair<Integer, Integer> pushProjectionIntoSimpleProj(PlanningContext ctx, SQLSelectItem expr, SimpleProjectionGen4Plan node,
                                                                       boolean inner, boolean hasAggregation, boolean reuseCol) throws SQLException {
        Pair<Integer, Integer> ret = pushProjection(ctx, expr, node.getInput(), inner, true, hasAggregation);
        int offset = ret.getLeft();
        for (int i = 0; i < node.getESimpleProjection().getCols().size(); i++) {
            // we return early if we already have the column in the simple projection's
            // output list so we do not add it again.
            if (reuseCol && node.getESimpleProjection().getCols().get(i).equals(offset)) {
                return Pair.of(i, 0);
            }
        }
        node.getESimpleProjection().getCols().add(offset);
        return Pair.of(node.getESimpleProjection().getCols().size() - 1, 1);
    }

    private static Pair<Integer, Integer> pushProjectionIntoOA(PlanningContext ctx, SQLSelectItem expr, OrderedAggregateGen4Plan node,
                                                               boolean inner, boolean hasAggregation) throws SQLException {
        boolean isColName = false;
        if (expr.getExpr() instanceof SQLName) {
            isColName = true;
        }
        for (AbstractAggregateGen4.AggregateParams aggregate : node.getAggregates()) {
            if (SQLExprUtils.equals(aggregate.getExpr(), expr.getExpr())) {
                return Pair.of(aggregate.getCol(), 0);
            }
            if (isColName && ((SQLName) expr.getExpr()).getSimpleName().equals(aggregate.getAlias())) {
                return Pair.of(aggregate.getCol(), 0);
            }
        }
        for (GroupByParams key : node.getGroupByKeys()) {
            if (SQLExprUtils.equals(key.getExpr(), expr.getExpr())) {
                return Pair.of(key.getKeyCol(), 0);
            }
        }
        Pair<Integer, Integer> ret = pushProjection(ctx, expr, node.getInput(), inner, true, hasAggregation);
        int offset = ret.getLeft();
        AbstractAggregateGen4.AggregateParams addAggregateParams = new AbstractAggregateGen4.AggregateParams(Engine.AggregateOpcodeG4.AggregateRandom, offset, SelectItemUtil.columnName(expr));
        addAggregateParams.setExpr(expr.getExpr());
        addAggregateParams.setOriginal(expr);
        node.getAggregates().add(addAggregateParams);
        return Pair.of(offset, 1);
    }

    private static Pair<Integer, Integer> pushProjectionIntoJoin(PlanningContext ctx, SQLSelectItem expr, JoinGen4Plan node,
                                                                 boolean reuseCol, boolean inner, boolean hasAggregation) throws SQLException {
        TableSet lhsSolves = node.getLeft().containsTables();
        TableSet rhsSolves = node.getRight().containsTables();
        TableSet deps = ctx.getSemTable().recursiveDeps(expr.getExpr());

        int column;
        int appended;

        boolean passDownReuseCol = reuseCol ? reuseCol : (expr.getAlias() == null || expr.getAlias().isEmpty());

        if (deps.isSolvedBy(lhsSolves)) {
            Pair<Integer, Integer> result = pushProjection(ctx, expr, node.getLeft(), inner, passDownReuseCol, hasAggregation);
            column = -(result.getLeft() + 1);
            appended = result.getRight();
        } else if (deps.isSolvedBy(rhsSolves)) {
            boolean newInner = inner && node.getOpcode() != Engine.JoinOpcode.LeftJoin;
            Pair<Integer, Integer> result = pushProjection(ctx, expr, node.getRight(), newInner, passDownReuseCol, hasAggregation);
            column = result.getLeft() + 1;
            appended = result.getRight();
        } else {
            // if an expression has aggregation, then it should not be split up and pushed to both sides,
            // for example an expression like count(*) will have dependencies on both sides, but we should not push it
            // instead we should return an error
            if (hasAggregation) {
                throw new SQLException("unsupported: cross-shard query with aggregates");
            }
            // now we break the expression into left and right side dependencies and rewrite the left ones to bind variables
            Triple<List<String>, List<SQLName>, SQLExpr> result = OperatorFuncs.breakExpressioninLHSandRHS(ctx, expr.getExpr(), lhsSolves);
            List<String> bvName = result.getLeft();
            List<SQLName> cols = result.getMiddle();
            SQLExpr rewrittenExpr = result.getRight();
            // go over all the columns coming from the left side of the tree and push them down. While at it, also update the bind variable map.
            // It is okay to reuse the columns on the left side since
            // the final expression which will be selected will be pushed into the right side.
            for (int i = 0; i < cols.size(); i++) {
                Pair<Integer, Integer> tmpResult = pushProjection(ctx, new SQLSelectItem(cols.get(i)), node.getLeft(), inner, true, false);
                node.getVars().put(bvName.get(i), tmpResult.getLeft());
            }
            // push the rewritten expression on the right side of the tree. Here we should take care whether we want to reuse the expression or not.
            expr.setExpr(rewrittenExpr);
            boolean newInner = inner && node.getOpcode() != Engine.JoinOpcode.LeftJoin;
            Pair<Integer, Integer> tmpResult = pushProjection(ctx, expr, node.getRight(), newInner, passDownReuseCol, false);
            column = tmpResult.getLeft() + 1;
            appended = tmpResult.getRight();
        }
        if (reuseCol && (appended == 0)) {
            for (int idx = 0; idx < node.getCols().size(); idx++) {
                int col = node.getCols().get(idx);
                if (column == col) {
                    return Pair.of(idx, 0);
                }
            }
            // the column was not appended to either child, but we could not find it in out cols list,
            // so we'll still add it
        }
        node.getCols().add(column);
        return Pair.of(node.getCols().size() - 1, 1);

    }
}
