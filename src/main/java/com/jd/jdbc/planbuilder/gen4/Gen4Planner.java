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

import com.jd.jdbc.VSchemaManager;
import com.jd.jdbc.context.PlanningContext;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.planbuilder.PlanBuilder;
import com.jd.jdbc.planbuilder.gen4.logical.LogicalPlan;
import com.jd.jdbc.planbuilder.gen4.logical.RouteGen4Plan;
import com.jd.jdbc.planbuilder.gen4.operator.OperatorTransformers;
import com.jd.jdbc.planbuilder.gen4.operator.OperatorUtil;
import com.jd.jdbc.planbuilder.gen4.operator.logical.LogicalOperator;
import com.jd.jdbc.planbuilder.gen4.operator.physical.PhysicalOperator;
import com.jd.jdbc.planbuilder.gen4.operator.physical.RoutePlanning;
import com.jd.jdbc.planbuilder.semantics.Analyzer;
import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.planbuilder.semantics.VSchema;
import com.jd.jdbc.sqlparser.ast.SQLLimit;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQuery;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import java.sql.SQLException;

public class Gen4Planner {

    public static PrimitiveEngine gen4SelectStmtPlanner(String query, String defaultKeyspace, SQLSelectStatement stmt, Object reservedVars, VSchemaManager vschema) throws SQLException {
        if (stmt.getSelect().getQuery() instanceof MySqlSelectQueryBlock) {
            // handle dual table for processing at vtgate.
            PrimitiveEngine p = PlanBuilder.handleDualSelects(stmt.getSelect().getQuery(), vschema, defaultKeyspace);
            boolean calcFoundRows = ((MySqlSelectQueryBlock) stmt.getSelect().getQuery()).isCalcFoundRows();
            if (p != null) {
                return p;
            }
        }

        VSchema vSchema = new VSchema(defaultKeyspace, vschema);
        LogicalPlan plan = newBuildSelectPlan(stmt, reservedVars, vSchema, null);

        return plan.getPrimitiveEngine();
    }

    private static LogicalPlan newBuildSelectPlan(SQLSelectStatement selStmt, Object reservedVars, VSchema vschema, Object version) throws SQLException {
        String ksName = vschema.getDefaultKeyspace();
        SemTable semTable = Analyzer.analyze(selStmt, ksName, vschema);

        // record any warning as planner warning.
        vschema.plannerWarning(semTable.getWarning());

        PlanningContext ctx = new PlanningContext(reservedVars, semTable, vschema, vschema.getVschemaKeyspace());

        if (!vschema.getVschemaKeyspace().getSharded() && semTable.singleUnshardedKeyspace() != null) {
            return SingleShardedShortcut.unshardedShortcut(ctx, selStmt.getSelect().getQuery(), vschema.getVschemaKeyspace());
        }

        Rewriter.queryRewrite(semTable, reservedVars, selStmt);


        LogicalOperator logical = OperatorUtil.createLogicalOperatorFromAST(selStmt, semTable);
        logical.checkValid();

        PhysicalOperator physOp = RoutePlanning.createPhysicalOperator(ctx, logical);

        LogicalPlan plan = OperatorTransformers.transformToLogicalPlan(ctx, physOp, true);

        plan = planHorizon(ctx, plan, selStmt.getSelect().getQuery(), true);

        plan.wireupGen4(ctx);

        plan = pushCommentDirectivesOnPlan(plan, selStmt);

        return plan;
    }

    public static LogicalPlan planHorizon(PlanningContext ctx, LogicalPlan plan, SQLSelectQuery in, boolean truncateColumns) throws SQLException {
        if (in instanceof MySqlSelectQueryBlock) {
            MySqlSelectQueryBlock selectQueryBlock = (MySqlSelectQueryBlock) in;
            HorizonPlanning hp = new HorizonPlanning(in);
//            replaceSubQuery(ctx, node)
            LogicalPlan horizonPlan = hp.planHorizon(ctx, plan, truncateColumns);
            return planLimit(selectQueryBlock.getLimit(), horizonPlan);
        }
        if (in instanceof SQLUnionQuery) {
            if (!(plan instanceof RouteGen4Plan) && ctx.getSemTable().getNotSingleRouteErr() != null) {
                throw new SQLException(ctx.getSemTable().getNotSingleRouteErr());
            }
            if (plan instanceof RouteGen4Plan && ((RouteGen4Plan) plan).isSingleShard()) {
                HorizonPlanning.planSingleShardRoutePlan(in, (RouteGen4Plan) plan);
            } else {
                plan = planOrderByOnUnion(ctx, plan, in);
            }

            return planLimit(((SQLUnionQuery) in).getLimit(), plan);
        }
        return plan;
    }

    private static LogicalPlan planOrderByOnUnion(PlanningContext ctx, LogicalPlan plan, SQLSelectQuery union) throws SQLException {
        QueryProjection qp = QueryProjection.createQPFromUnion(union);
        HorizonPlanning hp = new HorizonPlanning(qp);
        if (qp.getOrderExprs().size() > 0) {
            plan = hp.planOrderBy(ctx, qp.getOrderExprs(), plan);
        }
        return plan;
    }

    private static LogicalPlan pushCommentDirectivesOnPlan(LogicalPlan plan, SQLSelectStatement stmt) {
        return plan;
    }

    private static LogicalPlan planLimit(SQLLimit limit, LogicalPlan plan) throws SQLException {
        if (limit == null) {
            return plan;
        }

        if (plan instanceof RouteGen4Plan) {
            RouteGen4Plan planGen4 = (RouteGen4Plan) plan;
            if (planGen4.isSingleShard()) {
                planGen4.setLimit(limit);
                return plan;
            }
        }
        LogicalPlan lPlan = PostProcess.createLimit(plan, limit);
        // visit does not modify the plan.
        LogicalPlan.visit(lPlan, new PostProcess.SetUpperLimit());
        return lPlan;
    }
}
