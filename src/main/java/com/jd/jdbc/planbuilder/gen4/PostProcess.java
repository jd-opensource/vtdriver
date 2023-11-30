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

import com.jd.jdbc.common.tuple.ImmutablePair;
import com.jd.jdbc.common.tuple.Pair;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.planbuilder.gen4.logical.LimitGen4Plan;
import com.jd.jdbc.planbuilder.gen4.logical.LogicalPlan;
import com.jd.jdbc.planbuilder.gen4.logical.MemorySortGen4Plan;
import com.jd.jdbc.planbuilder.gen4.logical.RouteGen4Plan;
import com.jd.jdbc.sqlparser.ast.SQLLimit;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefExpr;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import java.sql.SQLException;

public class PostProcess {

    public static LogicalPlan createLimit(LogicalPlan plan, SQLLimit limit) throws SQLException {
        LogicalPlan limitPlan = LimitGen4Plan.createLimitPlan(plan, limit);
        return limitPlan;
    }

    /**
     * setUpperLimit is an optimization hint that tells that primitive
     * that it does not need to return more than the specified number of rows.
     * A primitive that cannot perform this can ignore the request.
     */
    static class SetUpperLimit implements PlanVisitor {

        @Override
        public Pair<Boolean, LogicalPlan> func(LogicalPlan plan) throws SQLException {

            if (HorizonPlanning.isJoin(plan)) {
                return new ImmutablePair(false, plan);
            }

            if (plan instanceof RouteGen4Plan) {
                SQLVariantRefExpr upperLimitVar = new SQLVariantRefExpr();
                upperLimitVar.setName(":__upper_limit");
                upperLimitVar.setIndex(-2);

                // The route pushes the limit regardless of the plan.
                // If it's a scatter query, the rows returned will be
                // more than the upper limit, but enough for the limit
                ((MySqlSelectQueryBlock) ((RouteGen4Plan) plan).getSelect()).setLimit(new SQLLimit(upperLimitVar));
            } else if (plan instanceof MemorySortGen4Plan) {
                EvalEngine.BindVariable pv = new EvalEngine.BindVariable("__upper_limit");
                ((MemorySortGen4Plan) plan).getEMemorySort().setUpperLimit(pv);
                // we don't want to go down to the rest of the tree
                return new ImmutablePair(false, plan);
            }
            return new ImmutablePair(true, plan);
        }
    }
}
