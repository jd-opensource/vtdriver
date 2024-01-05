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
import com.jd.jdbc.planbuilder.gen4.logical.JoinGen4Plan;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.ast.expr.SQLAggregateExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLAllColumnExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class AggregationPushing {

    @FunctionalInterface
    interface Func {
        Pair<List<Offsets>, List<List<Offsets>>> passThrough(List<Offsets> groupByOffsets, List<List<Offsets>> aggrOffsets);
    }

    public static Pair<List<QueryProjection.Aggr>, List<QueryProjection.Aggr>> splitAggregationsToLeftAndRight(
        PlanningContext ctx, List<QueryProjection.Aggr> aggregations, JoinGen4Plan join) throws SQLException {

        List<QueryProjection.Aggr> lhsAggrs = new ArrayList<>();
        List<QueryProjection.Aggr> rhsAggrs = new ArrayList<>();

        for (QueryProjection.Aggr aggr : aggregations) {
            boolean foundCountStar = false;
            if (aggr.getOriginal().getExpr() instanceof SQLAggregateExpr) {  // CountStar
                SQLAggregateExpr aggrExpr = (SQLAggregateExpr) aggr.getOriginal().getExpr();
                Engine.AggregateOpcodeG4 opcode = AbstractAggregateGen4.SUPPORTED_AGGREGATES.get(aggrExpr.getMethodName().toLowerCase());
                if (opcode == Engine.AggregateOpcodeG4.AggregateCountStar) { // countstar count(*) ??
                    lhsAggrs.add(aggr);
                    rhsAggrs.add(aggr);
                    foundCountStar = true;
                }
            }

            if (!foundCountStar) {
                TableSet deps = ctx.getSemTable().recursiveDeps(aggr.getOriginal().getExpr());
                // if we are sending down min/max, we don't have to multiply the results with anything
                QueryProjection.Aggr other = null;
                if (aggr.getOpCode() != Engine.AggregateOpcodeG4.AggregateMax || aggr.getOpCode() != Engine.AggregateOpcodeG4.AggregateMin) {
                    // create countstar expr
                    SQLAggregateExpr countStarExpr = new SQLAggregateExpr("count");
                    countStarExpr.getArguments().add(new SQLAllColumnExpr());
                    other = new QueryProjection.Aggr(new SQLSelectItem(countStarExpr), Engine.AggregateOpcodeG4.AggregateCountStar, "count(*)", 0);
                }

                if (deps.isSolvedBy(join.getLeft().containsTables())) {
                    lhsAggrs.add(aggr);
                    rhsAggrs.add(other);
                } else if (deps.isSolvedBy(join.getRight().containsTables())) {
                    rhsAggrs.add(aggr);
                    lhsAggrs.add(other);
                } else {
                    throw new SQLException("aggregation on columns from different sources not supported yet");
                }
            }
        }
        return Pair.of(lhsAggrs, rhsAggrs);
    }

    public static Triple<List<QueryProjection.GroupBy>, List<QueryProjection.GroupBy>, List<Integer>> splitGroupingsToLeftAndRight(
        PlanningContext ctx, JoinGen4Plan join, List<QueryProjection.GroupBy> grouping, List<QueryProjection.GroupBy> lhsGrouping) throws SQLException {

        List<QueryProjection.GroupBy> rhsGrouping = new ArrayList<>();

        TableSet lhsTS = join.getLeft().containsTables();
        TableSet rhsTS = join.getRight().containsTables();
        // here we store information about which side the grouping value is coming from.
        // Negative values from the left operator and positive values are offsets into the RHS
        List<Integer> groupingOffsets = new ArrayList<>();
        for (QueryProjection.GroupBy groupBy : grouping) {
            TableSet deps = ctx.getSemTable().recursiveDeps(groupBy.getInner());
            if (deps.isSolvedBy(lhsTS)) {
                groupingOffsets.add(-(lhsGrouping.size() + 1));
                lhsGrouping.add(groupBy);
            } else if (deps.isSolvedBy(rhsTS)) {
                groupingOffsets.add(rhsGrouping.size() + 1);
                rhsGrouping.add(groupBy);
            } else {
                throw new SQLException("grouping on columns from different sources not supported yet");
            }
        }
        return Triple.of(lhsGrouping, rhsGrouping, groupingOffsets);
    }
}
