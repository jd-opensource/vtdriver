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

package com.jd.jdbc.planbuilder.gen4.logical;

import com.jd.jdbc.common.util.CollectionUtils;
import com.jd.jdbc.context.PlanningContext;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.gen4.AbstractAggregateGen4;
import com.jd.jdbc.engine.gen4.GroupByParams;
import com.jd.jdbc.engine.gen4.OrderedAggregateGen4Engine;
import com.jd.jdbc.engine.gen4.ScalarAggregateGen4Engine;
import com.jd.jdbc.planbuilder.Truncater;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OrderedAggregateGen4Plan extends ResultsBuilder implements LogicalPlan, Truncater {

    private SQLName extraDistinct;

    /**
     * preProcess is true if one of the aggregates needs preprocessing.
     */
    private boolean preProcess;

    private boolean aggrOnEngine;

    /**
     * aggregates specifies the aggregation parameters for each
     * aggregation function: function opcode and input column number.
     */
    private List<AbstractAggregateGen4.AggregateParams> aggregates = new ArrayList<>();

    /**
     * groupByKeys specifies the input values that must be used for
     * the aggregation key.
     */
    private List<GroupByParams> groupByKeys = new ArrayList<>();

    private int truncateColumnCount;

    public OrderedAggregateGen4Plan() {
    }

    public OrderedAggregateGen4Plan(List<GroupByParams> groupByKeys) {
        this.groupByKeys = groupByKeys;
    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        PrimitiveEngine primitiveEngine = this.getInput().getPrimitiveEngine();
        if (CollectionUtils.isEmpty(this.groupByKeys)) {
            return new ScalarAggregateGen4Engine(this.preProcess, aggregates, this.aggrOnEngine, this.truncateColumnCount, null, primitiveEngine);
        }

        return new OrderedAggregateGen4Engine(this.preProcess, this.aggregates, this.aggrOnEngine, this.truncateColumnCount, this.groupByKeys, null, primitiveEngine);
    }


    @Override
    public void wireupGen4(PlanningContext ctx) throws SQLException {
        this.getInput().wireupGen4(ctx);
    }

    @Override
    public List<SQLSelectItem> outputColumns() throws SQLException {
        List<SQLSelectItem> outputCols = new ArrayList<>();
        // creates a deep clone of the outputColumns.
        for (SQLSelectItem selectItem : super.input.outputColumns()) {
            outputCols.add(selectItem.clone());
        }

        for (AbstractAggregateGen4.AggregateParams aggr : this.aggregates) {
            outputCols.set(aggr.getCol(), new SQLSelectItem(aggr.getExpr(), aggr.getAlias()));
        }
        if (this.truncateColumnCount > 0) {
            return outputCols.subList(0, this.truncateColumnCount);
        }
        return outputCols;
    }

    @Override
    public void setUpperLimit(SQLExpr count) throws SQLException {

    }

    @Override
    public void setTruncateColumnCount(Integer count) {
        this.truncateColumnCount = count;
    }
}
