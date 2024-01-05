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

package com.jd.jdbc.engine.gen4;

import com.jd.jdbc.IExecute;
import com.jd.jdbc.common.tuple.Pair;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.srvtopo.BindVariable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;

/**
 * OrderedAggregate is a primitive that expects the underlying primitive
 * to feed results in an order sorted by the Keys. Rows with duplicate
 * keys are aggregated using the Aggregate functions. The assumption
 * is that the underlying primitive is a scatter select with pre-sorted
 * rows.
 */
public class OrderedAggregateGen4Engine extends AbstractAggregateGen4 {
    /**
     * GroupByKeys specifies the input values that must be used for
     * the aggregation key.
     */
    @Getter
    private List<GroupByParams> groupByKeys;

    public OrderedAggregateGen4Engine(boolean preProcess, List<AggregateParams> aggregates, boolean aggrOnEngine, int truncateColumnCount, List<GroupByParams> groupByKeys,
                                      Map<Integer, Integer> collations, PrimitiveEngine input) {
        super.preProcess = preProcess;
        super.aggregates = aggregates;
        super.aggrOnEngine = aggrOnEngine;
        super.truncateColumnCount = truncateColumnCount;
        super.collations = collations;
        super.input = input;
        this.groupByKeys = groupByKeys;
    }

    @Override
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        IExecute.ExecuteMultiShardResponse resultResponse = this.input.execute(ctx, vcursor, bindVariableMap, wantFields);
        VtResultSet result = (VtResultSet) resultResponse.getVtRowList();

        VtResultSet out = new VtResultSet(convertFields(result.getFields(), super.preProcess, super.aggregates, super.aggrOnEngine), new ArrayList<>());

        // This code is similar to the one in StreamExecute.
        List<VtResultValue> current = null;
        List<VtResultValue> curDistincts = null;
        for (List<VtResultValue> row : result.getRows()) {
            if (current == null) {
                Pair<List<VtResultValue>, List<VtResultValue>> pair = convertRow(row, super.preProcess, super.aggregates, super.aggrOnEngine);
                current = pair.getLeft();
                curDistincts = pair.getRight();
                continue;
            }
            boolean equal = this.keysEqual(current, row);
            if (equal) {
                Pair<List<VtResultValue>, List<VtResultValue>> pair = merge(result.getFields(), current, row, curDistincts, super.collations, super.aggregates);
                current = pair.getLeft();
                curDistincts = pair.getRight();
                continue;
            }
            out.getRows().add(current);
            Pair<List<VtResultValue>, List<VtResultValue>> pair = convertRow(row, super.preProcess, super.aggregates, super.aggrOnEngine);
            current = pair.getLeft();
            curDistincts = pair.getRight();
        }
        if (current != null) {
            List<VtResultValue> finalValues = convertFinal(current, super.aggregates);
            out.getRows().add(finalValues);
        }
        return new IExecute.ExecuteMultiShardResponse(out.truncate(super.truncateColumnCount));
    }

    private boolean keysEqual(List<VtResultValue> row1, List<VtResultValue> row2) throws SQLException {
        for (GroupByParams key : this.groupByKeys) {
            Integer cmp = EvalEngine.nullSafeCompare(row1.get(key.getKeyCol()), row2.get(key.getKeyCol()));
            if (cmp != 0) {
                return false;
            }
        }
        return true;
    }
}
