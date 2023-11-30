/*
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
import com.jd.jdbc.engine.Engine.AggregateOpcodeG4;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.srvtopo.BindVariable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ScalarAggregate is a primitive used to do aggregations without grouping keys
 */
public class ScalarAggregateGen4Engine extends AbstractAggregateGen4 {

    public ScalarAggregateGen4Engine(boolean preProcess, List<AggregateParams> aggregates, boolean aggrOnEngine, Integer truncateColumnCount, Map<Integer, Integer> collations,
                                     PrimitiveEngine input) {
        super.preProcess = preProcess;
        super.aggregates = aggregates;
        super.aggrOnEngine = aggrOnEngine;
        super.truncateColumnCount = truncateColumnCount;
        super.collations = collations;
        super.input = input;
    }

    public ScalarAggregateGen4Engine(boolean preProcess, List<AggregateParams> aggregates, PrimitiveEngine input) {
        super.preProcess = preProcess;
        super.aggregates = aggregates;
        super.input = input;
    }

    @Override
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor cursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        IExecute.ExecuteMultiShardResponse resultResponse = this.input.execute(ctx, cursor, bindVariableMap, wantFields);
        VtResultSet queryResult = getExecuteMultiShardResponse((VtResultSet) resultResponse.getVtRowList());
        return new IExecute.ExecuteMultiShardResponse(queryResult.truncate(this.truncateColumnCount));
    }

    private VtResultSet getExecuteMultiShardResponse(VtResultSet result) throws SQLException {
        VtResultSet out = new VtResultSet(convertFields(result.getFields(), this.preProcess, this.aggregates, this.aggrOnEngine), new ArrayList<>());
        List<VtResultValue> resultRow = null;
        List<VtResultValue> curDistinct = null;
        for (List<VtResultValue> row : result.getRows()) {
            if (resultRow == null) {
                Pair<List<VtResultValue>, List<VtResultValue>> listListPair = convertRow(row, this.preProcess, this.aggregates, this.aggrOnEngine);
                resultRow = listListPair.getLeft();
                curDistinct = listListPair.getRight();
                continue;
            }
            Pair<List<VtResultValue>, List<VtResultValue>> merge = merge(result.getFields(), resultRow, row, curDistinct, collations, aggregates);
            resultRow = merge.getLeft();
            curDistinct = merge.getRight();
        }
        if (resultRow == null) {
            // When doing aggregation without grouping keys, we need to produce a single row containing zero-value for the
            // different aggregation functions
            resultRow = createEmptyRow();
        } else {
            resultRow = convertFinal(resultRow, this.aggregates);
        }
        out.getRows().add(resultRow);
        return out;
    }

    /**
     * creates the empty row for the case when we are missing grouping keys and have empty input table
     *
     * @return
     */
    private List<VtResultValue> createEmptyRow() throws SQLException {
        List<VtResultValue> out = new ArrayList<>();
        for (AggregateParams aggr : this.aggregates) {
            AggregateOpcodeG4 op = aggr.getOpcode();
            if (aggr.getOrigOpcode() != AggregateOpcodeG4.AggregateUnassigned) {
                op = aggr.getOrigOpcode();
            }
            VtResultValue value = createEmptyValueFor(op);
            out.add(value);
        }
        return out;
    }

    private VtResultValue createEmptyValueFor(AggregateOpcodeG4 opcode) throws SQLException {
        switch (opcode) {
            case AggregateCountDistinct:
            case AggregateCount:
            case AggregateCountStar:
                return COUNT_ZERO;
            case AggregateSumDistinct:
            case AggregateSum:
            case AggregateMin:
            case AggregateMax:
                return VtResultValue.NULL;
            default:
                throw new SQLException("unknown aggregation " + opcode);
        }
    }
}
