/*
Copyright 2021 JD Project Authors. Licensed under Apache-2.0.

Copyright 2019 The Vitess Authors.

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

package com.jd.jdbc.engine;

import com.jd.jdbc.IExecute;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.engine.Engine.AggregateOpcode;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.planbuilder.Truncater;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtRowList;
import com.jd.jdbc.sqltypes.VtStreamResultSet;
import io.vitess.proto.Query;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * OrderedAggregate is a primitive that expects the underlying primitive
 * to feed results in an order sorted by the Keys. Rows with duplicate
 * keys are aggregated using the Aggregate functions. The assumption
 * is that the underlying primitive is a scatter select with pre-sorted
 * rows.
 */
@Getter
@Setter
public class OrderedAggregateEngine implements PrimitiveEngine, Truncater {
    private static final Log logger = LogFactory.getLog(OrderedAggregateEngine.class);

    /**
     * Some predefined values
     */
    public static Map<AggregateOpcode, Query.Type> OPCODE_TYPE = new HashMap<AggregateOpcode, Query.Type>() {{
        put(AggregateOpcode.AggregateCountDistinct, Query.Type.INT64);
        put(AggregateOpcode.AggregateSumDistinct, Query.Type.DECIMAL);
    }};

    public static VtResultValue COUNT_ZERO = null;

    public static VtResultValue COUNT_ONE = null;

    public static VtResultValue SUM_ZERO = null;

    static {
        try {
            COUNT_ZERO = VtResultValue.newVtResultValue(Query.Type.INT64, 0L);
            COUNT_ONE = VtResultValue.newVtResultValue(Query.Type.INT64, 1L);
            SUM_ZERO = VtResultValue.newVtResultValue(Query.Type.DECIMAL, new BigDecimal(0));
        } catch (SQLException ignored) {
            // unreachable
            logger.error("unreachable! init zero data error!", ignored);
        }
    }

    /**
     * HasDistinct is true if one of the aggregates is distinct.
     */
    private Boolean hasDistinct = false;

    /**
     * Aggregates specifies the aggregation parameters for each
     * aggregation function: function opcode and input column number.
     */
    private List<AggregateParams> aggregateParamsList = new ArrayList<>();

    /**
     * Keys specifies the input values that must be used for
     * the aggregation key.
     */
    private List<Integer> keyList = new ArrayList<>();

    /**
     * TruncateColumnCount specifies the number of columns to return
     * in the final result. Rest of the columns are truncated
     * from the result received. If 0, no truncation happens.
     */
    private Integer truncateColumnCount = 0;

    /**
     * Input is the primitive that will feed into this Primitive.
     */
    private PrimitiveEngine input;

    /**
     * @param opcode
     * @return
     * @throws SQLException
     */
    private static VtResultValue createEmptyValueFor(AggregateOpcode opcode) throws SQLException {
        switch (opcode) {
            case AggregateCountDistinct:
            case AggregateCount:
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

    @Override
    public String getKeyspaceName() {
        return this.input.getKeyspaceName();
    }

    @Override
    public String getTableName() {
        return this.input.getTableName();
    }

    @Override
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor cursor, Map<String, Query.BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        IExecute.ExecuteMultiShardResponse resultResponse = this.orderedAggregateExecute(ctx, cursor, bindVariableMap, wantFields);
        VtResultSet queryResult = (VtResultSet) resultResponse.getVtRowList();
        return new IExecute.ExecuteMultiShardResponse(queryResult.truncate(this.truncateColumnCount));
    }

    @Override
    public IExecute.ExecuteMultiShardResponse mergeResult(VtResultSet result, Map<String, Query.BindVariable> bindValues, boolean wantFields) throws SQLException {
        IExecute.ExecuteMultiShardResponse executeMultiShardResponse = this.input.mergeResult(result, bindValues, wantFields);
        VtResultSet vtResultSet = (VtResultSet) executeMultiShardResponse.getVtRowList();
        return getExecuteMultiShardResponse(vtResultSet);
    }

    @Override
    public IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, Query.BindVariable> bindValues) throws SQLException {
        IExecute.ResolvedShardQuery resolvedShardQuery = this.input.resolveShardQuery(ctx, vcursor, bindValues);
        return new IExecute.ResolvedShardQuery(resolvedShardQuery.getRss(), resolvedShardQuery.getQueries());
    }

    @Override
    public IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, Query.BindVariable> bindValues, Map<String, String> switchTableMap) throws SQLException {
        IExecute.ResolvedShardQuery resolvedShardQuery = this.input.resolveShardQuery(ctx, vcursor, bindValues, switchTableMap);
        return new IExecute.ResolvedShardQuery(resolvedShardQuery.getRss(), resolvedShardQuery.getQueries());
    }

    @Override
    public Boolean canResolveShardQuery() {
        return this.input.canResolveShardQuery();
    }

    /**
     * @param ctx
     * @param vcursor
     * @param bindValues
     * @param wantFields
     * @return
     * @throws Exception
     */
    @Override
    public IExecute.VtStream streamExecute(IContext ctx, Vcursor vcursor, Map<String, Query.BindVariable> bindValues, boolean wantFields) throws SQLException {
        IExecute.VtStream vtStream = this.input.streamExecute(ctx, vcursor, bindValues, wantFields);
        return new IExecute.VtStream() {
            private final VtStreamResultSet vtStreamResultSet = new VtStreamResultSet(vtStream);

            private IExecute.VtStream stream = vtStream;

            private Query.Field[] fields;

            private List<VtResultValue> current;

            private VtResultValue curDistinct;

            private boolean isEmpty = true;

            private boolean returnEmpty = false;

            private VtRowList internalFetch() throws SQLException {
                VtResultSet vtResultSet = new VtResultSet();
                vtResultSet.setRows(new ArrayList<>());
                if (stream == null) {
                    return vtResultSet;
                }

                while (vtStreamResultSet.hasNext()) {
                    isEmpty = false;
                    List<VtResultValue> row = vtStreamResultSet.next();

                    if (fields == null) {
                        fields = convertFields(vtStreamResultSet.getFields());
                    }

                    if (current == null) {
                        rowProcessResponse response = convertRow(row);
                        current = response.getRow();
                        curDistinct = response.getCurDistinct();
                        continue;
                    }

                    Boolean equal = keysEqual(current, row);
                    if (equal) {
                        rowProcessResponse response = merge(vtStreamResultSet.getFields(), current, row, curDistinct);
                        current = response.getRow();
                        curDistinct = response.getCurDistinct();
                        continue;
                    }

                    vtResultSet.getRows().add(current);
                    rowProcessResponse response = convertRow(row);
                    current = response.getRow();
                    curDistinct = response.getCurDistinct();

                    return vtResultSet;
                }

                if (isEmpty && keyList.size() == 0) {
                    // When doing aggregation without grouping keys, we need to produce a single row containing zero-value for the
                    // different aggregation functions
                    if (returnEmpty) {
                        return vtResultSet;
                    }
                    returnEmpty = true;
                    List<VtResultValue> row = createEmptyRow();
                    vtResultSet.getRows().add(row);
                    fields = convertFields(vtStreamResultSet.getFields());
                    return vtResultSet;
                }

                if (current != null) {
                    vtResultSet.getRows().add(current);
                    current = null;
                    return vtResultSet;
                }

                return vtResultSet;
            }

            @Override
            public VtRowList fetch(boolean wantFields) throws SQLException {
                VtResultSet vtResultSet = (VtResultSet) internalFetch();
                vtResultSet.setRowsAffected(vtResultSet.getRows().size());
                if (wantFields) {
                    vtResultSet.setFields(fields);
                }
                return vtResultSet.truncate(truncateColumnCount);
            }

            @Override
            public void close() throws SQLException {
                if (stream != null) {
                    stream.close();
                    stream = null;
                }
            }
        };
    }

    /**
     * @param cursor
     * @param bindVariableMap
     * @param wantFields
     * @return
     * @throws Exception
     */
    private IExecute.ExecuteMultiShardResponse orderedAggregateExecute(IContext ctx, Vcursor cursor, Map<String, Query.BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        IExecute.ExecuteMultiShardResponse resultResponse = this.input.execute(ctx, cursor, bindVariableMap, wantFields);
        return getExecuteMultiShardResponse((VtResultSet) resultResponse.getVtRowList());
    }

    /**
     * @param fields
     * @param row1
     * @param row2
     * @param curDistinct
     * @return
     * @throws SQLException
     */
    private rowProcessResponse merge(Query.Field[] fields, List<VtResultValue> row1, List<VtResultValue> row2, VtResultValue curDistinct) throws SQLException {
        // result := sqltypes.CopyRow(row1)
        List<VtResultValue> result = new ArrayList<>(row1);
        for (AggregateParams aggr : this.aggregateParamsList) {
            if (aggr.isDistinct()) {
                if (row2.get(aggr.getCol()).isNull()) {
                    continue;
                }
                Integer cmp = EvalEngine.nullSafeCompare(curDistinct, row2.get(aggr.getCol()));
                if (cmp == 0) {
                    continue;
                }
                curDistinct = row2.get(aggr.getCol());
            }
            switch (aggr.getOpcode()) {
                case AggregateCount:
                case AggregateSum:
                    result.set(aggr.col, EvalEngine.nullSafeAdd(row1.get(aggr.col), row2.get(aggr.col), fields[aggr.col].getType()));
                    break;
                case AggregateMin:
                    result.set(aggr.col, EvalEngine.min(row1.get(aggr.col), row2.get(aggr.col)));
                    break;
                case AggregateMax:
                    result.set(aggr.col, EvalEngine.max(row1.get(aggr.col), row2.get(aggr.col)));
                    break;
                case AggregateCountDistinct:
                    result.set(aggr.col, EvalEngine.nullSafeAdd(row1.get(aggr.col), COUNT_ONE, OPCODE_TYPE.get(aggr.opcode)));
                    break;
                case AggregateSumDistinct:
                    result.set(aggr.col, EvalEngine.nullSafeAdd(row1.get(aggr.col), row2.get(aggr.col), OPCODE_TYPE.get(aggr.opcode)));
                    break;
                default:
                    break;
            }
        }
        return new rowProcessResponse(result, curDistinct);
    }

    /**
     * creates the empty row for the case when we are missing grouping keys and have empty input table
     *
     * @return
     */
    private List<VtResultValue> createEmptyRow() throws SQLException {
        List<VtResultValue> out = new ArrayList<>();
        for (AggregateParams aggregateParams : this.aggregateParamsList) {
            VtResultValue value = createEmptyValueFor(aggregateParams.getOpcode());
            out.add(value);
        }
        return out;
    }

    /**
     * @param fields
     * @return
     */
    private Query.Field[] convertFields(Query.Field[] fields) {
        if (!this.hasDistinct) {
            return fields;
        }

        for (AggregateParams aggr : this.aggregateParamsList) {
            if (!aggr.isDistinct()) {
                continue;
            }
            fields[aggr.col] = Query.Field.newBuilder()
                .setName(aggr.alias)
                .setType(OPCODE_TYPE.get(aggr.opcode))
                .build();
        }
        return fields;
    }

    /**
     * @param row
     * @return
     * @throws SQLException
     */
    private rowProcessResponse convertRow(List<VtResultValue> row) throws SQLException {
        if (!this.hasDistinct) {
            // NULL represents the NULL value.
            return new rowProcessResponse(row, VtResultValue.NULL);
        }

        VtResultValue curDistinct = null;
        for (AggregateParams aggr : this.aggregateParamsList) {
            if (aggr.getOpcode() == AggregateOpcode.AggregateCountDistinct) {
                curDistinct = row.get(aggr.col);
                // Type is int64. Ok to call MakeTrusted.
                if (row.get(aggr.col).isNull()) {
                    row.set(aggr.col, COUNT_ZERO);
                } else {
                    row.set(aggr.col, COUNT_ONE);
                }
            } else if (aggr.getOpcode() == AggregateOpcode.AggregateSumDistinct) {
                curDistinct = row.get(aggr.col);
                try {
                    row.set(aggr.col, EvalEngine.cast(row.get(aggr.col), OPCODE_TYPE.get(aggr.getOpcode())));
                } catch (SQLException e) {
                    row.set(aggr.col, SUM_ZERO);
                }
            }
        }
        return new rowProcessResponse(row, curDistinct);
    }

    /**
     * @param row1
     * @param row2
     * @return
     * @throws SQLException
     */
    private Boolean keysEqual(List<VtResultValue> row1, List<VtResultValue> row2) throws SQLException {
        for (Integer key : this.keyList) {
            Integer cmp = EvalEngine.nullSafeCompare(row1.get(key), row2.get(key));
            if (cmp != 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public VtResultSet getFields(Vcursor vcursor, Map<String, Query.BindVariable> bindValues) throws SQLException {
        VtResultSet qr = this.input.getFields(vcursor, bindValues);
        Query.Field[] fields = this.convertFields(qr.getFields());
        qr = new VtResultSet();
        qr.setFields(fields);
        return qr.truncate(this.truncateColumnCount);
    }

    @Override
    public Boolean needsTransaction() {
        return this.input.needsTransaction();
    }

    @Override
    public void setTruncateColumnCount(Integer count) {
        this.truncateColumnCount = count;
    }

    private IExecute.ExecuteMultiShardResponse getExecuteMultiShardResponse(VtResultSet result) throws SQLException {
        VtResultSet out = new VtResultSet(convertFields(result.getFields()), new ArrayList<>());
        // This code is similar to the one in StreamExecute.
        List<VtResultValue> current = null;
        VtResultValue curDistinct = null;

        for (List<VtResultValue> row : result.getRows()) {
            if (current == null) {
                rowProcessResponse response = this.convertRow(row);
                current = response.getRow();
                curDistinct = response.getCurDistinct();
                continue;
            }

            Boolean equal = this.keysEqual(current, row);
            if (equal) {
                rowProcessResponse response = merge(result.getFields(), current, row, curDistinct);
                current = response.getRow();
                curDistinct = response.getCurDistinct();
                continue;
            }
            out.getRows().add(current);
            rowProcessResponse response = convertRow(row);
            current = response.getRow();
            curDistinct = response.getCurDistinct();
        }

        if (result.getRows().size() == 0 && this.keyList.size() == 0) {
            // When doing aggregation without grouping keys, we need to produce a single row containing zero-value for the
            // different aggregation functions
            List<VtResultValue> row = this.createEmptyRow();
            out.getRows().add(row);
        }

        if (current != null) {
            out.getRows().add(current);
        }
        out.setRowsAffected(out.getRows().size());

        return new IExecute.ExecuteMultiShardResponse(out.truncate(this.truncateColumnCount));
    }

    @Getter
    @Setter
    public static class AggregateParams {
        private AggregateOpcode opcode;

        private Integer col;

        private String alias;

        public AggregateParams(AggregateOpcode opcode, Integer col) {
            this.opcode = opcode;
            this.col = col;
        }

        public AggregateParams(AggregateOpcode opcode, Integer col, String alias) {
            this.opcode = opcode;
            this.col = col;
            this.alias = alias;
        }

        public Boolean isDistinct() {
            return this.opcode == AggregateOpcode.AggregateCountDistinct || this.opcode == AggregateOpcode.AggregateSumDistinct;
        }
    }

    @Getter
    @AllArgsConstructor
    public static class rowProcessResponse {
        private final List<VtResultValue> row;

        private final VtResultValue curDistinct;
    }
}
