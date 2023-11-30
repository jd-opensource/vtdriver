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

import com.jd.jdbc.common.tuple.ImmutablePair;
import com.jd.jdbc.common.tuple.Pair;
import com.jd.jdbc.engine.Engine.AggregateOpcodeG4;
import com.jd.jdbc.engine.OrderedAggregateEngine;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.planbuilder.Truncater;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.srvtopo.BindVariable;
import io.vitess.proto.Query;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

public abstract class AbstractAggregateGen4 implements PrimitiveEngine, Truncater {

    private static final Log logger = LogFactory.getLog(OrderedAggregateEngine.class);

    /**
     * SupportedAggregates maps the list of supported aggregate
     * functions to their opcodes.
     */
    public static final Map<String, AggregateOpcodeG4> SUPPORTED_AGGREGATES = new HashMap<>(9);

    static {
        SUPPORTED_AGGREGATES.put("count", AggregateOpcodeG4.AggregateCount);
        SUPPORTED_AGGREGATES.put("sum", AggregateOpcodeG4.AggregateSum);
        SUPPORTED_AGGREGATES.put("min", AggregateOpcodeG4.AggregateMin);
        SUPPORTED_AGGREGATES.put("max", AggregateOpcodeG4.AggregateMax);

        // These functions don't exist in mysql, but are used to display the plan.
        SUPPORTED_AGGREGATES.put("count_distinct", AggregateOpcodeG4.AggregateCountDistinct);
        SUPPORTED_AGGREGATES.put("sum_distinct", AggregateOpcodeG4.AggregateSumDistinct);
        SUPPORTED_AGGREGATES.put("vgtid", AggregateOpcodeG4.AggregateGtid);
        SUPPORTED_AGGREGATES.put("count_star", AggregateOpcodeG4.AggregateCountStar);
        SUPPORTED_AGGREGATES.put("random", AggregateOpcodeG4.AggregateRandom);
    }

    public static Map<AggregateOpcodeG4, Query.Type> OPCODE_TYPE = new HashMap<>(6);

    static {
        OPCODE_TYPE.put(AggregateOpcodeG4.AggregateCountDistinct, Query.Type.INT64);
        OPCODE_TYPE.put(AggregateOpcodeG4.AggregateCount, Query.Type.INT64);
        OPCODE_TYPE.put(AggregateOpcodeG4.AggregateCountStar, Query.Type.INT64);
        OPCODE_TYPE.put(AggregateOpcodeG4.AggregateSumDistinct, Query.Type.DECIMAL);
        OPCODE_TYPE.put(AggregateOpcodeG4.AggregateSum, Query.Type.DECIMAL);
        OPCODE_TYPE.put(AggregateOpcodeG4.AggregateGtid, Query.Type.VARCHAR);
    }

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
     * PreProcess is true if one of the aggregates needs preprocessing.
     */
    protected boolean preProcess;

    /**
     * Aggregates specifies the aggregation parameters for each
     * aggregation function: function opcode and input column number.
     */
    @Getter
    protected List<AggregateParams> aggregates;

    protected boolean aggrOnEngine;

    /**
     * TruncateColumnCount specifies the number of columns to return
     * in the final result. Rest of the columns are truncated
     * from the result received. If 0, no truncation happens.
     */
    @Getter
    protected int truncateColumnCount;

    /**
     * Collations stores the collation ID per column offset.
     * It is used for grouping keys and distinct aggregate functions
     */
    protected Map<Integer, Integer> collations;

    /**
     * Input is the primitive that will feed into this Primitive.
     */
    @Getter
    protected PrimitiveEngine input;

    @Override
    public String getKeyspaceName() {
        return input.getKeyspaceName();
    }

    @Override
    public String getTableName() {
        return input.getTableName();
    }

    @Override
    public Boolean needsTransaction() {
        return input.needsTransaction();
    }

    @Override
    public void setTruncateColumnCount(Integer count) {
        this.truncateColumnCount = count;
    }

    @Override
    public VtResultSet getFields(Vcursor vcursor, Map<String, BindVariable> bindValues) throws SQLException {
        VtResultSet qr = this.input.getFields(vcursor, bindValues);
        Query.Field[] fields = convertFields(qr.getFields(), this.preProcess, this.aggregates, this.aggrOnEngine);
        qr = new VtResultSet();
        qr.setFields(fields);
        return qr.truncate(this.truncateColumnCount);
    }

    protected VtResultValue findComparableCurrentDistinct(List<VtResultValue> row, AggregateParams aggr) {
        VtResultValue curDistinct = row.get(aggr.keyCol);
        if (aggr.wAssigned && curDistinct.isComparable()) {
            aggr.keyCol = aggr.getWCol();
            curDistinct = row.get(aggr.keyCol);
        }
        return curDistinct;
    }

    public Pair<List<VtResultValue>, List<VtResultValue>> merge(Query.Field[] fields, List<VtResultValue> row1, List<VtResultValue> row2, List<VtResultValue> curDistincts,
                                                                Map<Integer, Integer> colls, List<AggregateParams> aggregates) throws SQLException {
        List<VtResultValue> result = new ArrayList<>(row1);
        for (int index = 0; index < aggregates.size(); index++) {
            AggregateParams aggr = aggregates.get(index);
            if (aggr.isDistinct()) {
                if (row2.get(aggr.keyCol).isNull()) {
                    continue;
                }
                Integer cmp = EvalEngine.nullSafeCompare(curDistincts.get(index), row2.get(aggr.getCol()));
                if (cmp == 0) {
                    continue;
                }
                curDistincts.set(index, findComparableCurrentDistinct(row2, aggr));
            }
            switch (aggr.getOpcode()) {
                case AggregateCountStar:
                    result.set(aggr.col, EvalEngine.nullSafeAdd(row1.get(aggr.col), COUNT_ONE, fields[aggr.col].getType()));
                    break;
                case AggregateCount:
                    VtResultValue value = COUNT_ONE;
                    if (row2.get(aggr.col).isNull()) {
                        value = COUNT_ZERO;
                    }
                    result.set(aggr.col, EvalEngine.nullSafeAdd(row1.get(aggr.col), value, fields[aggr.col].getType()));
                    break;
                case AggregateSum:
                    VtResultValue v1 = row1.get(aggr.col);
                    VtResultValue v2 = row2.get(aggr.col);
                    if (v1.isNull() && v2.isNull()) {
                        break;
                    }
                    result.set(aggr.col, EvalEngine.nullSafeAdd(v1, v2, fields[aggr.col].getType()));
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
                case AggregateRandom:
                    // we just grab the first value per grouping. no need to do anything more complicated here
                    break;
                default:
                    throw new SQLException("BUG: Unexpected opcode: " + aggr.opcode);
            }
        }
        return new ImmutablePair<>(result, curDistincts);
    }

    protected List<VtResultValue> convertFinal(List<VtResultValue> current, List<AggregateParams> aggregates) {
        List<VtResultValue> result = new ArrayList<>(current);
        return result;
    }

    protected Query.Field[] convertFields(Query.Field[] fields, boolean preProcess, List<AggregateParams> aggrs, boolean aggrOnEngine) {
        if (!preProcess) {
            return fields;
        }
        for (AggregateParams aggr : aggrs) {
            if (!aggr.preProcess() && !aggrOnEngine) {
                continue;
            }
            Query.Field field = Query.Field.newBuilder().setName(aggr.getAlias()).setType(OPCODE_TYPE.get(aggr.getOpcode())).build();
            fields[aggr.getCol()] = field;
            if (aggr.isDistinct()) {
                aggr.setKeyCol(aggr.getCol());
            }
        }
        return fields;
    }

    protected Pair<List<VtResultValue>, List<VtResultValue>> convertRow(List<VtResultValue> row, boolean preProcess, List<AggregateParams> aggregates, boolean aggrOnEngine) {
        if (!preProcess) {
            return new ImmutablePair<>(row, null);
        }
        List<VtResultValue> newRow = new ArrayList<>(row);
        List<VtResultValue> curDistincts = new ArrayList<>(aggregates.size());
        for (int index = 0; index < aggregates.size(); index++) {
            AggregateParams aggr = aggregates.get(index);
            switch (aggr.getOpcode()) {
                case AggregateCountStar:
                    newRow.set(aggr.getCol(), COUNT_ONE);
                    break;
                case AggregateCount:
                    VtResultValue val = COUNT_ONE;
                    if (row.get(aggr.getCol()).isNull()) {
                        val = COUNT_ZERO;
                    }
                    newRow.set(aggr.getCol(), val);
                    break;
                case AggregateCountDistinct:
                    curDistincts.add(index, findComparableCurrentDistinct(row, aggr));
                    // Type is int64. Ok to call MakeTrusted.
                    if (row.get(aggr.getKeyCol()).isNull()) {
                        newRow.set(aggr.getCol(), COUNT_ZERO);
                    } else {
                        newRow.set(aggr.getCol(), COUNT_ONE);
                    }
                    break;
                case AggregateSum:
                    if (!aggrOnEngine) {
                        break;
                    }
                    if (row.get(aggr.getCol()).isNull()) {
                        break;
                    }
                    try {
                        newRow.set(aggr.getCol(), EvalEngine.cast(row.get(aggr.getCol()), OPCODE_TYPE.get(aggr.getOpcode())));
                    } catch (Exception e) {
                        newRow.set(aggr.getCol(), SUM_ZERO);
                    }
                    break;
                case AggregateSumDistinct:
                    curDistincts.add(index, findComparableCurrentDistinct(row, aggr));
                    try {
                        newRow.set(aggr.getCol(), EvalEngine.cast(row.get(aggr.getCol()), OPCODE_TYPE.get(aggr.getOpcode())));
                    } catch (Exception e) {
                        newRow.set(aggr.getCol(), SUM_ZERO);
                    }
                    break;
                default:
                    break;
            }
        }
        return new ImmutablePair<>(newRow, curDistincts);
    }

    public static String printOpcode(AggregateOpcodeG4 code) throws SQLException {
        for (Map.Entry<String, AggregateOpcodeG4> entry : SUPPORTED_AGGREGATES.entrySet()) {
            if (entry.getValue() == code) {
                return entry.getKey();
            }
        }
        throw new SQLException("unexpect AggregateOpcodeG4");
    }


    /**
     * AggregateParams specify the parameters for each aggregation.
     * It contains the opcode and input column number.
     **/
    @Getter
    @Setter
    public static class AggregateParams {
        private AggregateOpcodeG4 opcode;

        private int col;

        private String alias;

        private SQLExpr expr;

        private SQLSelectItem original;

        // These are used only for distinct opcodes.
        private int keyCol;

        private int wCol;

        private boolean wAssigned;

        private Integer collationId;

        /**
         * This is based on the function passed in the select expression and
         * not what we use to aggregate at the engine primitive level.
         */
        private AggregateOpcodeG4 origOpcode;

        public void setOrigOpcode(AggregateOpcodeG4 opcode) {
            this.origOpcode = opcode == null ? AggregateOpcodeG4.AggregateUnassigned : opcode;
        }

        public void setOpcode(AggregateOpcodeG4 opcode) {
            this.opcode = opcode == null ? AggregateOpcodeG4.AggregateUnassigned : opcode;
        }

        public AggregateParams() {

        }

        public AggregateParams(AggregateOpcodeG4 opcode, Integer col) {
            this.opcode = opcode == null ? AggregateOpcodeG4.AggregateUnassigned : opcode;
            this.col = col;
        }

        public AggregateParams(AggregateOpcodeG4 opcode, Integer col, String alias) {
            this.opcode = opcode == null ? AggregateOpcodeG4.AggregateUnassigned : opcode;
            this.col = col;
            this.alias = alias;
        }

        public AggregateParams(AggregateOpcodeG4 opcode, Integer col, String alias, SQLExpr expr, SQLSelectItem original) {
            this.opcode = opcode == null ? AggregateOpcodeG4.AggregateUnassigned : opcode;
            this.col = col;
            this.alias = alias;
            this.expr = expr;
            this.original = original;
        }

        public Boolean isDistinct() {
            return this.opcode == AggregateOpcodeG4.AggregateCountDistinct || this.opcode == AggregateOpcodeG4.AggregateSumDistinct;
        }

        public Boolean preProcess() {
            return this.opcode == AggregateOpcodeG4.AggregateCountDistinct || this.opcode == AggregateOpcodeG4.AggregateSumDistinct ||
                this.opcode == AggregateOpcodeG4.AggregateCount;  // AggregateGtid
        }
    }
}
