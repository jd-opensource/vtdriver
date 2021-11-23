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
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.planbuilder.Truncater;
import com.jd.jdbc.sqltypes.VtPlanValue;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtValue;
import io.vitess.proto.Query;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MemorySortEngine implements PrimitiveEngine, Truncater {

    private VtPlanValue upperLimit = new VtPlanValue();

    private List<OrderByParams> orderByParams = new ArrayList<>();

    private PrimitiveEngine input = null;

    /**
     * // TruncateColumnCount specifies the number of columns to return
     * // in the final result. Rest of the columns are truncated
     * // from the result received. If 0, no truncation happens.
     */
    private Integer truncateColumnCount = 0;

    @Override
    public String getKeyspaceName() {
        return this.input.getKeyspaceName();
    }

    @Override
    public String getTableName() {
        return this.input.getTableName();
    }

    @Override
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, Query.BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        int count = this.fetchCount(bindVariableMap);

        IExecute.ExecuteMultiShardResponse response = this.input.execute(ctx, vcursor, bindVariableMap, wantFields);

        VtResultSet resultSet = (VtResultSet) response.getVtRowList();
        return getExecuteMultiShardResponse(count, resultSet);
    }

    @Override
    public IExecute.ExecuteMultiShardResponse mergeResult(VtResultSet resultSet, Map<String, Query.BindVariable> bindValues, boolean wantFields) throws SQLException {
        int count = this.fetchCount(bindValues);
        IExecute.ExecuteMultiShardResponse executeMultiShardResponse = this.input.mergeResult(resultSet, bindValues, wantFields);
        VtResultSet vtResultSet = (VtResultSet) executeMultiShardResponse.getVtRowList();
        return getExecuteMultiShardResponse(count, vtResultSet);
    }

    @Override
    public IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, Query.BindVariable> bindVariableMap) throws SQLException {
        return this.input.resolveShardQuery(ctx, vcursor, bindVariableMap);
    }

    @Override
    public IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, Query.BindVariable> bindVariableMap, Map<String, String> switchTableMap) throws SQLException {
        return this.input.resolveShardQuery(ctx, vcursor, bindVariableMap, switchTableMap);
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
        int count = this.fetchCount(bindValues);
        IExecute.VtStream vtStream = this.input.streamExecute(ctx, vcursor, bindValues, wantFields);
        return new MemorySortStream(vtStream, orderByParams, truncateColumnCount, count);
    }

    @Override
    public VtResultSet getFields(Vcursor vcursor, Map<String, Query.BindVariable> bindValues) throws SQLException {
        return this.input.getFields(vcursor, bindValues);
    }

    @Override
    public Boolean needsTransaction() {
        return this.input.needsTransaction();
    }

    @Override
    public void setTruncateColumnCount(Integer count) {
        this.truncateColumnCount = count;
    }

    /**
     * @param bindVariableMap
     * @return
     * @throws SQLException
     */
    private Integer fetchCount(Map<String, Query.BindVariable> bindVariableMap) throws SQLException {
        VtValue resolved = this.upperLimit.resolveValue(bindVariableMap);
        if (resolved.isNull()) {
            return Integer.MAX_VALUE;
        }
        BigInteger num = EvalEngine.toUint64(resolved);
        int count = num.intValue();
        if (count < 0) {
            throw new SQLException("requested limit is out of range: " + num);
        }
        return count;
    }

    private IExecute.ExecuteMultiShardResponse getExecuteMultiShardResponse(int count, VtResultSet resultSet) throws SQLException {
        SortHeap sh = new SortHeap(resultSet.getRows(), this.orderByParams);

        sh.rows.sort(sh);
        if (sh.exception != null) {
            throw sh.exception;
        }
        resultSet.setRows(sh.rows);
        if (resultSet.getRows().size() > count) {
            resultSet.setRows(resultSet.getRows().subList(0, count));
            resultSet.setRowsAffected(count);
        }
        return new IExecute.ExecuteMultiShardResponse(resultSet.truncate(this.truncateColumnCount));
    }

    private static class SortHeap implements Comparator<List<VtResultValue>> {
        private final List<List<VtResultValue>> rows;

        private final List<OrderByParams> orderBy;

        private final Boolean reverse;

        private SQLException exception;

        public SortHeap(List<List<VtResultValue>> rows, List<OrderByParams> orderBy) {
            this.rows = rows;
            this.orderBy = orderBy;
            this.reverse = false;
            this.exception = null;
        }

        @Override
        public int compare(List<VtResultValue> o1, List<VtResultValue> o2) {
            for (OrderByParams order : this.orderBy) {
                if (this.exception != null) {
                    return -1;
                }
                int cmp;
                try {
                    cmp = EvalEngine.nullSafeCompare(o1.get(order.col), o2.get(order.col));
                } catch (SQLException e) {
                    this.exception = e;
                    return -1;
                }
                if (cmp == 0) {
                    continue;
                }
                if (!this.reverse.equals(order.desc)) {
                    cmp = -cmp;
                }
                return cmp;
            }
            return 0;
        }
    }
}
