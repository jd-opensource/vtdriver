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

package com.jd.jdbc.engine.gen4;

import com.jd.jdbc.IExecute;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.evalengine.EvalResult;
import com.jd.jdbc.planbuilder.Truncater;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.srvtopo.BindVariable;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import lombok.Getter;
import lombok.Setter;

/**
 * MemorySort is a primitive that performs in-memory sorting.
 */
@Getter
@Setter
public class MemorySortGen4Engine implements PrimitiveEngine, Truncater {

    private EvalEngine.Expr upperLimit;

    private List<OrderByParamsGen4> orderByParams = new ArrayList<>(10);

    private PrimitiveEngine input = null;

    /**
     * TruncateColumnCount specifies the number of columns to return
     * in the final result. Rest of the columns are truncated
     * from the result received. If 0, no truncation happens.
     */
    private int truncateColumnCount = 0;

    public MemorySortGen4Engine() {
    }

    public MemorySortGen4Engine(List<OrderByParamsGen4> orderByParams, PrimitiveEngine input) {
        this.orderByParams = orderByParams;
        this.input = input;
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
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        int count = this.fetchCount(bindVariableMap);

        IExecute.ExecuteMultiShardResponse response = this.input.execute(ctx, vcursor, bindVariableMap, wantFields);

        VtResultSet resultSet = (VtResultSet) response.getVtRowList();
        return getExecuteMultiShardResponse(count, resultSet);
    }

    @Override
    public Boolean canResolveShardQuery() {
        return this.input.canResolveShardQuery();
    }

    @Override
    public VtResultSet getFields(Vcursor vcursor, Map<String, BindVariable> bindValues) throws SQLException {
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
    private Integer fetchCount(Map<String, BindVariable> bindVariableMap) throws SQLException {
        if (this.upperLimit == null) {
            return Integer.MAX_VALUE;
        }
        EvalEngine.ExpressionEnv env = new EvalEngine.ExpressionEnv(bindVariableMap);
        EvalResult resolved = env.evaluate(this.upperLimit);
        BigInteger num = EvalEngine.toUint64(resolved.value());

        int count = num.intValue();
        if (count < 0) {
            throw new SQLException("requested limit is out of range: " + num);
        }
        return count;
    }

    private IExecute.ExecuteMultiShardResponse getExecuteMultiShardResponse(int count, VtResultSet resultSet) throws SQLException {
        List<VitessCompare> compares = VitessCompare.extractSlices(this.orderByParams);

        SortHeapComparator sh = new SortHeapComparator(compares);
        PriorityQueue<List<VtResultValue>> queue = new PriorityQueue<>(resultSet.getRows().size(), sh);
        queue.addAll(resultSet.getRows());
        if (sh.getException() != null) {
            throw sh.getException();
        }
        List<List<VtResultValue>> rows = new ArrayList<>(resultSet.getRows().size());
        while (!queue.isEmpty()) {
            List<VtResultValue> poll = queue.poll();
            rows.add(poll);
        }
        resultSet.setRows(rows);
        if (resultSet.getRows().size() > count) {
            resultSet.setRows(resultSet.getRows().subList(0, count));
        }
        return new IExecute.ExecuteMultiShardResponse(resultSet.truncate(this.truncateColumnCount));
    }

    private static class SortHeapComparator implements Comparator<List<VtResultValue>> {

        private final List<VitessCompare> comparers;

        private final boolean reverse;

        @Getter
        private SQLException exception;

        SortHeapComparator(List<VitessCompare> comparers) {
            this.comparers = comparers;
            this.reverse = false;
        }

        @Override
        public int compare(List<VtResultValue> o1, List<VtResultValue> o2) {
            for (VitessCompare c : this.comparers) {
                if (this.exception != null) {
                    return -1;
                }
                int cmp;
                try {
                    cmp = c.compare(o1, o2);
                } catch (SQLException e) {
                    this.exception = e;
                    return -1;
                }
                if (cmp == 0) {
                    continue;
                }
                if (this.reverse) {
                    cmp = -cmp;
                }
                return cmp;
            }
            return 0;
        }
    }
}
