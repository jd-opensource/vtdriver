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
import com.jd.jdbc.VcursorImpl;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.planbuilder.PlanBuilder;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqlparser.utils.Utils;
import com.jd.jdbc.sqltypes.SqlTypes;
import com.jd.jdbc.sqltypes.VtPlanValue;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtRowList;
import com.jd.jdbc.sqltypes.VtStreamResultSet;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.vitess.mysql.VitessPropertyKey;
import io.vitess.proto.Query;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LimitEngine implements PrimitiveEngine {
    public static final Integer LIMIT_VAR_REFINDEX = -2;

    public static final String LIMIT_VAR_NAME = ":__upper_limit";

    private static final Log LOGGER = LogFactory.getLog(PlanBuilder.class);

    private static final String DEEP_PAGINATION_THRESHOLD = "deepPaginationThreshold";

    private static final Integer DEFAULT_DEEP_PAGINATION_THRESHOLD = 1000000000;

    private VtPlanValue count;

    private VtPlanValue offset;

    private PrimitiveEngine input;

    private Integer fetchCount;

    private Integer fetchOffset;

    public LimitEngine() {
        this.count = new VtPlanValue();
        this.offset = new VtPlanValue();
        this.input = null;
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
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, Query.BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        Integer count = this.fetchCount(bindVariableMap);
        Integer offset = this.fetchOffset(bindVariableMap);
        // When offset is present, we hijack the limit value so we can calculate
        // the offset in memory from the result of the scatter query with count + offset.
        bindVariableMap.put("__upper_limit", SqlTypes.int64BindVariable((long) (count + offset)));

        Properties properties = ((VcursorImpl) vcursor).getSafeSession().getVitessConnection().getProperties();
        Integer deepPaginationThreshold = Utils.getInteger(properties, DEEP_PAGINATION_THRESHOLD);
        deepPaginationThreshold = deepPaginationThreshold == null ? DEFAULT_DEEP_PAGINATION_THRESHOLD : deepPaginationThreshold;

        if (offset > deepPaginationThreshold) {
            IExecute.VtStream vtStream = this.input.streamExecute(ctx, vcursor, bindVariableMap, wantFields);
            LimitStream limitStream = new LimitStream(count, offset, vtStream);
            Integer maxRows = (Integer) ctx.getContextValue(VitessPropertyKey.MAX_ROWS.getKeyName());
            VtRowList vtRowList = new VtStreamResultSet(limitStream, wantFields).reserve(maxRows);
            List<List<VtResultValue>> rows = new ArrayList<>();
            while (vtRowList != null && vtRowList.hasNext()) {
                List<VtResultValue> next = vtRowList.next();
                rows.add(next);
            }
            VtResultSet resultSet = new VtResultSet(rows.size(), rows);
            return new IExecute.ExecuteMultiShardResponse(resultSet);
        }
        IExecute.ExecuteMultiShardResponse response = this.input.execute(ctx, vcursor, bindVariableMap, wantFields);
        VtResultSet result = (VtResultSet) response.getVtRowList();

        return getExecuteMultiShardResponse(result, count, offset);
    }

    @Override
    public IExecute.ExecuteMultiShardResponse mergeResult(VtResultSet result, Map<String, Query.BindVariable> bindValues, boolean wantFields) throws SQLException {
        IExecute.ExecuteMultiShardResponse executeMultiShardResponse = this.input.mergeResult(result, bindValues, wantFields);
        VtResultSet vtResultSet = (VtResultSet) executeMultiShardResponse.getVtRowList();
        return getExecuteMultiShardResponse(vtResultSet, fetchCount, fetchOffset);
    }

    private IExecute.ExecuteMultiShardResponse getExecuteMultiShardResponse(VtResultSet result, Integer count, Integer offset) {
        // There are more rows in the response than limit + offset
        if (count + offset <= result.getRows().size()) {
            result.setRows(result.getRows().subList(offset, count + offset));
            result.setRowsAffected(count);
            return new IExecute.ExecuteMultiShardResponse(result);
        }
        // Remove extra rows from response
        if (offset <= result.getRows().size()) {
            result.setRows(result.getRows().subList(offset, result.getRows().size()));
            result.setRowsAffected(result.getRows().size());
            return new IExecute.ExecuteMultiShardResponse(result);
        }
        // offset is beyond the result set
        result.setRows(new ArrayList<>());
        result.setRowsAffected(0);
        return new IExecute.ExecuteMultiShardResponse(result);
    }

    @Override
    public IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, Query.BindVariable> bindValues) throws SQLException {
        fetchCount = this.fetchCount(bindValues);
        fetchOffset = this.fetchOffset(bindValues);
        // When offset is present, we hijack the limit value so we can calculate
        // the offset in memory from the result of the scatter query with count + offset.
        bindValues.put("__upper_limit", SqlTypes.int64BindVariable((long) (fetchCount + fetchOffset)));

        IExecute.ResolvedShardQuery resolvedShardQuery = this.input.resolveShardQuery(ctx, vcursor, bindValues);

        return new IExecute.ResolvedShardQuery(resolvedShardQuery.getRss(), resolvedShardQuery.getQueries());
    }

    @Override
    public IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, Query.BindVariable> bindValues, Map<String, String> switchTableMap) throws SQLException {
        fetchCount = this.fetchCount(bindValues);
        fetchOffset = this.fetchOffset(bindValues);
        // When offset is present, we hijack the limit value so we can calculate
        // the offset in memory from the result of the scatter query with count + offset.
        bindValues.put("__upper_limit", SqlTypes.int64BindVariable((long) (fetchCount + fetchOffset)));

        IExecute.ResolvedShardQuery resolvedShardQuery = this.input.resolveShardQuery(ctx, vcursor, bindValues, switchTableMap);

        return new IExecute.ResolvedShardQuery(resolvedShardQuery.getRss(), resolvedShardQuery.getQueries());
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
        Integer count = this.fetchCount(bindValues);
        Integer offset = this.fetchOffset(bindValues);
        // When offset is present, we hijack the limit value so we can calculate
        // the offset in memory from the result of the scatter query with count + offset.
        bindValues.put("__upper_limit", SqlTypes.int64BindVariable((long) (count + offset)));

        IExecute.VtStream vtStream = this.input.streamExecute(ctx, vcursor, bindValues, wantFields);

        return new LimitStream(count, offset, vtStream);
    }

    @Override
    public VtResultSet getFields(Vcursor vcursor, Map<String, Query.BindVariable> bindValues) throws SQLException {
        return this.input.getFields(vcursor, bindValues);
    }

    @Override
    public Boolean canResolveShardQuery() {
        return this.input.canResolveShardQuery();
    }

    @Override
    public Boolean needsTransaction() {
        return this.input.needsTransaction();
    }

    /**
     * @param bindVariableMap
     * @return
     * @throws SQLException
     */
    private Integer fetchCount(Map<String, Query.BindVariable> bindVariableMap) throws SQLException {
        if (this.count.isNull()) {
            return 0;
        }
        VtValue resolved = this.count.resolveValue(bindVariableMap);
        BigInteger num = EvalEngine.toUint64(resolved);
        int count = num.intValue();
        if (count < 0) {
            throw new SQLException("requested limit is out of range: " + num);
        }
        return count;
    }

    /**
     * @param bindValues
     * @return
     * @throws SQLException
     */
    private Integer fetchOffset(Map<String, Query.BindVariable> bindValues) throws SQLException {
        if (this.offset.isNull()) {
            return 0;
        }
        VtValue resolved = this.offset.resolveValue(bindValues);
        BigInteger num = EvalEngine.toUint64(resolved);
        int count = num.intValue();
        if (count < 0) {
            throw new SQLException("requested limit is out of range: " + num);
        }
        return count;
    }
}
