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
import com.jd.jdbc.planbuilder.PlanBuilder;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqltypes.SqlTypes;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.srvtopo.BindVariable;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LimitGen4Engine implements PrimitiveEngine {
    private static final Log LOGGER = LogFactory.getLog(PlanBuilder.class);

    private EvalEngine.Expr count;

    private EvalEngine.Expr offset;

    private PrimitiveEngine input;

    private Integer fetchCount;

    private Integer fetchOffset;

    public LimitGen4Engine() {
        this.input = null;
    }

    public LimitGen4Engine(PrimitiveEngine input, EvalEngine.Expr count, EvalEngine.Expr offset) {
        this.count = count;
        this.offset = offset;
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
        Integer count = getCount(vcursor, bindVariableMap);
        Integer offset = getOffset(vcursor, bindVariableMap);

        // When offset is present, we hijack the limit value so we can calculate
        // the offset in memory from the result of the scatter query with count + offset.
        bindVariableMap.put("__upper_limit", SqlTypes.int64BindVariable((long) (count + offset)));

        IExecute.ExecuteMultiShardResponse response = this.input.execute(ctx, vcursor, bindVariableMap, wantFields);
        VtResultSet result = (VtResultSet) response.getVtRowList();

        return getExecuteMultiShardResponse(result, count, offset);
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
    public VtResultSet getFields(Vcursor vcursor, Map<String, BindVariable> bindValues) throws SQLException {
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

    public Integer getCount(Vcursor vcursor, Map<String, BindVariable> bindValues) throws SQLException {
        EvalEngine.ExpressionEnv env = new EvalEngine.ExpressionEnv(bindValues);
        return getIntFrom(env, this.count);
    }

    public Integer getOffset(Vcursor vcursor, Map<String, BindVariable> bindValues) throws SQLException {
        EvalEngine.ExpressionEnv env = new EvalEngine.ExpressionEnv(bindValues);
        return getIntFrom(env, this.offset);
    }

    private Integer getIntFrom(EvalEngine.ExpressionEnv env, EvalEngine.Expr expr) throws SQLException {
        if (expr == null) {
            return 0;
        }
        EvalResult evalResult = env.evaluate(expr);
        VtValue value = evalResult.value();
        if (value.isNull()) {
            return 0;
        }

        BigInteger num = EvalEngine.toUint64(value);
        int count = num.intValue();
        if (count < 0) {
            throw new SQLException("requested limit is out of range: " + value);
        }
        return count;
    }
}
