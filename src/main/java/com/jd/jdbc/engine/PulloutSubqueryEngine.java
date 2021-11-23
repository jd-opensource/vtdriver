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
import com.jd.jdbc.sqltypes.SqlTypes;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtValue;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PulloutSubqueryEngine implements PrimitiveEngine {
    private Engine.PulloutOpcode opcode;

    /**
     * SubqueryResult and HasValues are used to send in the bindvar used in the query to the underlying primitive
     */
    private String subqueryResult;

    private String hasValues;

    private PrimitiveEngine subquery;

    private PrimitiveEngine underlying;

    public PulloutSubqueryEngine() {
    }

    public PulloutSubqueryEngine(Engine.PulloutOpcode opcode, String subqueryResult, String hasValues) {
        this.opcode = opcode;
        this.subqueryResult = subqueryResult;
        this.hasValues = hasValues;
    }

    @Override
    public String getKeyspaceName() {
        return null;
    }

    @Override
    public String getTableName() {
        return null;
    }

    @Override
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, Query.BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        BindVarsResponse bindVarsResponse = this.execSubqery(ctx, vcursor, bindVariableMap, wantFields);
        return this.underlying.execute(ctx, vcursor, bindVarsResponse.getBindValues(), wantFields);
    }

    /**
     * @param vcursor
     * @param bindVariableMap
     * @param wantFields
     * @return
     */
    private BindVarsResponse execSubqery(IContext ctx, Vcursor vcursor, Map<String, Query.BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        IExecute.ExecuteMultiShardResponse subResultSet = this.subquery.execute(ctx, vcursor, bindVariableMap, wantFields);
        if (subResultSet.getVtRowList() == null) {
            throw new SQLException("VtRowList is null");
        }

        VtResultSet subResult = (VtResultSet) subResultSet.getVtRowList();

        Map<String, Query.BindVariable> combinedVars = bindVariableMap == null ? new HashMap<>() : new HashMap<>(bindVariableMap);
        switch (this.opcode) {
            case PulloutValue:
                if (subResult.getRows() == null || subResult.getRows().size() == 0) {
                    combinedVars.put(this.subqueryResult, SqlTypes.NULL_BIND_VARIABLE);
                } else if (subResult.getRows().size() == 1) {
                    if (subResult.getRows().get(0).size() != 1) {
                        throw new SQLException("subquery returned more than one column");
                    }
                    combinedVars.put(this.subqueryResult, SqlTypes.valueBindVariable(subResult.getRows().get(0).get(0)));
                } else {
                    throw new SQLException("subquery returned more than one row");
                }
                break;
            case PulloutIn:
            case PulloutNotIn:
                if (subResult.getRows() == null || subResult.getRows().isEmpty()) {
                    combinedVars.put(this.hasValues, SqlTypes.int64BindVariable(0L));
                    // Add a bogus value. It will not be checked.
                    List<VtValue> valueList = new ArrayList<VtValue>() {{
                        add(VtValue.newVtValue(Query.Type.INT64, "0".getBytes()));
                    }};
                    Query.BindVariable subBindVariable = SqlTypes.valuesBindVariable(Query.Type.TUPLE, valueList);
                    combinedVars.put(this.subqueryResult, subBindVariable);
                } else {
                    if (subResult.getRows().get(0).size() != 1) {
                        throw new SQLException("subquery returned more than one column");
                    }
                    combinedVars.put(this.hasValues, SqlTypes.int64BindVariable(1L));
                    Query.BindVariable values = Query.BindVariable.newBuilder().setType(Query.Type.TUPLE).build();
                    for (List<VtResultValue> vtValueList : subResult.getRows()) {
                        values = values.toBuilder().addValues(SqlTypes.vtValueToProto(vtValueList.get(0))).build();
                    }
                    combinedVars.put(this.subqueryResult, values);
                }
                break;
            case PulloutExists:
                if (subResult.getRows() == null || subResult.getRows().size() == 0) {
                    combinedVars.put(this.hasValues, SqlTypes.valueBindVariable(VtValue.newVtValue(Query.Type.INT64, "0".getBytes())));
                } else {
                    combinedVars.put(this.hasValues, SqlTypes.valueBindVariable(VtValue.newVtValue(Query.Type.INT64, "1".getBytes())));
                }
                break;
            default:
                break;
        }
        return new BindVarsResponse(combinedVars);
    }

    @Override
    public IExecute.ExecuteMultiShardResponse mergeResult(VtResultSet vtResultSet, Map<String, Query.BindVariable> bindValues, boolean wantFields) throws SQLException {
        throw new SQLFeatureNotSupportedException("unsupported multiquery for pullout subquery");
    }

    @Override
    public IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, Query.BindVariable> bindValues) throws SQLException {
        throw new SQLFeatureNotSupportedException("unsupported multiquery for pullout subquery");
    }

    @Override
    public IExecute.VtStream streamExecute(IContext ctx, Vcursor vcursor, Map<String, Query.BindVariable> bindValues, boolean wantFields) throws SQLException {
        BindVarsResponse bindVarsResponse = this.execSubqery(ctx, vcursor, bindValues, wantFields);
        return this.underlying.streamExecute(ctx, vcursor, bindVarsResponse.getBindValues(), wantFields);
    }

    @Override
    public VtResultSet getFields(Vcursor vcursor, Map<String, Query.BindVariable> bindValues) throws SQLException {
        Map<String, Query.BindVariable> combinedVars = new HashMap<>(bindValues);
        switch (this.opcode) {
            case PulloutValue:
                combinedVars.put(this.subqueryResult, SqlTypes.NULL_BIND_VARIABLE);
                break;
            case PulloutIn:
            case PulloutNotIn:
                combinedVars.put(this.hasValues, SqlTypes.valueBindVariable(VtValue.newVtValue(Query.Type.INT64, "0".getBytes())));
                List<VtValue> valueList = new ArrayList<VtValue>() {{
                    add(VtValue.newVtValue(Query.Type.INT64, "0".getBytes()));
                }};
                Query.BindVariable subBindVariable = SqlTypes.valuesBindVariable(Query.Type.TUPLE, valueList);
                combinedVars.put(this.subqueryResult, subBindVariable);
                break;
            case PulloutExists:
                combinedVars.put(this.hasValues, SqlTypes.valueBindVariable(VtValue.newVtValue(Query.Type.INT64, "0".getBytes())));
                break;
            default:
                break;
        }
        return this.underlying.getFields(vcursor, combinedVars);
    }

    @Override
    public Boolean canResolveShardQuery() {
        return Boolean.FALSE;
    }

    @Override
    public Boolean needsTransaction() {
        return this.underlying.needsTransaction() || this.subquery.needsTransaction();
    }

    @Getter
    @AllArgsConstructor
    public static class BindVarsResponse {
        private final Map<String, Query.BindVariable> bindValues;
    }
}
