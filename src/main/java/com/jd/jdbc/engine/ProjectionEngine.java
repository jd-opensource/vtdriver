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
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtRowList;
import com.jd.jdbc.srvtopo.BindVariable;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ProjectionEngine implements PrimitiveEngine {

    List<String> cols;

    List<EvalEngine.Expr> exprs;

    PrimitiveEngine input;

    @Override
    public String getKeyspaceName() {
        return this.input.getKeyspaceName();
    }

    @Override
    public String getTableName() {
        return this.input.getKeyspaceName();
    }

    @Override
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        IExecute.ExecuteMultiShardResponse result = this.input.execute(ctx, vcursor, bindVariableMap, wantFields);
        VtResultSet resultSet = (VtResultSet) result.getVtRowList();

        return getExecuteMultiShardResponse(resultSet, bindVariableMap, wantFields);
    }

    @Override
    public IExecute.ExecuteMultiShardResponse mergeResult(VtResultSet resultSet, Map<String, BindVariable> bindValues, boolean wantFields) throws SQLException {
        IExecute.ExecuteMultiShardResponse executeMultiShardResponse = this.input.mergeResult(resultSet, bindValues, wantFields);
        VtResultSet vtResultSet = (VtResultSet) executeMultiShardResponse.getVtRowList();
        return getExecuteMultiShardResponse(vtResultSet, bindValues, wantFields);
    }

    @Override
    public IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindValueList) throws SQLException {
        IExecute.ResolvedShardQuery resolvedShardQuery = this.input.resolveShardQuery(ctx, vcursor, bindValueList);
        return new IExecute.ResolvedShardQuery(resolvedShardQuery.getRss(), resolvedShardQuery.getQueries());
    }

    @Override
    public IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindValueList, Map<String, String> switchTableMap) throws SQLException {
        IExecute.ResolvedShardQuery resolvedShardQuery = this.input.resolveShardQuery(ctx, vcursor, bindValueList, switchTableMap);
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
    public IExecute.VtStream streamExecute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindValues, boolean wantFields) throws SQLException {
        IExecute.VtStream vtStream = this.input.streamExecute(ctx, vcursor, bindValues, wantFields);
        return new IExecute.VtStream() {
            private IExecute.VtStream stream = vtStream;

            private boolean fetched = false;

            @Override
            public VtRowList fetch(boolean wantFields) throws SQLException {
                VtResultSet vtResultSet = new VtResultSet();
                if (fetched) {
                    return vtResultSet;
                }

                vtResultSet = (VtResultSet) stream.fetch(wantFields);
                EvalEngine.ExpressionEnv env = new EvalEngine.ExpressionEnv(bindValues);
                if (wantFields) {
                    addFields(vtResultSet, bindValues);
                }
                List<List<VtResultValue>> rows = new ArrayList<>();
                for (List<VtResultValue> row : vtResultSet.getRows()) {
                    env.setRow(row);
                    for (EvalEngine.Expr expr : exprs) {
                        EvalEngine.EvalResult res = expr.evaluate(env);
                        row.add(res.resultValue());
                    }
                    rows.add(row);
                }
                vtResultSet.setRows(rows);
                fetched = true;
                return vtResultSet;
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

    private void addFields(VtResultSet qr, Map<String, BindVariable> bindVariableMap) throws SQLException {
        EvalEngine.ExpressionEnv env = new EvalEngine.ExpressionEnv(bindVariableMap);
        Query.Field[] existedFields = qr.getFields();
        if (existedFields == null) {
            existedFields = new Query.Field[0];
        }

        Query.Field[] newFields = Arrays.copyOf(existedFields, existedFields.length + this.cols.size());
        for (int i = 0; i < this.cols.size(); i++) {
            String col = this.cols.get(i);
            Query.Type type = this.exprs.get(i).type(env);
            Query.Field newField = Query.Field.newBuilder().setName(col).setType(type).build();
            newFields[existedFields.length + i] = newField;
        }
        qr.setFields(newFields);
    }

    @Override
    public Boolean needsTransaction() {
        return false;
    }

    private IExecute.ExecuteMultiShardResponse getExecuteMultiShardResponse(VtResultSet resultSet, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        EvalEngine.ExpressionEnv env = new EvalEngine.ExpressionEnv(bindVariableMap);

        if (wantFields) {
            this.addFields(resultSet, bindVariableMap);
        }

        List<List<VtResultValue>> rows = new ArrayList<>();
        for (List<VtResultValue> row : resultSet.getRows()) {
            env.setRow(row);
            for (EvalEngine.Expr expr : this.exprs) {
                EvalEngine.EvalResult res = expr.evaluate(env);
                row.add(res.resultValue());
            }
            rows.add(row);
        }
        resultSet.setRows(rows);
        return new IExecute.ExecuteMultiShardResponse(resultSet);
    }
}
