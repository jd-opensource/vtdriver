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
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtRowList;
import com.jd.jdbc.srvtopo.BindVariable;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
public class SubQueryEngine implements PrimitiveEngine {
    private final List<Integer> cols = new ArrayList<>();

    private PrimitiveEngine subqueryEngine;

    @Override
    public String getKeyspaceName() {
        return null;
    }

    @Override
    public String getTableName() {
        return null;
    }

    @Override
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        IExecute.ExecuteMultiShardResponse inner = this.subqueryEngine.execute(ctx, vcursor, bindVariableMap, wantFields);
        VtResultSet result = this.buildResult(inner.getVtRowList());
        return new IExecute.ExecuteMultiShardResponse(result);
    }

    /**
     * buildResult builds a new result by pulling the necessary coluns from
     * the subquery in the requested order
     *
     * @param inner
     * @return
     */
    private VtResultSet buildResult(VtRowList inner) {
        VtResultSet innerResult = (VtResultSet) inner;

        VtResultSet result = new VtResultSet();
        result.setFields(innerResult.getFields());
        result.setRows(new ArrayList<>(innerResult.getRows().size()));

        for (List<VtResultValue> innerRow : innerResult.getRows()) {
            List<VtResultValue> row = new ArrayList<>(this.cols.size());
            for (Integer col : cols) {
                row.add(innerRow.get(col));
            }
            result.getRows().add(row);
        }
        result.setRowsAffected(innerResult.getRowsAffected());
        return result;
    }

    @Override
    public IExecute.ExecuteMultiShardResponse mergeResult(VtResultSet vtResultSet, Map<String, BindVariable> bindValues, boolean wantFields) throws SQLException {
        IExecute.ExecuteMultiShardResponse inner = this.subqueryEngine.mergeResult(vtResultSet, bindValues, wantFields);
        VtResultSet result = this.buildResult(inner.getVtRowList());
        return new IExecute.ExecuteMultiShardResponse(result);
    }

    @Override
    public IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindValues) throws SQLException {
        IExecute.ResolvedShardQuery resolvedShardQuery = this.subqueryEngine.resolveShardQuery(ctx, vcursor, bindValues);
        return new IExecute.ResolvedShardQuery(resolvedShardQuery.getRss(), resolvedShardQuery.getQueries());
    }

    @Override
    public IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindValues, Map<String, String> switchTableMap) throws SQLException {
        IExecute.ResolvedShardQuery resolvedShardQuery = this.subqueryEngine.resolveShardQuery(ctx, vcursor, bindValues, switchTableMap);
        return new IExecute.ResolvedShardQuery(resolvedShardQuery.getRss(), resolvedShardQuery.getQueries());
    }

    @Override
    public Boolean canResolveShardQuery() {
        return this.subqueryEngine.canResolveShardQuery();
    }

    @Override
    public IExecute.VtStream streamExecute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindValues, boolean wantFields) throws SQLException {
        IExecute.VtStream vtStream = this.subqueryEngine.streamExecute(ctx, vcursor, bindValues, wantFields);
        return new IExecute.VtStream() {
            private IExecute.VtStream stream = vtStream;

            @Override
            public VtRowList fetch(boolean wantFields) throws SQLException {
                VtRowList vtRowList = stream.fetch(wantFields);
                return buildResult(vtRowList);
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

    @Override
    public VtResultSet getFields(Vcursor vcursor, Map<String, BindVariable> bindValues) throws SQLException {
        VtResultSet inner = this.subqueryEngine.getFields(vcursor, bindValues);
        VtResultSet resultSet = new VtResultSet();
        resultSet.setFields(this.buildFields(inner));
        return resultSet;
    }

    private Query.Field[] buildFields(VtResultSet inner) {
        if (inner.getFields().length == 0) {
            return null;
        }
        Query.Field[] fields = new Query.Field[this.cols.size()];
        for (int i = 0; i < this.cols.size(); i++) {
            fields[i] = inner.getFields()[this.cols.get(i)];
        }
        return fields;
    }

    @Override
    public Boolean needsTransaction() {
        return this.subqueryEngine.needsTransaction();
    }
}
