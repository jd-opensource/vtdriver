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

import com.jd.jdbc.IExecute;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.srvtopo.BindVariable;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

// SimpleProjection selects which columns to keep from the input
@Getter
@Setter
public class SimpleProjectionGen4Engine implements PrimitiveEngine {
    // Cols defines the column numbers from the underlying primitive
    // to be returned.
    private List<Integer> cols = new ArrayList<>();

    private PrimitiveEngine input;


    @Override
    public String getKeyspaceName() {
        return this.getInput().getKeyspaceName();
    }

    @Override
    public String getTableName() {
        return this.getInput().getTableName();
    }

    @Override
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        IExecute.ExecuteMultiShardResponse inner = this.getInput().execute(ctx, vcursor, bindVariableMap, wantFields);
        return new IExecute.ExecuteMultiShardResponse(this.buildResult((VtResultSet) inner.getVtRowList()));
    }

    @Override
    public Boolean needsTransaction() {
        return this.getInput().needsTransaction();
    }

    @Override
    public VtResultSet getFields(Vcursor vcursor, Map<String, BindVariable> bindvars) throws SQLException {
        VtResultSet inner = this.input.getFields(vcursor, bindvars);
        VtResultSet ret = new VtResultSet();
        ret.setFields(this.buildFields(inner).toArray(new Query.Field[0]));
        return ret;
    }

    @Override
    public List<PrimitiveEngine> inputs() {
        return Arrays.asList(this.input);
    }


    private VtResultSet buildResult(VtResultSet inner) {
        VtResultSet newInner = new VtResultSet();
        newInner.setFields(this.buildFields(inner).toArray(new Query.Field[0]));

        List<List<VtResultValue>> rows = new ArrayList<>(inner.getRows().size());

        for (List<VtResultValue> innerRow : inner.getRows()) {
            List<VtResultValue> newRow = new ArrayList<>(this.cols.size());
            for (int col : this.cols) {
                newRow.add(innerRow.get(col));
            }
            rows.add(newRow);
        }
        newInner.setRows(rows);
        newInner.setRowsAffected(inner.getRowsAffected());
        return newInner;
    }

    private List<Query.Field> buildFields(VtResultSet inner) {
        if (inner.getFields().length == 0) {
            return null;
        }
        List<Query.Field> fields = new ArrayList<>(inner.getFields().length);

        for (int col : this.cols) {
            fields.add(inner.getFields()[col]);
        }
        return fields;
    }
}
