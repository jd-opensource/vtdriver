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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;

@Getter
public class DistinctGen4Engine implements PrimitiveEngine {
    private PrimitiveEngine source;

    private List<CheckCol> checkCols;

    private boolean truncate;

    private ProbeTable probeTable;

    public DistinctGen4Engine(PrimitiveEngine source, List<CheckCol> checkCols, boolean truncate) {
        this.source = source;
        this.checkCols = checkCols;
        this.truncate = truncate;
    }

    @Override
    public String getKeyspaceName() {
        return this.source.getKeyspaceName();
    }

    @Override
    public String getTableName() {
        return this.source.getTableName();
    }

    @Override
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVars, boolean wantFields) throws SQLException {
        IExecute.ExecuteMultiShardResponse response = this.source.execute(ctx, vcursor, bindVars, wantFields);
        VtResultSet input = (VtResultSet) response.getVtRowList();

        VtResultSet result = new VtResultSet();
        result.setFields(input.getFields());

        ProbeTable pt = newProbeTable(this.checkCols);

        for (List<VtResultValue> row : input.getRows()) {
            boolean exists = pt.exists(row);
            if (!exists) {
                result.getRows().add(row);
            }
        }
        if (this.truncate) {
            return new IExecute.ExecuteMultiShardResponse(result.truncate(this.checkCols.size()));
        }
        return new IExecute.ExecuteMultiShardResponse(result);
    }

    private ProbeTable newProbeTable(List<CheckCol> checkCols) {
        List<CheckCol> cols = new ArrayList<>(checkCols);
        return new ProbeTable(new HashMap<>(), cols);
    }

    @Override
    public Boolean needsTransaction() {
        return this.source.needsTransaction();
    }

    @Override
    public VtResultSet getFields(Vcursor vcursor, Map<String, BindVariable> bindValues) throws SQLException {
        return this.source.getFields(vcursor, bindValues);
    }
}
