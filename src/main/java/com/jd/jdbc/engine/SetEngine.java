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
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.srvtopo.BindVariable;
import java.sql.SQLException;
import java.util.Map;

import static com.jd.jdbc.IExecute.ExecuteMultiShardResponse;

public class SetEngine extends BasePrimitiveEngine implements PrimitiveEngine {

    public SetEngine(String keyspaceName, String tableName, String vindex, SQLStatement stmt) {
        super(keyspaceName, tableName, vindex, stmt);
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
    public ExecuteMultiShardResponse execute(IContext ctx, Vcursor cursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) {
        return null;
    }

    @Override
    public IExecute.ExecuteMultiShardResponse mergeResult(VtResultSet vtResultSet, Map<String, BindVariable> bindValues, boolean wantFields) throws SQLException {
        return null;
    }

    /**
     * @return
     */
    @Override
    public Boolean needsTransaction() {
        return false;
    }
}
