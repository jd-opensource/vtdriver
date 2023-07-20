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

package com.jd.jdbc.engine.table;

import com.jd.jdbc.IExecute.ExecuteMultiShardResponse;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.engine.DeleteEngine;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.key.DestinationAllShard;
import com.jd.jdbc.queryservice.util.RoleUtils;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLDeleteStatement;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.srvtopo.BindVariable;
import java.sql.SQLException;
import java.util.Map;
import lombok.Data;

@Data
public class TableDeleteEngine extends TableDMLEngine implements PrimitiveEngine {

    private String tableName;

    @Override
    public String getKeyspaceName() {
        return super.keyspace.getName();
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }

    @Override
    public void setTableName(final String tableName) {
        this.tableName = tableName;
    }

    @Override
    public ExecuteMultiShardResponse execute(final IContext ctx, final Vcursor vcursor, final Map<String, BindVariable> bindVariableMap, final boolean wantFields) throws SQLException {
        if (RoleUtils.notMaster(ctx)) {
            throw new SQLException("delete is not allowed for read only connection");
        }
        switch (super.opcode) {
            case Equal:
                return this.execDeleteEqual(ctx, vcursor, bindVariableMap);
            case In:
                return this.execDeleteIn(ctx, vcursor, bindVariableMap);
            case Scatter:
                return this.execDeleteByDestination(ctx, vcursor, bindVariableMap, new DestinationAllShard());
            default:
                throw new SQLException("unsupported query route: " + super.opcode);
        }
    }

    @Override
    public Boolean needsTransaction() {
        return true;
    }

    private ExecuteMultiShardResponse execDeleteEqual(final IContext ctx, final Vcursor vcursor, final Map<String, BindVariable> bindVariableMap) throws SQLException {
        TableEngine.TableDestinationResponse response = getResolvedTablesEqual(bindVariableMap);
        return getExecuteMultiShardResponse(ctx, vcursor, bindVariableMap, response);
    }

    private ExecuteMultiShardResponse execDeleteIn(final IContext ctx, final Vcursor vcursor, final Map<String, BindVariable> bindVariableMap) throws SQLException {
        TableEngine.TableDestinationResponse response = resolveTablesQueryIn(bindVariableMap);
        return getExecuteMultiShardResponse(ctx, vcursor, bindVariableMap, response);
    }

    private ExecuteMultiShardResponse execDeleteByDestination(final IContext ctx, final Vcursor vcursor, final Map<String, BindVariable> bindVariableMap, final Destination destination)
        throws SQLException {
        TableEngine.TableDestinationResponse response = resolveAllTablesQuery(bindVariableMap);
        return getExecuteMultiShardResponse(ctx, vcursor, bindVariableMap, response);
    }

    @Override
    public void setSQLStatement(final SQLStatement query) throws SQLException {
        if (query instanceof SQLDeleteStatement) {
            super.query = query;
            return;
        }
        throw new SQLException("Error, expect Delete Statement");
    }

    private ExecuteMultiShardResponse getExecuteMultiShardResponse(final IContext ctx, final Vcursor vcursor, final Map<String, BindVariable> bindVariableMap,
                                                                   final TableEngine.TableDestinationResponse response) throws SQLException {
        if (response == null) {
            return new ExecuteMultiShardResponse(new VtResultSet());
        }
        if (!(super.executeEngine instanceof DeleteEngine)) {
            throw new SQLException();
        }
        VtResultSet tableBatchExecuteResult = TableEngine.getTableBatchExecuteResult(ctx, super.executeEngine, vcursor, bindVariableMap, response);
        return new ExecuteMultiShardResponse(tableBatchExecuteResult).setUpdate();
    }
}
