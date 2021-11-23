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

import com.jd.jdbc.IExecute;
import com.jd.jdbc.common.Constant;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.UpdateEngine;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLUpdateStatement;
import com.jd.jdbc.sqltypes.VtResultSet;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.sql.SQLException;
import java.util.Map;

public class TableUpdateEngine extends TableDMLEngine implements PrimitiveEngine {
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
    public IExecute.ExecuteMultiShardResponse execute(final IContext ctx, final Vcursor vcursor, final Map<String, Query.BindVariable> bindVariableMap, final boolean wantFields) throws SQLException {
        if (ctx.getContextValue(Constant.DRIVER_PROPERTY_ROLE_KEY) != Topodata.TabletType.MASTER) {
            throw new SQLException("delete is not allowed for read only connection");
        }
        switch (super.opcode) {
            case Equal:
                return this.execUpdateEqual(ctx, vcursor, bindVariableMap);
            case In:
                return this.execUpdateIn(ctx, vcursor, bindVariableMap);
            case Scatter:
                return this.execUpdateByDestination(ctx, vcursor, bindVariableMap);
            default:
                throw new SQLException("unsupported query route: " + super.opcode);
        }
    }

    @Override
    public Boolean needsTransaction() {
        return true;
    }

    @Override
    public void setSQLStatement(final SQLStatement query) throws SQLException {
        if (query instanceof SQLUpdateStatement) {
            this.query = query;
            return;
        }
        throw new SQLException("Error, expect update statement");
    }

    private IExecute.ExecuteMultiShardResponse execUpdateEqual(final IContext ctx, final Vcursor vcursor, final Map<String, Query.BindVariable> bindVariableMap) throws SQLException {
        Engine.TableDestinationResponse response = getResolvedShardsEqual(bindVariableMap);
        return getExecuteMultiShardResponse(ctx, vcursor, bindVariableMap, response);
    }


    private IExecute.ExecuteMultiShardResponse execUpdateIn(final IContext ctx, final Vcursor vcursor, final Map<String, Query.BindVariable> bindVariableMap) throws SQLException {
        Engine.TableDestinationResponse response = resolveShardQueryIn(bindVariableMap);
        return getExecuteMultiShardResponse(ctx, vcursor, bindVariableMap, response);
    }

    private IExecute.ExecuteMultiShardResponse execUpdateByDestination(final IContext ctx, final Vcursor vcursor, final Map<String, Query.BindVariable> bindVariableMap) throws SQLException {
        Engine.TableDestinationResponse response = resolveAllShardQuery(bindVariableMap);
        return getExecuteMultiShardResponse(ctx, vcursor, bindVariableMap, response);
    }

    private IExecute.ExecuteMultiShardResponse getExecuteMultiShardResponse(final IContext ctx, final Vcursor vcursor, final Map<String, Query.BindVariable> bindVariableMap,
                                                                            final Engine.TableDestinationResponse response) throws SQLException {
        if (response == null) {
            return new IExecute.ExecuteMultiShardResponse(new VtResultSet());
        }
        if (!(super.executeEngine instanceof UpdateEngine)) {
            throw new SQLException();
        }
        VtResultSet tableBatchExecuteResult = Engine.getTableBatchExecuteResult(ctx, super.executeEngine, vcursor, bindVariableMap, response);
        return new IExecute.ExecuteMultiShardResponse(tableBatchExecuteResult).setUpdate();
    }
}