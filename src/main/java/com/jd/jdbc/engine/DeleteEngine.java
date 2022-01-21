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
import com.jd.jdbc.IExecute.ExecuteMultiShardResponse;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.key.DestinationAllShard;
import com.jd.jdbc.queryservice.util.RoleUtils;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLDeleteStatement;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.ResolvedShard;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
public class DeleteEngine extends DMLEngine implements PrimitiveEngine {

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
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        if (RoleUtils.notMaster(ctx)) {
            throw new SQLException("delete is not allowed for read only connection");
        }
        switch (super.opcode) {
            case Unsharded:
                return this.execDeleteUnsharded(vcursor, bindVariableMap);
            case Equal:
                return this.execDeleteEqual(vcursor, bindVariableMap);
            case In:
                return this.execDeleteIn(vcursor, bindVariableMap);
            case Scatter:
                return this.execDeleteByDestination(vcursor, bindVariableMap, new DestinationAllShard());
            case ByDestination:
                return this.execDeleteByDestination(vcursor, bindVariableMap, super.targetDestination);
            default:
                throw new SQLException("unsupported query route: " + super.opcode);
        }
    }

    @Override
    public Boolean needsTransaction() {
        return true;
    }

    private ExecuteMultiShardResponse execDeleteUnsharded(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        List<ResolvedShard> rsList = getResolvedShardsUnsharded(vcursor);
        return Engine.execShard(vcursor, super.query, bindVariableMap, rsList.get(0), true, true).setUpdate();
    }

    private ExecuteMultiShardResponse execDeleteEqual(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        ResolvedShard rs = getResolvedShardsEqual(vcursor, bindVariableMap).get(0);
        return Engine.execShard(vcursor, super.query, bindVariableMap, rs, true, true).setUpdate();
    }

    private ExecuteMultiShardResponse execDeleteIn(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        IExecute.ResolvedShardQuery rsq = resolveShardQueryIn(vcursor, bindVariableMap, null);
        return execMultiShard(vcursor, rsq.getRss(), rsq.getQueries(), super.multiShardAutocommit);
    }

    private ExecuteMultiShardResponse execDeleteByDestination(Vcursor vcursor, Map<String, BindVariable> bindVariableMap, Destination destination) throws SQLException {
        IExecute.ResolvedShardQuery rsq = resolveShardQueryByDestination(vcursor, bindVariableMap, destination, null);
        return execMultiShard(vcursor, rsq.getRss(), rsq.getQueries(), super.multiShardAutocommit);
    }

    @Override
    public void setSQLStatement(SQLStatement query) throws SQLException {
        if (query instanceof SQLDeleteStatement) {
            super.query = query;
            return;
        }
        throw new SQLException("Error, expect Delete Statement");
    }

    @Override
    public IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindValue) throws SQLException {
        switch (super.opcode) {
            case Unsharded:
                return resolveShardQueryUnsharded(vcursor, bindValue, null);
            case Equal:
                return resolveShardQueryEqual(vcursor, bindValue, null);
            case In:
                return resolveShardQueryIn(vcursor, bindValue, null);
            case Scatter:
                return resolveShardQueryByDestination(vcursor, bindValue, new DestinationAllShard(), null);
            case ByDestination:
                return resolveShardQueryByDestination(vcursor, bindValue, super.targetDestination, null);
            default:
                throw new SQLException("unsupported query route: " + super.opcode);
        }
    }

    @Override
    public IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindValue, Map<String, String> switchTableMap) throws SQLException {
        switch (super.opcode) {
            case Unsharded:
                return resolveShardQueryUnsharded(vcursor, bindValue, switchTableMap);
            case Equal:
                return resolveShardQueryEqual(vcursor, bindValue, switchTableMap);
            case In:
                return resolveShardQueryIn(vcursor, bindValue, switchTableMap);
            case Scatter:
                return resolveShardQueryByDestination(vcursor, bindValue, new DestinationAllShard(), switchTableMap);
            case ByDestination:
                return resolveShardQueryByDestination(vcursor, bindValue, super.targetDestination, null);
            default:
                throw new SQLException("unsupported query route: " + super.opcode);
        }
    }

    @Override
    public ExecuteMultiShardResponse mergeResult(VtResultSet vtResultSet, Map<String, BindVariable> bindVariable, boolean wantFields) {
        return new ExecuteMultiShardResponse(vtResultSet).setUpdate();
    }
}
