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
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.key.DestinationAnyShard;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtRowList;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.BoundQuery;
import com.jd.jdbc.srvtopo.ResolvedShard;
import com.jd.jdbc.srvtopo.Resolver;
import com.jd.jdbc.vindexes.VKeyspace;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SingleRowEngine implements PrimitiveEngine {

    private final SQLSelectQuery singleRowQuery;

    private final VKeyspace keyspace;

    public SingleRowEngine(SQLSelectQuery query, VKeyspace keyspace) {
        this.singleRowQuery = query;
        this.keyspace = keyspace;
    }

    @Override
    public String getKeyspaceName() {
        return this.keyspace.getName();
    }

    @Override
    public String getTableName() {
        return "";
    }

    @Override
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        return getExecuteMultiShardResponse();
    }

    @Override
    public IExecute.ExecuteMultiShardResponse mergeResult(VtResultSet vtResultSet, Map<String, BindVariable> bindValues, boolean wantFields) throws SQLException {
        return getExecuteMultiShardResponse();
    }

    @Override
    public IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindValues) throws SQLException {
        Resolver.ResolveDestinationResult resolveDestinationResult = vcursor.resolveDestinations(this.keyspace.getName(), null, new ArrayList<Destination>() {{
            add(new DestinationAnyShard());
        }});
        String charEncoding = vcursor.getCharEncoding();
        List<ResolvedShard> rsList = resolveDestinationResult.getResolvedShards();
        if (rsList.size() != 1) {
            throw new SQLException("Keyspace does not have exactly one shard: " + rsList);
        }
        List<BoundQuery> queries = Engine.getQueries(this.singleRowQuery, new ArrayList<Map<String, BindVariable>>() {{
            add(bindValues);
        }}, charEncoding);
        return new IExecute.ResolvedShardQuery(resolveDestinationResult.getResolvedShards(), queries);
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
        return new IExecute.VtStream() {
            @Override
            public VtRowList fetch(boolean wantFields) throws SQLException {
                List<List<VtResultValue>> rows = new ArrayList<>();
                rows.add(new ArrayList<>());
                return new VtResultSet(1, rows);
            }

            @Override
            public void close() throws SQLException {
            }
        };
    }

    @Override
    public Boolean needsTransaction() {
        return false;
    }

    private IExecute.ExecuteMultiShardResponse getExecuteMultiShardResponse() {
        List<List<VtResultValue>> rows = new ArrayList<>();
        rows.add(new ArrayList<>());
        VtResultSet resultSet = new VtResultSet(1, rows);
        return new IExecute.ExecuteMultiShardResponse(resultSet);
    }
}
