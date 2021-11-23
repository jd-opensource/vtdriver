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
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.key.DestinationAllShard;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqltypes.VtPlanValue;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.srvtopo.ResolvedShard;
import com.jd.jdbc.srvtopo.Resolver;
import com.jd.jdbc.vindexes.SingleColumn;
import com.jd.jdbc.vindexes.VKeyspace;
import com.jd.jdbc.vindexes.hash.BinaryHash;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.Setter;
import vschema.Vschema;

@Getter
@Setter
public abstract class DMLEngine {
    // Opcode is the execution opcode.
    protected DMLOpcode opcode;

    // Keyspace specifies the keyspace to send the query to.
    protected VKeyspace keyspace;

    // TargetDestination specifies the destination to send the query to.
    protected Destination targetDestination;

    // Query specifies the query to be executed.
    protected SQLStatement query;

    // Vindex specifies the vindex to be used.
    protected SingleColumn vindex;

    // Values specifies the vindex values to use for routing.
    // For now, only one value is specified.
    protected List<VtPlanValue> vtPlanValueList = new ArrayList<>();

    // Keyspace Id Vindex
    protected SingleColumn ksidVindex;

    // Table specifies the table for the update.
    protected Vschema.Table table;

    // OwnedVindexQuery is used for updating changes in lookup vindexes.
    protected String ownedVindexQuery;

    // Option to override the standard behavior and allow a multi-shard update
    // to use single round trip autocommit.
    protected boolean multiShardAutocommit;

    // QueryTimeout contains the optional timeout (in milliseconds) to apply to this query
    protected int queryTimeout;

    public IExecute.ExecuteMultiShardResponse execMultiShard(Vcursor vcursor, List<ResolvedShard> rss, List<Query.BoundQuery> queries, Boolean multiShardAutocommit) throws SQLException {
        boolean autocommit = (rss.size() == 1 || multiShardAutocommit) && vcursor.autocommitApproval();
        return vcursor.executeMultiShard(rss, queries, true, autocommit).setUpdate();
    }

    public ResolvedShard resolveSingleShard(Vcursor vcursor, SingleColumn vindex, VKeyspace keyspace, VtValue vtValue) throws SQLException {
        Destination[] destinations = new BinaryHash().map(new VtValue[] {vtValue});

        Resolver.ResolveDestinationResult resolveDestinationResult = vcursor.resolveDestinations(keyspace.getName(), null, Arrays.asList(destinations));
        List<ResolvedShard> rsList = resolveDestinationResult.getResolvedShards();
        if (rsList.size() != 1) {
            throw new SQLException("ResolveDestinations maps to " + rsList.size() + " shards");
        }
        return rsList.get(0);
    }

    protected List<ResolvedShard> getResolvedShardsUnsharded(Vcursor vcursor) throws SQLException {
        Resolver.ResolveDestinationResult resolveDestinationResult = vcursor.resolveDestinations(
            keyspace.getName(), null, new ArrayList<Destination>() {{
                add(new DestinationAllShard());
            }});
        List<ResolvedShard> rsList = resolveDestinationResult.getResolvedShards();
        if (rsList.size() != 1) {
            throw new SQLException("Keyspace does not have exactly one shard: " + rsList);
        }
        Engine.allowOnlyMaster(rsList);
        return rsList;
    }

    protected List<ResolvedShard> getResolvedShardsEqual(Vcursor vcursor, Map<String, Query.BindVariable> bindValue) throws SQLException {
        VtValue value = vtPlanValueList.get(0).resolveValue(bindValue);
        Destination[] destinations = new BinaryHash().map(new VtValue[] {value});
        Resolver.ResolveDestinationResult resolveDestinationResult = vcursor.resolveDestinations(keyspace.getName(), null, Arrays.asList(destinations));
        List<ResolvedShard> rsList = resolveDestinationResult.getResolvedShards();
        if (rsList.size() != 1) {
            throw new SQLException("ResolveDestinations maps to " + rsList.size() + " shards");
        }
        Engine.allowOnlyMaster(rsList);
        return rsList;
    }

    protected List<ResolvedShard> getResolvedShardsIn(Vcursor vcursor, Map<String, Query.BindVariable> bindValue) throws SQLException {
        List<VtValue> vtValues = this.vtPlanValueList.get(0).resolveList(bindValue);
        Resolver.ResolveDestinationResult resolveDestinationResult = Engine.resolveShards(vcursor, vindex, keyspace, vtValues);
        List<ResolvedShard> rsList = resolveDestinationResult.getResolvedShards();
        Engine.allowOnlyMaster(rsList);
        return rsList;
    }

    protected List<ResolvedShard> getResolvedShardsByDestination(Vcursor vcursor, Destination destination) throws SQLException {
        Resolver.ResolveDestinationResult resolveDestinationResult = vcursor.resolveDestinations(keyspace.getName(), null, new ArrayList<Destination>() {{
            add(destination);
        }});
        List<ResolvedShard> rsList = resolveDestinationResult.getResolvedShards();
        Engine.allowOnlyMaster(rsList);
        return rsList;
    }

    protected IExecute.ResolvedShardQuery resolveShardQueryUnsharded(Vcursor vcursor, Map<String, Query.BindVariable> bindValue, Map<String, String> switchTableMap) throws SQLException {
        List<ResolvedShard> rsList = getResolvedShardsUnsharded(vcursor);
        String charEncoding = vcursor.getCharEncoding();
        List<Query.BoundQuery> queries = Engine.getQueries(this.query, new ArrayList<Map<String, Query.BindVariable>>() {{
            add(bindValue);
        }}, switchTableMap, charEncoding);
        return new IExecute.ResolvedShardQuery(rsList, queries);
    }

    protected IExecute.ResolvedShardQuery resolveShardQueryEqual(Vcursor vcursor, Map<String, Query.BindVariable> bindValue) throws SQLException {
        List<ResolvedShard> rsList = getResolvedShardsEqual(vcursor, bindValue);
        String charEncoding = vcursor.getCharEncoding();
        List<Query.BoundQuery> queries = Engine.getQueries(this.query, new ArrayList<Map<String, Query.BindVariable>>() {{
            add(bindValue);
        }}, charEncoding);
        return new IExecute.ResolvedShardQuery(rsList, queries);
    }

    protected IExecute.ResolvedShardQuery resolveShardQueryEqual(Vcursor vcursor, Map<String, Query.BindVariable> bindValue, Map<String, String> switchTables) throws SQLException {
        String charEncoding = vcursor.getCharEncoding();
        List<ResolvedShard> rsList = getResolvedShardsEqual(vcursor, bindValue);
        List<Query.BoundQuery> queries = Engine.getQueries(this.query, new ArrayList<Map<String, Query.BindVariable>>() {{
            add(bindValue);
        }}, switchTables, charEncoding);
        return new IExecute.ResolvedShardQuery(rsList, queries);
    }

    protected IExecute.ResolvedShardQuery resolveShardQueryIn(Vcursor vcursor, Map<String, Query.BindVariable> bindValue, Map<String, String> switchTables) throws SQLException {
        String charEncoding = vcursor.getCharEncoding();
        List<ResolvedShard> rsList = getResolvedShardsIn(vcursor, bindValue);
        List<Map<String, Query.BindVariable>> bindVariableMapList = IntStream.range(0, rsList.size()).mapToObj(i -> bindValue).collect(Collectors.toList());
        List<Query.BoundQuery> queries = Engine.getQueries(this.query, bindVariableMapList, switchTables, charEncoding);
        return new IExecute.ResolvedShardQuery(rsList, queries);
    }

    protected IExecute.ResolvedShardQuery resolveShardQueryByDestination(Vcursor vcursor, Map<String, Query.BindVariable> bindValue, Destination destination, Map<String, String> switchTables)
        throws SQLException {
        String charEncoding = vcursor.getCharEncoding();
        List<ResolvedShard> rsList = getResolvedShardsByDestination(vcursor, destination);
        List<Map<String, Query.BindVariable>> bindVariableMapList = IntStream.range(0, rsList.size()).mapToObj(i -> bindValue).collect(Collectors.toList());
        List<Query.BoundQuery> queries = Engine.getQueries(query, bindVariableMapList, switchTables, charEncoding);
        return new IExecute.ResolvedShardQuery(rsList, queries);
    }

    public abstract void setSQLStatement(SQLStatement query) throws SQLException;

    public abstract void setTableName(String tableName);
}