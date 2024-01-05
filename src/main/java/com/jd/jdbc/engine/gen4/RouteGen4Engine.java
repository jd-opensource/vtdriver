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
import com.jd.jdbc.IExecute.ExecuteMultiShardResponse;
import com.jd.jdbc.common.tuple.Pair;
import com.jd.jdbc.common.util.CollectionUtils;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.key.DestinationAnyShard;
import com.jd.jdbc.planbuilder.Truncater;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqltypes.SqlTypes;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.BoundQuery;
import com.jd.jdbc.srvtopo.ResolvedShard;
import com.jd.jdbc.srvtopo.Resolver;
import com.jd.jdbc.vindexes.VKeyspace;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

public class RouteGen4Engine implements PrimitiveEngine, Truncater {

    /**
     * Query specifies the query to be executed.
     */
    @Setter
    @Getter
    private String query = "";

    /**
     * FieldQuery specifies the query to be executed for a GetFieldInfo request.
     */
    @Setter
    @Getter
    private String fieldQuery = "";

    @Setter
    private SQLSelectQuery selectFieldQuery;

    @Setter
    private String tableName = "";

    @Setter
    private SQLSelectQuery selectQuery;

    /**
     * TruncateColumnCount specifies the number of columns to return
     * in the final result. Rest of the columns are truncated
     * from the result received. If 0, no truncation happens.
     */
    @Getter
    private int truncateColumnCount = 0;

    /**
     * OrderBy specifies the key order for merge sorting. This will be
     * set only for scatter queries that need the results to be
     * merge-sorted.
     */
    @Getter
    private final List<OrderByParamsGen4> orderBy = new ArrayList<>();

    /**
     * NoRoutesSpecialHandling will make the route send a query to arbitrary shard if the routing logic can't find
     * the correct shard. This is important for queries where no matches does not mean empty result - examples would be:
     * select count(*) from tbl where lookupColumn = 'not there'
     * select exists(<subq>)
     */
    @Setter
    private boolean noRoutesSpecialHandling;

    @Getter
    private final RoutingParameters routingParameters = new RoutingParameters();

    public RouteGen4Engine(Engine.RouteOpcode routeOpcode, VKeyspace keyspace) {
        this.routingParameters.routeOpcode = routeOpcode;
        this.routingParameters.keyspace = keyspace;
    }

    public RouteGen4Engine(Engine.RouteOpcode routeOpcode, VKeyspace keyspace, String query, String fieldQuery, SQLSelectQuery selectQuery) {
        this.routingParameters.routeOpcode = routeOpcode;
        this.routingParameters.keyspace = keyspace;
        this.query = query;
        this.fieldQuery = fieldQuery;
        this.selectQuery = selectQuery;
    }

    @Override
    public String getKeyspaceName() {
        return this.routingParameters.keyspace.getName();
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }

    @Override
    public ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        VtResultSet vtResultSet = this.exec(vcursor, bindVariableMap, wantFields);
        return new ExecuteMultiShardResponse(vtResultSet.truncate(this.truncateColumnCount));
    }

    @Override
    public Boolean canResolveShardQuery() {
        return true;
    }

    /**
     * GetFields fetches the field info.
     *
     * @param vcursor
     * @param bindVariableMap
     * @return
     * @throws Exception
     */
    @Override
    public VtResultSet getFields(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        Resolver.ResolveDestinationResult resolveDestinationResult = vcursor.resolveDestinations(this.routingParameters.keyspace.getName(), null, Collections.singletonList(new DestinationAnyShard()));
        if (resolveDestinationResult == null) {
            throw new SQLException("no shards for keyspace: " + this.routingParameters.keyspace.getName());
        }
        List<ResolvedShard> resolvedShardList = resolveDestinationResult.getResolvedShards();
        if (resolvedShardList == null) {
            throw new SQLException("no shards for keyspace: " + this.routingParameters.keyspace.getName());
        }
        if (resolvedShardList.size() != 1) {
            throw new SQLException("no shards for keyspace: " + this.routingParameters.keyspace.getName());
        }
        ExecuteMultiShardResponse executeMultiShardResponse = Engine.execShard(vcursor, this.selectFieldQuery, bindVariableMap, resolvedShardList.get(0), false, false);
        return ((VtResultSet) executeMultiShardResponse.getVtRowList()).truncate(this.truncateColumnCount);
    }

    @Override
    public Boolean needsTransaction() {
        return false;
    }

    private VtResultSet exec(Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        Pair<List<ResolvedShard>, List<Map<String, BindVariable>>> pair = this.routingParameters.findRoute(vcursor, bindVariableMap);
        List<ResolvedShard> rss = pair.getLeft();
        List<Map<String, BindVariable>> bvs = pair.getRight();

        // No route
        if (CollectionUtils.isEmpty(rss)) {
            if (!this.noRoutesSpecialHandling) {
                if (wantFields) {
                    return this.getFields(vcursor, new HashMap<>(16, 1));
                }
                return new VtResultSet();
            }
            pair = this.routingParameters.paramsAnyShard(vcursor, bindVariableMap);
            rss = pair.getLeft();
            bvs = pair.getRight();
        }
        List<BoundQuery> queries = Engine.getQueriesGen4(this.selectQuery, bvs, null);
        ExecuteMultiShardResponse executeMultiShardResponse = vcursor.executeMultiShard(rss, queries, false, false);

        VtResultSet vtResultSet = (VtResultSet) executeMultiShardResponse.getVtRowList();

        if (CollectionUtils.isEmpty(orderBy)) {
            return vtResultSet;
        }
        return this.sort(vtResultSet);
    }

    /**
     * @param in
     * @return
     */
    private VtResultSet sort(VtResultSet in) throws SQLException {
        // Since Result is immutable, we make a copy.
        // The copy can be shallow because we won't be changing
        // the contents of any row.
        VtResultSet out = new VtResultSet(in.getFields(), in.getRows());
        List<VitessCompare> compares = VitessCompare.extractSlices(this.orderBy);
        ResultComparator comparator = new ResultComparator(compares);
        out.getRows().sort(comparator);
        if (comparator.getException() != null) {
            throw comparator.getException();
        }
        return out;
    }

    public IExecute.ExecuteMultiShardResponse executeAfterLookup(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap,
                                                                 boolean wantFields, VtValue[] ids, List<Destination> dest) throws SQLException {
        List<Query.Value> protoIds = new ArrayList<>(ids.length);

        for (VtValue id : ids) {
            protoIds.add(SqlTypes.vtValueToProto(id));
        }
        List<ResolvedShard> rss = vcursor.resolveDestinations(this.routingParameters.keyspace.getName(), protoIds, dest).getResolvedShards();

        List<Map<String, BindVariable>> bvs = new ArrayList<>(rss.size());
        for (ResolvedShard rs : rss) {
            bvs.add(bindVariableMap);
        }
        return this.executeShards(ctx, vcursor, bindVariableMap, wantFields, rss, bvs);

    }

    private IExecute.ExecuteMultiShardResponse executeShards(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVars,
                                                             boolean wantFields, List<ResolvedShard> rss, List<Map<String, BindVariable>> bvs) throws SQLException {
        if (this.routingParameters.routeOpcode == Engine.RouteOpcode.SelectNext) {
            // TODO
        }
        // No route
        if (rss.size() == 0) {
            if (!this.noRoutesSpecialHandling) {
                if (wantFields) {
                    return new ExecuteMultiShardResponse(this.getFields(vcursor, bindVars));
                }
                return new ExecuteMultiShardResponse(new VtResultSet());
            }
            // Here we were earlier returning no rows back.
            // But this was incorrect for queries like select count(*) from user where name='x'
            // If the lookup_vindex for name, returns no shards, we still want a result from here
            // with a single row with 0 as the output.
            // However, at this level it is hard to distinguish between the cases that need a result
            // and the ones that don't. So, we are sending the query to any shard! This is safe because
            // the query contains a predicate that make it not match any rows on that shard. (If they did,
            // we should have gotten that shard back already from findRoute)

            Pair<List<ResolvedShard>, List<Map<String, BindVariable>>> pair = this.routingParameters.paramsAnyShard(vcursor, bindVars);
            rss = pair.getLeft();
            bvs = pair.getRight();
        }
        //  getQuery()
        String charEncoding = vcursor.getCharEncoding();
        List<BoundQuery> queries = Engine.getQueriesGen4(this.selectQuery, bvs, charEncoding);
        IExecute.ExecuteMultiShardResponse results = vcursor.executeMultiShard(rss, queries, false, false);

        // TODO error process

        if (this.orderBy.size() == 0) {
            return results;
        }

        return new ExecuteMultiShardResponse(this.sort((VtResultSet) results.getVtRowList()));

    }

    @Override
    public void setTruncateColumnCount(Integer count) {
        this.truncateColumnCount = count;
    }

    private static class ResultComparator implements Comparator<List<VtResultValue>> {

        private final List<VitessCompare> comparers;

        @Getter
        private SQLException exception;

        ResultComparator(List<VitessCompare> comparers) {
            this.comparers = comparers;
        }

        @Override
        public int compare(List<VtResultValue> o1, List<VtResultValue> o2) {
            if (this.exception != null) {
                return -1;
            }
            // If there are any errors below, the function sets
            // the external err and returns true. Once err is set,
            // all subsequent calls return true. This will make
            // Slice think that all elements are in the correct
            // order and return more quickly.
            for (VitessCompare c : this.comparers) {
                int cmp;
                try {
                    cmp = c.compare(o1, o2);
                } catch (SQLException e) {
                    this.exception = e;
                    return -1;
                }
                if (cmp == 0) {
                    continue;
                }
                return cmp;
            }
            return 0;
        }
    }

    @Data
    @AllArgsConstructor
    public static class ParamsResponse {
        private List<ResolvedShard> resolvedShardList;

        private List<Map<String, BindVariable>> shardVarList;
    }
}
