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
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.key.DestinationAllShard;
import com.jd.jdbc.key.DestinationAnyShard;
import com.jd.jdbc.queryservice.StreamIterator;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqltypes.SqlTypes;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.BoundQuery;
import com.jd.jdbc.srvtopo.ResolvedShard;
import com.jd.jdbc.srvtopo.Resolver;
import com.jd.jdbc.vindexes.SingleColumn;
import com.jd.jdbc.vindexes.VKeyspace;
import io.netty.util.internal.StringUtil;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Route represents the instructions to route a read query to
 * one or many vttablets.
 */
@Data
public class RouteEngine extends AbstractRouteEngine {
    /**
     * RouteOpcode is a number representing the opcode
     * for the Route primitve
     */
    private Engine.RouteOpcode routeOpcode;

    /**
     * TargetDestination specifies an explicit target destination to send the query to.
     * This bypases the core of the v3 engine.
     */
    private Destination targetDestination;

    /**
     * TargetTabletType specifies an explicit target destination tablet type
     * this is only used in conjunction with TargetDestination
     */
    private Topodata.TabletType targetTabletType;

    /**
     * Query specifies the query to be executed.
     */
    private String query = "";

    private String fieldQuery = "";

    private SQLSelectQuery selectFieldQuery;

    /**
     * Vindex specifies the vindex to be used.
     */
    private SingleColumn vindex;

    /**
     * ueryTimeout contains the optional timeout (in milliseconds) to apply to this query
     */
    private Integer queryTimeout = 0;

    /**
     * ScatterErrorsAsWarnings is true if results should be returned even if some shards have an error
     */
    private Boolean scatterErrorsAsWarnings = false;

    /**
     * SysTableKeyspaceExpr contains the schema expressions
     * It will be used to route the system table queries to a keyspace.
     */
    private List<EvalEngine.Expr> sysTableKeyspaceExpr = new ArrayList<>();

    private boolean isQueryPinnedTable;

    private String pinned = "";

    public RouteEngine(Engine.RouteOpcode routeOpcode, VKeyspace keyspace) {
        this.routeOpcode = routeOpcode;
        this.keyspace = keyspace;
    }

    public RouteEngine(Engine.RouteOpcode routeOpcode, VKeyspace keyspace, boolean isQueryPinnedTable, String pinned) {
        this.routeOpcode = routeOpcode;
        this.keyspace = keyspace;
        this.isQueryPinnedTable = isQueryPinnedTable;
        this.pinned = pinned;
    }

    @Override
    public ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        VtResultSet vtResultSet = this.exec(vcursor, bindVariableMap, wantFields);
        return new ExecuteMultiShardResponse(vtResultSet.truncate(this.truncateColumnCount));
    }

    @Override
    public ExecuteMultiShardResponse mergeResult(VtResultSet vtResultSet, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        if (this.orderBy == null || this.orderBy.isEmpty()) {
            return new ExecuteMultiShardResponse(vtResultSet.truncate(this.truncateColumnCount));
        }
        vtResultSet = this.sort(vtResultSet);
        return new ExecuteMultiShardResponse(vtResultSet.truncate(this.truncateColumnCount));
    }

    @Override
    public IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        ParamsResponse paramsResponse = getResolveDestinationResult(vcursor, bindVariableMap);
        if (paramsResponse == null || paramsResponse.getResolvedShardList() == null || paramsResponse.getResolvedShardList().isEmpty()) {
            return getFieldResolvedShardQuery(vcursor);
        }
        String charEncoding = vcursor.getCharEncoding();
        List<BoundQuery> queries = Engine.getQueries(this.selectQuery, paramsResponse.getShardVarList(), charEncoding);
        return new IExecute.ResolvedShardQuery(paramsResponse.getResolvedShardList(), queries);
    }

    @Override
    public IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap, Map<String, String> switchTableMap) throws SQLException {
        ParamsResponse paramsResponse = getResolveDestinationResult(vcursor, bindVariableMap);
        if (paramsResponse == null || paramsResponse.getResolvedShardList() == null || paramsResponse.getResolvedShardList().isEmpty()) {
            return getFieldResolvedShardQuery(vcursor);
        }
        String charEncoding = vcursor.getCharEncoding();
        List<BoundQuery> queries = Engine.getQueries(this.selectQuery, paramsResponse.getShardVarList(), switchTableMap, charEncoding);
        return new IExecute.ResolvedShardQuery(paramsResponse.getResolvedShardList(), queries);
    }

    @Override
    public Boolean canResolveShardQuery() {
        return Boolean.TRUE;
    }

    /**
     * @param ctx
     * @param vcursor
     * @param bindVariableMap
     * @param wantFields
     * @return
     * @throws Exception
     */
    @Override
    public IExecute.VtStream streamExecute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        return this.streamExec(vcursor, bindVariableMap, wantFields);
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
        Resolver.ResolveDestinationResult resolveDestinationResult = vcursor.resolveDestinations(this.keyspace.getName(), null, new ArrayList<Destination>() {{
            add(new DestinationAnyShard());
        }});
        if (resolveDestinationResult == null) {
            throw new SQLException("no shards for keyspace: " + this.keyspace.getName());
        }
        List<ResolvedShard> resolvedShardList = resolveDestinationResult.getResolvedShards();
        if (resolvedShardList == null) {
            throw new SQLException("no shards for keyspace: " + this.keyspace.getName());
        }
        if (resolvedShardList.size() != 1) {
            throw new SQLException("no shards for keyspace: " + this.keyspace.getName());
        }
        ExecuteMultiShardResponse executeMultiShardResponse = Engine.execShard(vcursor, this.selectFieldQuery, bindVariableMap, resolvedShardList.get(0), false, false);
        return ((VtResultSet) executeMultiShardResponse.getVtRowList()).truncate(this.truncateColumnCount);
    }

    @Override
    public Boolean needsTransaction() {
        return false;
    }

    private IExecute.ResolvedShardQuery getFieldResolvedShardQuery(Vcursor vcursor) throws SQLException {
        Resolver.ResolveDestinationResult resolveDestinationResult = vcursor.resolveDestinations(this.keyspace.getName(), null, Collections.singletonList(new DestinationAnyShard()));
        if (resolveDestinationResult == null
            || resolveDestinationResult.getResolvedShards() == null
            || resolveDestinationResult.getResolvedShards().size() != 1) {
            throw new SQLException("no shards for keyspace: " + this.keyspace.getName());
        }

        List<BoundQuery> queries = Collections.singletonList(new BoundQuery(fieldQuery));
        return new IExecute.ResolvedShardQuery(resolveDestinationResult.getResolvedShards(), queries);
    }

    private VtResultSet exec(Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        ParamsResponse paramsResponse = getResolveDestinationResult(vcursor, bindVariableMap);
        String charEncoding = vcursor.getCharEncoding();
        // No route
        if (paramsResponse == null || paramsResponse.getResolvedShardList() == null || paramsResponse.getResolvedShardList().isEmpty()) {
            if (wantFields) {
                return this.getFields(vcursor, new HashMap<>(16, 1));
            }
            return new VtResultSet();
        }
        List<BoundQuery> queries = Engine.getQueries(this.selectQuery, paramsResponse.getShardVarList(), charEncoding);
        ExecuteMultiShardResponse executeMultiShardResponse = vcursor.executeMultiShard(paramsResponse.getResolvedShardList(), queries, false, false);

        VtResultSet vtResultSet = (VtResultSet) executeMultiShardResponse.getVtRowList();

        if (this.orderBy == null || this.orderBy.isEmpty()) {
            return vtResultSet;
        }
        return this.sort(vtResultSet);
    }

    /**
     * @param vcursor
     * @param bindVariableMap
     * @param wantFields
     * @return
     * @throws Exception
     */
    private IExecute.VtStream streamExec(Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        ParamsResponse paramsResponse = getResolveDestinationResult(vcursor, bindVariableMap);
        String charEncoding = vcursor.getCharEncoding();
        List<StreamIterator> iteratorList = null;

        if (this.selectQuery instanceof MySqlSelectQueryBlock) {
            if (((MySqlSelectQueryBlock) this.selectQuery).isForUpdate()) {
                throw new SQLFeatureNotSupportedException("Select with lock not allowed for streaming");
            }
        }

        List<BoundQuery> queries;
        List<ResolvedShard> shards;
        if (paramsResponse == null || paramsResponse.getResolvedShardList() == null || paramsResponse.getResolvedShardList().isEmpty()) {
            IExecute.ResolvedShardQuery shardQuery = getFieldResolvedShardQuery(vcursor);
            queries = shardQuery.getQueries();
            shards = shardQuery.getRss();
        } else {
            queries = Engine.getQueries(this.selectQuery, paramsResponse.getShardVarList(), charEncoding);
            shards = paramsResponse.getResolvedShardList();
        }

        iteratorList = vcursor.streamExecuteMultiShard(shards, queries);
        return new RouteStream(iteratorList, orderBy, truncateColumnCount, this, vcursor, bindVariableMap);
    }

    /**
     * @param vcursor
     * @param bindVariableMap
     * @return
     * @throws Exception
     */
    private ParamsResponse getResolveDestinationResult(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        ParamsResponse paramsResponse;
        switch (routeOpcode) {
            case SelectDBA:
                paramsResponse = this.paramsSystemQuery(vcursor, bindVariableMap);
                break;
            case SelectUnsharded:
            case SelectReference:
                paramsResponse = this.paramsAnyShard(vcursor, bindVariableMap);
                break;
            case SelectScatter:
                paramsResponse = this.paramsAllShard(vcursor, bindVariableMap);
                break;
            case SelectEqual:
            case SelectEqualUnique:
            case SelectNext:
                paramsResponse = this.paramsSelectEqual(vcursor, bindVariableMap);
                break;
            case SelectIN:
                paramsResponse = this.paramsSelectIn(vcursor, bindVariableMap);
                break;
            case SelectNone:
                paramsResponse = new ParamsResponse(new ArrayList<>(), new ArrayList<>());
                break;
            default:
                // Unreachable.
                throw new SQLException("unsupported query route: " + routeOpcode);
        }
        return paramsResponse;
    }

    /**
     * @param vcursor
     * @return
     * @throws Exception
     */
    private ParamsResponse paramsAllShard(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        Resolver.ResolveDestinationResult resolveDestinationResult = vcursor.resolveDestinations(
            this.keyspace.getName(), null, new ArrayList<Destination>() {{
                add(new DestinationAllShard());
            }});
        List<ResolvedShard> rss = resolveDestinationResult.getResolvedShards();
        return new ParamsResponse(rss, IntStream.range(0, rss.size()).mapToObj(i -> bindVariableMap).collect(Collectors.toList()));
    }

    /**
     * @param vcursor
     * @return
     * @throws Exception
     */
    public ParamsResponse paramsSystemQuery(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        String keyspace = "";
        boolean schemaExists = false;
        EvalEngine.ExpressionEnv env = new EvalEngine.ExpressionEnv(bindVariableMap);
        for (EvalEngine.Expr expr : this.sysTableKeyspaceExpr) {
            EvalEngine.EvalResult evalResult = expr.evaluate(env);
            String other = evalResult.value().toString();
            if (StringUtil.isNullOrEmpty(keyspace)) {
                keyspace = other;
                bindVariableMap.put(SqlTypes.BV_SCHEMA_NAME, SqlTypes.stringBindVariable(keyspace));
                schemaExists = true;
            } else if (!other.equalsIgnoreCase(keyspace)) {
                throw new SQLException("can't use more than one keyspace per system table query - found both '" + keyspace + "' and '" + other + "'");
            }
        }

        if (StringUtil.isNullOrEmpty(keyspace)) {
            keyspace = this.keyspace.getName();
        }

        List<ResolvedShard> destinations;
        try {
            Resolver.ResolveDestinationResult resolveDestinationResult = vcursor.resolveDestinations(
                keyspace, null, new ArrayList<Destination>() {{
                    add(new DestinationAnyShard());
                }});
            destinations = resolveDestinationResult.getResolvedShards();
            if (schemaExists) {
                bindVariableMap.put(SqlTypes.BV_REPLACE_SCHEMA_NAME, SqlTypes.int64BindVariable(1L));
            }
        } catch (Exception e) {
            try {
                Resolver.ResolveDestinationResult resolveDestinationResult = vcursor.resolveDestinations(
                    this.keyspace.getName(), null, new ArrayList<Destination>() {{
                        add(new DestinationAnyShard());
                    }});
                destinations = resolveDestinationResult.getResolvedShards();
            } catch (Exception e1) {
                throw new SQLException("failed to find information about keyspace `" + keyspace + "`");
            }
        }
        return new ParamsResponse(destinations, new ArrayList<Map<String, BindVariable>>() {{
            add(bindVariableMap);
        }});
    }

    /**
     * @param vcursor
     * @return
     * @throws Exception
     */
    private ParamsResponse paramsAnyShard(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        Resolver.ResolveDestinationResult resolveDestinationResult = vcursor.resolveDestinations(
            this.keyspace.getName(), null, new ArrayList<Destination>() {{
                add(new DestinationAnyShard());
            }});
        List<ResolvedShard> rss = resolveDestinationResult.getResolvedShards();
        return new ParamsResponse(rss, IntStream.range(0, rss.size()).mapToObj(i -> bindVariableMap).collect(Collectors.toList()));
    }

    /**
     * @param vcursor
     * @return
     * @throws Exception
     */
    private ParamsResponse paramsSelectEqual(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        VtValue value = this.vtPlanValueList.get(0).resolveValue(bindVariableMap);
        Resolver.ResolveDestinationResult resolveDestinationResult = Engine.resolveShards(vcursor, this.vindex,
            this.keyspace, new ArrayList<VtValue>() {{
                add(value);
            }});
        List<ResolvedShard> rss = resolveDestinationResult.getResolvedShards();
        return new ParamsResponse(rss, IntStream.range(0, rss.size()).mapToObj(i -> bindVariableMap).collect(Collectors.toList()));
    }

    /**
     * @param vcursor
     * @return
     * @throws Exception
     */
    private ParamsResponse paramsSelectIn(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        List<VtValue> keys = this.vtPlanValueList.get(0).resolveList(bindVariableMap);
        Resolver.ResolveDestinationResult resolveDestinationResult = Engine.resolveShards(vcursor, this.vindex, this.keyspace, keys);
        List<ResolvedShard> rss = resolveDestinationResult.getResolvedShards();
        List<List<Query.Value>> values = resolveDestinationResult.getValues();
        return new ParamsResponse(rss, Engine.shardVars(bindVariableMap, values));
    }

    /**
     * @param in
     * @return
     */
    private VtResultSet sort(VtResultSet in) throws SQLException {
        // Since Result is immutable, we make a copy.
        // The copy can be shallow because we won't be changing
        // the contents of any row.
        VtResultSet out = new VtResultSet();
        out.setFields(in.getFields());
        out.setRows(in.getRows());
        out.setRowsAffected(in.getRowsAffected());
        out.setInsertID(in.getInsertID());

        VtResultComparator comparator = new VtResultComparator(this.orderBy);
        out.getRows().sort(comparator);
        if (comparator.getException() != null) {
            throw comparator.getException();
        }
        return out;
    }

    @Data
    @AllArgsConstructor
    public static class ParamsResponse {
        private List<ResolvedShard> resolvedShardList;

        private List<Map<String, BindVariable>> shardVarList;
    }
}
