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

import com.jd.jdbc.common.tuple.ImmutablePair;
import com.jd.jdbc.common.tuple.Pair;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.evalengine.EvalResult;
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.key.DestinationAllShard;
import com.jd.jdbc.key.DestinationAnyShard;
import com.jd.jdbc.planbuilder.RoutePlan;
import com.jd.jdbc.sqltypes.SqlTypes;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.ResolvedShard;
import com.jd.jdbc.srvtopo.Resolver;
import com.jd.jdbc.vindexes.MultiColumn;
import com.jd.jdbc.vindexes.SingleColumn;
import com.jd.jdbc.vindexes.VKeyspace;
import io.netty.util.internal.StringUtil;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Data;

@Data
public class RoutingParameters {
    /**
     * RouteOpcode is a number representing the opcode
     * for the Route primitve
     */
    protected Engine.RouteOpcode routeOpcode;

    protected VKeyspace keyspace;

    private List<EvalEngine.Expr> systableTableSchema = new ArrayList<>();

    private Map<String, EvalEngine.Expr> systableTableName = new HashMap<>();

    /**
     * TargetDestination specifies an explicit target destination to send the query to.
     * This bypases the core of the v3 engine.
     */
    private Destination targetDestination;

    /**
     * Vindex specifies the vindex to be used.
     */
    private SingleColumn vindex;

    private List<EvalEngine.Expr> values = new ArrayList<>();

    protected boolean isQueryPinnedTable;

    protected Pair<List<ResolvedShard>, List<Map<String, BindVariable>>> findRoute(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        RouteGen4Engine.ParamsResponse paramsResponse = null;
        switch (routeOpcode) {
            case SelectDBA:
                paramsResponse = this.systemQuery(vcursor, bindVariableMap);
                break;
            case SelectUnsharded:
                paramsResponse = this.paramsAllShard(vcursor, bindVariableMap);
                break;
            case SelectReference:
                return this.paramsAnyShard(vcursor, bindVariableMap);
            case SelectScatter:
            case SelectNext:
                paramsResponse = this.paramsAllShard(vcursor, bindVariableMap);
                break;
            case SelectEqual:
            case SelectEqualUnique:
                if (this.vindex instanceof MultiColumn) {
                } else {
                    paramsResponse = this.paramsSelectEqual(vcursor, bindVariableMap);
                }
                break;
            case SelectIN:
                if (this.vindex instanceof MultiColumn) {
                } else {
                    paramsResponse = this.paramsSelectIn(vcursor, bindVariableMap);
                }
                break;
            case SelectNone:
                paramsResponse = new RouteGen4Engine.ParamsResponse(new ArrayList<>(), new ArrayList<>());
                break;
            default:
                // Unreachable.
                throw new SQLException("unsupported query route: " + routeOpcode);
        }
        return new ImmutablePair<>(paramsResponse.getResolvedShardList(), paramsResponse.getShardVarList());
    }

    public RouteGen4Engine.ParamsResponse systemQuery(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        if (this.systableTableName.size() == 0 && this.systableTableSchema.size() == 0) {
            return new RouteGen4Engine.ParamsResponse(defaultRoute(vcursor), new ArrayList<Map<String, BindVariable>>() {{
                add(bindVariableMap);
            }});
        }

        String specifiedKS = "";
        boolean schemaExists = false;
        EvalEngine.ExpressionEnv env = new EvalEngine.ExpressionEnv(bindVariableMap);
        for (EvalEngine.Expr expr : this.systableTableSchema) {
            EvalResult result = env.evaluate(expr);
            String ks = result.value().toString();
            if (StringUtil.isNullOrEmpty(specifiedKS)) {
                specifiedKS = ks;
            } else if (!ks.equalsIgnoreCase(specifiedKS)) {
                throw new SQLException("can't use more than one keyspace per system table query - found both '" + specifiedKS + "' and '" + ks + "'");
            }
        }

        if (!StringUtil.isNullOrEmpty(specifiedKS)) {
            bindVariableMap.put(SqlTypes.BV_SCHEMA_NAME, SqlTypes.stringBindVariable(specifiedKS));
        }
        Map<String, String> tableNames = new HashMap();
        for (Map.Entry<String, EvalEngine.Expr> entry : this.systableTableName.entrySet()) {
            String tblBvName = entry.getKey();
            EvalResult val = env.evaluate(entry.getValue());
            String tabName = val.value().toString();
            tableNames.put(tblBvName, tabName);
            bindVariableMap.put(tblBvName, SqlTypes.stringBindVariable(tabName));
        }

        // if the table_schema is system schema, route to default keyspace.
        if (RoutePlan.systemTable(specifiedKS)) {
            return new RouteGen4Engine.ParamsResponse(defaultRoute(vcursor), new ArrayList<Map<String, BindVariable>>() {{
                add(bindVariableMap);
            }});
        }
        // the use has specified a table_name - let's check if it's a routed table
        if (tableNames.size() > 0) {

        }
        List<ResolvedShard> destinations;
        try {
            Resolver.ResolveDestinationResult resolveDestinationResult = vcursor.resolveDestinations(
                specifiedKS, null, new ArrayList<Destination>() {{
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
//        return destinations;
        return new RouteGen4Engine.ParamsResponse(destinations, new ArrayList<Map<String, BindVariable>>() {{
            add(bindVariableMap);
        }});
    }

    public List<ResolvedShard> defaultRoute(Vcursor vcursor) throws SQLException {
        Resolver.ResolveDestinationResult resolveDestinationResult = null;
        try {
            resolveDestinationResult = vcursor.resolveDestinations(this.keyspace.getName(), null, new ArrayList<Destination>() {{
                add(new DestinationAnyShard());
            }});
        } catch (SQLException e) {
            throw new SQLException("failed to find information about keyspace `" + keyspace.getName() + "`");
        }
        return resolveDestinationResult.getResolvedShards();

    }

    /**
     * @param vcursor
     * @return
     * @throws Exception
     */
    protected Pair<List<ResolvedShard>, List<Map<String, BindVariable>>> paramsAnyShard(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        Resolver.ResolveDestinationResult resolveDestinationResult = vcursor.resolveDestinations(this.keyspace.getName(), null, Collections.singletonList(new DestinationAnyShard()));
        List<ResolvedShard> rss = resolveDestinationResult.getResolvedShards();
        return new ImmutablePair<>(rss, IntStream.range(0, rss.size()).mapToObj(i -> bindVariableMap).collect(Collectors.toList()));
    }

    /**
     * @param vcursor
     * @return
     * @throws Exception
     */
    private RouteGen4Engine.ParamsResponse paramsAllShard(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        Resolver.ResolveDestinationResult resolveDestinationResult = vcursor.resolveDestinations(this.keyspace.getName(), null, Collections.singletonList(new DestinationAllShard()));
        List<ResolvedShard> rss = resolveDestinationResult.getResolvedShards();
        return new RouteGen4Engine.ParamsResponse(rss, IntStream.range(0, rss.size()).mapToObj(i -> bindVariableMap).collect(Collectors.toList()));
    }

    /**
     * @param vcursor
     * @return
     * @throws Exception
     */
    private RouteGen4Engine.ParamsResponse paramsSelectEqual(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        EvalEngine.ExpressionEnv env = new EvalEngine.ExpressionEnv(bindVariableMap);
        EvalResult value = env.evaluate(this.values.get(0));
        Resolver.ResolveDestinationResult resolveDestinationResult = Engine.resolveShards(vcursor, this.vindex,
            this.keyspace, new ArrayList<VtValue>() {{
                add(value.value());
            }});
        List<ResolvedShard> rss = resolveDestinationResult.getResolvedShards();
        return new RouteGen4Engine.ParamsResponse(rss, IntStream.range(0, rss.size()).mapToObj(i -> bindVariableMap).collect(Collectors.toList()));
    }

    /**
     * @param vcursor
     * @return
     * @throws Exception
     */
    private RouteGen4Engine.ParamsResponse paramsSelectIn(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        EvalEngine.ExpressionEnv env = new EvalEngine.ExpressionEnv(bindVariableMap);
        EvalResult value = env.evaluate(this.values.get(0));
        Resolver.ResolveDestinationResult resolveDestinationResult = Engine.resolveShards(vcursor, this.vindex, this.keyspace, value.tupleValues());
        List<ResolvedShard> rss = resolveDestinationResult.getResolvedShards();
        List<List<Query.Value>> values = resolveDestinationResult.getValues();
        return new RouteGen4Engine.ParamsResponse(rss, Engine.shardVars(bindVariableMap, values));
    }

}
