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

import com.google.common.collect.Lists;
import com.jd.jdbc.IExecute;
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRestoreVisitor;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.BoundQuery;
import com.jd.jdbc.srvtopo.ResolvedShard;
import com.jd.jdbc.srvtopo.Resolver;
import com.jd.jdbc.topo.topoproto.TopoProto;
import com.jd.jdbc.vindexes.SingleColumn;
import com.jd.jdbc.vindexes.VKeyspace;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;

import static com.jd.jdbc.engine.Engine.AggregateOpcode.AggregateAvg;
import static com.jd.jdbc.engine.Engine.AggregateOpcode.AggregateAvgCount;
import static com.jd.jdbc.engine.Engine.AggregateOpcode.AggregateAvgSum;
import static com.jd.jdbc.engine.Engine.AggregateOpcode.AggregateCount;
import static com.jd.jdbc.engine.Engine.AggregateOpcode.AggregateCountDistinct;
import static com.jd.jdbc.engine.Engine.AggregateOpcode.AggregateMax;
import static com.jd.jdbc.engine.Engine.AggregateOpcode.AggregateMin;
import static com.jd.jdbc.engine.Engine.AggregateOpcode.AggregateSum;
import static com.jd.jdbc.engine.Engine.AggregateOpcode.AggregateSumDistinct;
import static com.jd.jdbc.engine.Engine.PulloutOpcode.PulloutExists;
import static com.jd.jdbc.engine.Engine.PulloutOpcode.PulloutIn;
import static com.jd.jdbc.engine.Engine.PulloutOpcode.PulloutNotIn;
import static com.jd.jdbc.engine.Engine.PulloutOpcode.PulloutValue;

public class Engine {

    public static final String LIST_VAR_NAME = "__vals";

    /**
     * SupportedAggregates maps the list of supported aggregate
     * functions to their opcodes.
     */
    public static Map<String, AggregateOpcode> supportedAggregates = new HashMap<String, AggregateOpcode>(6) {{
        put("count", AggregateCount);
        put("sum", AggregateSum);
        put("min", AggregateMin);
        put("max", AggregateMax);
        put("avg", AggregateAvg);
        // These functions don't exist in mysql, but are used
        // to display the plan.
        put("count_distinct", AggregateCountDistinct);
        put("sum_distinct", AggregateSumDistinct);
        put("avg_sum", AggregateAvgSum);
        put("avg_count", AggregateAvgCount);
    }};

    public static Map<PulloutOpcode, String> pulloutName = new HashMap<PulloutOpcode, String>(4) {{
        put(PulloutValue, "PulloutValue");
        put(PulloutIn, "PulloutIn");
        put(PulloutNotIn, "PulloutNotIn");
        put(PulloutExists, "PulloutExists");
    }};

    /**
     * InsertVarName returns a name for the bind var for this column. This method is used by the planner and engine,
     * to make sure they both produce the same names
     *
     * @param col
     * @param rowNum
     * @return
     */
    public static String insertVarName(String col, Integer rowNum) {
        String varName = "_" + compliantName(col) + "_" + rowNum;
        return varName;
    }

    /**
     * CompliantName returns a compliant id name
     * that can be used for a bind var.
     *
     * @param in
     * @return
     */
    public static String compliantName(String in) {
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < in.length(); i++) {
            if (!Character.isLetter(in.charAt(i))) {
                if (i == 0 || !Character.isDigit(in.charAt(i))) {
                    buf.append('_');
                    continue;
                }
            }
            buf.append(in.charAt(i));
        }
        return buf.toString();
    }

    /**
     * @param vcursor
     * @param vindex
     * @param keyspace
     * @param vindexKeys
     * @return
     * @throws Exception
     */
    public static Resolver.ResolveDestinationResult resolveShards(Vcursor vcursor, SingleColumn vindex, VKeyspace keyspace, List<VtValue> vindexKeys) throws SQLException {
        // Convert vindexKeys to []*querypb.Value
        List<Query.Value> ids = vindexKeys.stream().map(VtValue::toQueryValue).collect(Collectors.toList());

        // Map using the Vindex
        Destination[] destinations = vindex.map(vindexKeys.toArray(new VtValue[0]));

        // And use the Resolver to map to ResolvedShards.
        return vcursor.resolveDestinations(keyspace.getName(), ids, Arrays.asList(destinations));
    }

    /**
     * @param vcursor
     * @param query
     * @param bindVariableMap
     * @param rs
     * @param rollbackOnError
     * @param canAutocommit
     * @return
     */
    public static IExecute.ExecuteMultiShardResponse execShard(Vcursor vcursor, SQLObject query, Map<String, BindVariable> bindVariableMap, ResolvedShard rs, boolean rollbackOnError,
                                                               boolean canAutocommit) throws SQLException {
        boolean autocommit = canAutocommit && vcursor.autocommitApproval();
        String charEncoding = vcursor.getCharEncoding();
        List<BoundQuery> queries = getQueries(query, new ArrayList<Map<String, BindVariable>>() {{
            add(bindVariableMap);
        }}, charEncoding);
        return vcursor.executeMultiShard(
            Lists.newArrayList(rs),
            queries,
            rollbackOnError,
            autocommit);
    }

    /**
     * @param query
     * @param bindVariableMapList
     * @param charEncoding
     * @return
     */
    public static List<BoundQuery> getQueries(SQLObject query, List<Map<String, BindVariable>> bindVariableMapList, String charEncoding) throws SQLException {
        List<BoundQuery> queries = new ArrayList<>(bindVariableMapList.size());
        for (Map<String, BindVariable> bindVariableMap : bindVariableMapList) {
            StringBuilder realQueryOutput = new StringBuilder();
            VtRestoreVisitor vtRestoreVisitor = new VtRestoreVisitor(realQueryOutput, bindVariableMap, charEncoding);
            query.accept(vtRestoreVisitor);
            if (vtRestoreVisitor.getException() != null) {
                throw vtRestoreVisitor.getException();
            }
            queries.add(new BoundQuery(realQueryOutput.toString()));
        }
        return queries;
    }

    /**
     * @param query
     * @param bindVariableMapList
     * @param switchTables
     * @param charEncoding
     * @return
     */
    public static List<BoundQuery> getQueries(SQLObject query, List<Map<String, BindVariable>> bindVariableMapList, Map<String, String> switchTables, String charEncoding) throws SQLException {
        List<BoundQuery> queries = new ArrayList<>(bindVariableMapList.size());
        for (Map<String, BindVariable> bindVariableMap : bindVariableMapList) {
            StringBuilder realQueryOutput = new StringBuilder();
            VtRestoreVisitor vtRestoreVisitor = new VtRestoreVisitor(realQueryOutput, bindVariableMap, switchTables, charEncoding);
            query.accept(vtRestoreVisitor);
            if (vtRestoreVisitor.getException() != null) {
                throw vtRestoreVisitor.getException();
            }
            queries.add(new BoundQuery(realQueryOutput.toString()));
        }
        return queries;
    }

    /**
     * @param bindVariableMap
     * @param mapVals
     * @return
     */
    public static List<Map<String, BindVariable>> shardVars(Map<String, BindVariable> bindVariableMap, List<List<Query.Value>> mapVals) {
        List<Map<String, BindVariable>> shardVarList = new ArrayList<>();
        for (List<Query.Value> mapVal : mapVals) {
            Map<String, BindVariable> newbv = new LinkedHashMap<>(16, 1);
            newbv.putAll(bindVariableMap);
            newbv.put(LIST_VAR_NAME, new BindVariable(mapVal, Query.Type.TUPLE));
            shardVarList.add(newbv);
        }
        return shardVarList;
    }

    /**
     * @param rsList
     * @throws SQLException
     */
    public static void allowOnlyMaster(List<ResolvedShard> rsList) throws SQLException {
        for (ResolvedShard rs : rsList) {
            if (rs != null && !Topodata.TabletType.MASTER.equals(rs.getTarget().getTabletType())) {
                throw new SQLException("supported only for master tablet type, current type: " + TopoProto.tabletTypeLstring(rs.getTarget().getTabletType()));
            }
        }
    }

    /**
     * @param rs
     * @throws SQLException
     */
    public static void allowOnlyMaster(ResolvedShard rs) throws SQLException {
        if (rs != null && !Topodata.TabletType.MASTER.equals(rs.getTarget().getTabletType())) {
            throw new SQLException("supported only for master tablet type, current type: " + TopoProto.tabletTypeLstring(rs.getTarget().getTabletType()));
        }
    }

    /**
     * This is the list of RouteOpcode values.
     */
    public enum RouteOpcode {
        /**
         * SelectUnsharded is the opcode for routing a
         * select statement to an unsharded database.
         */
        SelectUnsharded(0),
        /**
         * SelectEqualUnique is for routing a query to
         * a single shard. Requires: A Unique Vindex, and
         * a single Value.
         */
        SelectEqualUnique(1),
        /**
         * SelectEqual is for routing a query using a
         * non-unique vindex. Requires: A Vindex, and
         * a single Value.
         */
        SelectEqual(2),
        /**
         * SelectIN is for routing a query that has an IN
         * clause using a Vindex. Requires: A Vindex,
         * and a Values list.
         */
        SelectIN(3),
        /**
         * SelectScatter is for routing a scatter query
         * to all shards of a keyspace.
         */
        SelectScatter(4),
        /**
         * SelectNext is for fetching from a sequence.
         */
        SelectNext(5),
        /**
         * SelectDBA is for executing a DBA statement.
         */
        SelectDBA(6),
        /**
         * SelectReference is for fetching from a reference table.
         */
        SelectReference(7),
        /**
         * SelectNone is used for queries that always return empty values
         */
        SelectNone(8),
        /**
         * NumRouteOpcodes is the number of opcodes
         */
        NumRouteOpcodes(9);

        @Getter
        private final Integer value;

        RouteOpcode(Integer value) {
            this.value = value;
        }
    }

    /**
     * This is the list of TableRouteOpcode values.
     */
    public enum TableRouteOpcode {
        /**
         * SelectEqualUnique is for routing a query to
         * a single table. Requires: A Unique Tindex, and
         * a single Value.
         */
        SelectEqualUnique(0),
        /**
         * SelectEqual is for routing a query using a
         * non-unique Tindex. Requires: A Tindex, and
         * a single Value.
         */
        SelectEqual(1),
        /**
         * SelectIN is for routing a query that has an IN
         * clause using a Tindex. Requires: A Tindex,
         * and a Values list.
         */
        SelectIN(2),
        /**
         * SelectScatter is for routing a scatter query
         * to all tables of a keyspace.
         */
        SelectScatter(3),
        /**
         * SelectNext is for fetching from a sequence.
         */
        SelectNext(4),
        /**
         * SelectNone is used for queries that always return empty values
         */
        SelectNone(5),
        /**
         * NumRouteOpcodes is the number of opcodes
         */
        NumRouteOpcodes(6);

        @Getter
        private final Integer value;

        TableRouteOpcode(Integer value) {
            this.value = value;
        }
    }

    /**
     * These constants list the possible aggregate opcodes.
     */
    public enum AggregateOpcode {
        /***/
        AggregateCount(0),
        AggregateSum(1),
        AggregateMin(2),
        AggregateMax(3),
        AggregateCountDistinct(4),
        AggregateSumDistinct(5),
        AggregateAvg(6),
        AggregateAvgSum(7),
        AggregateAvgCount(8);

        @Getter
        private final Integer value;

        AggregateOpcode(Integer value) {
            this.value = value;
        }
    }

    /**
     * This is the list of InsertOpcode values.
     */
    public enum InsertOpcode {
        /**
         * InsertUnsharded is for routing an insert statement
         * to an unsharded keyspace.
         */
        InsertUnsharded(0),
        /**
         * InsertSharded is for routing an insert statement
         * to individual shards. Requires: A list of Values, one
         * for each ColVindex. If the table has an Autoinc column,
         * A Generate subplan must be created.
         */
        InsertSharded(1),
        /**
         * InsertShardedIgnore is for INSERT IGNORE and
         * INSERT...ON DUPLICATE KEY constructs.
         */
        InsertShardedIgnore(2),
        /**
         * InsertByDestination is to route explicitly to a given
         * target destination.
         */
        InsertByDestination(3);

        @Getter
        private final Integer value;

        InsertOpcode(Integer value) {
            this.value = value;
        }
    }

    /**
     * This is the list of PulloutOpcode values.
     */
    public enum PulloutOpcode {
        /***/
        PulloutValue(0),
        PulloutIn(1),
        PulloutNotIn(2),
        PulloutExists(3);

        @Getter
        private final Integer value;

        PulloutOpcode(Integer value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return pulloutName.get(this);
        }
    }

    /**
     * JoinOpcode is a number representing the opcode for the Join primitive.
     */
    public enum JoinOpcode {
        /***/
        NormalJoin(0),
        LeftJoin(1);

        @Getter
        private final Integer value;

        JoinOpcode(Integer value) {
            this.value = value;
        }

        @Override
        public String toString() {
            if (this == NormalJoin) {
                return "Join";
            }
            return "LeftJoin";
        }
    }

}
