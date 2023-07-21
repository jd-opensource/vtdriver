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

import com.google.common.collect.Sets;
import com.jd.jdbc.IExecute;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.planbuilder.MultiQueryPlan;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.tindexes.ActualTable;
import com.jd.jdbc.tindexes.LogicTable;
import com.jd.jdbc.tindexes.TableDestinationGroup;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import lombok.AllArgsConstructor;
import lombok.Getter;

public class TableEngine {

    public static TableDestinationResponse paramsAllShard(final List<LogicTable> logicTables, final Map<String, BindVariable> bindVariableMap) throws SQLException {
        List<TableDestinationGroup> allActualTableGroup = getAllActualTableGroup(logicTables);
        List<Map<String, BindVariable>> bindVariableList = new ArrayList<>();
        for (int i = 0; i < allActualTableGroup.size(); i++) {
            bindVariableList.add(bindVariableMap);
        }
        return new TableDestinationResponse(allActualTableGroup, bindVariableList);
    }

    public static List<TableDestinationGroup> getAllActualTableGroup(final List<LogicTable> logicTables) {
        List<TableDestinationGroup> allActualTableGroup = new ArrayList<>();
        if (logicTables == null || logicTables.isEmpty()) {
            return allActualTableGroup;
        }

        List<Set<ActualTable>> routingTableGroups = new ArrayList<>();

        for (LogicTable lgt : logicTables) {
            Set<ActualTable> actualTableSet = new TreeSet<>(lgt.getActualTableList());
            routingTableGroups.add(actualTableSet);
        }

        Set<List<ActualTable>> actualTableGroups = Sets.cartesianProduct(routingTableGroups);

        for (List<ActualTable> actualTableGroup : actualTableGroups) {
            allActualTableGroup.add(new TableDestinationGroup(actualTableGroup));
        }
        return allActualTableGroup;
    }

    public static PrimitiveEngine buildTableQueryPlan(IContext ctx, PrimitiveEngine executeEngine, Vcursor vcursor, Map<String, BindVariable> bindVariableMap,
                                                      TableDestinationResponse resolvedShardsEqual) throws SQLException {
        List<Map<String, BindVariable>> values = resolvedShardsEqual.getTableVarList();
        List<PrimitiveEngine> sourceList = new ArrayList<>();
        List<IExecute.ResolvedShardQuery> shardQueryList = new ArrayList<>();
        Map<String, String> actLtbMap = new HashMap<>();
        for (TableDestinationGroup destionation : resolvedShardsEqual.getTables()) {
            Map<String, String> switchTable = destionation.getSwitchTableMap();
            actLtbMap.putAll(destionation.getActLtbMap());
            sourceList.add(executeEngine);
            shardQueryList.add(executeEngine.resolveShardQuery(ctx, vcursor, bindVariableMap, switchTable));
        }
        PrimitiveEngine primitive = MultiQueryPlan.buildTableQueryPlan(sourceList, shardQueryList, values, actLtbMap);
        return primitive;
    }

    /**
     * this value can only accept multiple logicTable sharing one planValue
     *
     * @param logicTables
     * @param bindVariableMap
     * @param keys
     * @return
     * @throws SQLException
     */
    public static TableDestinationResponse resolveTablesQueryIn(final List<LogicTable> logicTables, final Map<String, BindVariable> bindVariableMap, final List<VtValue> keys) throws SQLException {

        Map<TableDestinationGroup, List<Query.Value>> tablesMap = new TreeMap<>();
        for (VtValue key : keys) {
            TableDestinationGroup tableGroup = new TableDestinationGroup();
            for (LogicTable ltb : logicTables) {
                ActualTable actualTable = ltb.map(key);
                if (actualTable == null) {
                    throw new SQLException("cannot calculate split table, logic table: " + ltb.getLogicTable() + "； shardingColumnValue: " + key);
                }
                tableGroup.addActualTable(actualTable);
            }

            if (tablesMap.containsKey(tableGroup)) {
                tablesMap.get(tableGroup).add(key.toQueryValue());
            } else {
                tablesMap.put(tableGroup, new ArrayList<>());
                tablesMap.get(tableGroup).add(key.toQueryValue());
            }
        }

        List<TableDestinationGroup> tables = new ArrayList<>(tablesMap.size());
        List<List<Query.Value>> planValuePerTableGroup = new ArrayList<>(tablesMap.size());

        for (Map.Entry<TableDestinationGroup, List<Query.Value>> tablesEntry : tablesMap.entrySet()) {
            tables.add(tablesEntry.getKey());
            planValuePerTableGroup.add(tablesEntry.getValue());
        }

        return new TableDestinationResponse(tables, Engine.shardVars(bindVariableMap, planValuePerTableGroup));
    }

    public static VtResultSet getTableBatchExecuteResult(IContext ctx, PrimitiveEngine executeEngine, Vcursor vcursor, Map<String, BindVariable> bindVariableMap,
                                                         TableDestinationResponse resolvedShardsEqual) throws SQLException {
        PrimitiveEngine primitiveEngine = buildTableQueryPlan(ctx, executeEngine, vcursor, bindVariableMap, resolvedShardsEqual);
        return execCollectMultQueries(ctx, primitiveEngine, vcursor, false);
    }

    public static VtResultSet execCollectMultQueries(IContext ctx, PrimitiveEngine primitive, Vcursor vcursor, boolean wantField) throws SQLException {
        if (!(primitive instanceof TableQueryEngine)) {
            throw new SQLException("wrong engine type: execute table query must be TableQueryEngine");
        }

        IExecute.ExecuteMultiShardResponse executeMultiShardResponses = primitive.execute(ctx, vcursor, null, wantField);
        return (VtResultSet) executeMultiShardResponses.getVtRowList();
    }

    @AllArgsConstructor
    @Getter
    public static class TableDestinationResponse {
        private final List<TableDestinationGroup> tables;

        private final List<Map<String, BindVariable>> tableVarList;
    }
}
