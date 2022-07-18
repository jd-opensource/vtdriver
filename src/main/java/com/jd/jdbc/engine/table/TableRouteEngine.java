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

import com.google.common.collect.Lists;
import com.jd.jdbc.IExecute;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.engine.AbstractRouteEngine;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.tindexes.ActualTable;
import com.jd.jdbc.tindexes.LogicTable;
import com.jd.jdbc.tindexes.TableIndex;
import com.jd.jdbc.vindexes.VKeyspace;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
public class TableRouteEngine extends AbstractRouteEngine {
    /**
     * RouteOpcode is a number representing the opcode
     * for the Route primitve
     */
    private Engine.TableRouteOpcode routeOpcode;

    /**
     * Values specifies the vindex values to use for routing table.
     */
    private TableIndex tableIndex;

    /**
     * ueryTimeout contains the optional timeout (in milliseconds) to apply to this query
     */
    // private Integer queryTimeout = 0;

    private List<LogicTable> logicTables = new ArrayList<>();

    private PrimitiveEngine executeEngine;

    public TableRouteEngine(final Engine.TableRouteOpcode routeOpcode, final VKeyspace keyspace) {
        this.routeOpcode = routeOpcode;
        this.keyspace = keyspace;
    }

    @Override
    public IExecute.ExecuteMultiShardResponse execute(final IContext ctx, final Vcursor vcursor, final Map<String, BindVariable> bindVariableMap, final boolean wantFields) throws SQLException {
        Engine.TableDestinationResponse tableDestinationResponse = this.getResolveDestinationResult(bindVariableMap);
        VtResultSet resultSet = new VtResultSet();
        // No route
        if (tableDestinationResponse == null) {
            if (wantFields) {
                resultSet = this.getFields(vcursor, new HashMap<>(16, 1));
                return new IExecute.ExecuteMultiShardResponse(resultSet);
            }
            return new IExecute.ExecuteMultiShardResponse(resultSet);
        }
        if (executeEngine.canResolveShardQuery()) {

            VtResultSet tableBatchExecuteResult = Engine.getTableBatchExecuteResult(ctx, executeEngine, vcursor, bindVariableMap, tableDestinationResponse);
            if (this.orderBy == null || this.orderBy.isEmpty()) {
                return new IExecute.ExecuteMultiShardResponse(tableBatchExecuteResult);
            }
            resultSet = this.sort(tableBatchExecuteResult);
            return new IExecute.ExecuteMultiShardResponse(resultSet);
        } else {
            throw new SQLException("unsupported engine for partitioned table");
        }
    }

    @Override
    public Boolean needsTransaction() {
        return false;
    }

    private Engine.TableDestinationResponse getResolveDestinationResult(final Map<String, BindVariable> bindVariableMap) throws SQLException {
        Engine.TableDestinationResponse tableDestinationResponse;
        switch (this.routeOpcode) {
            case SelectScatter:
                tableDestinationResponse = this.paramsAllShard(bindVariableMap);
                break;
            case SelectEqual:
            case SelectEqualUnique:
                tableDestinationResponse = this.paramsSelectEqual(bindVariableMap);
                break;
            case SelectIN:
                tableDestinationResponse = this.paramsSelectIn(bindVariableMap);
                break;
            default:
                // Unreachable.
                throw new SQLException("unsupported query route: " + routeOpcode);
        }
        return tableDestinationResponse;
    }

    private Engine.TableDestinationResponse paramsAllShard(final Map<String, BindVariable> bindVariableMap) throws SQLException {
        List<List<ActualTable>> allActualTableGroup = this.getAllActualTableGroup(this.logicTables);
        List<Map<String, BindVariable>> bindVariableList = new ArrayList<>();
        for (int i = 0; i < allActualTableGroup.size(); i++) {
            bindVariableList.add(bindVariableMap);
        }
        return new Engine.TableDestinationResponse(allActualTableGroup, bindVariableList);
    }

    private List<List<ActualTable>> getAllActualTableGroup(final List<LogicTable> logicTables) {
        List<List<ActualTable>> allActualTableGroup = new ArrayList<>();
        if (logicTables == null || logicTables.isEmpty()) {
            return allActualTableGroup;
        }
        for (ActualTable act : logicTables.get(0).getActualTableList()) {
            List<ActualTable> tableGroup = Lists.newArrayList(act);
            allActualTableGroup.add(tableGroup);
        }
        for (int i = 1; i < logicTables.size(); i++) {
            List<ActualTable> actualTables = logicTables.get(i).getActualTableList();
            List<List<ActualTable>> tempActualTableGroup = new ArrayList<>();
            for (int j = 0; j < allActualTableGroup.size(); j++) {
                for (int k = 0; k < actualTables.size(); k++) {
                    List<ActualTable> actualTableGroup = new ArrayList<>();
                    for (ActualTable act : allActualTableGroup.get(j)) {
                        actualTableGroup.add(act);
                    }
                    actualTableGroup.add(actualTables.get(k));
                    tempActualTableGroup.add(actualTableGroup);
                }
            }
            allActualTableGroup = tempActualTableGroup;
        }
        return allActualTableGroup;
    }

    private Engine.TableDestinationResponse paramsSelectEqual(final Map<String, BindVariable> bindVariableMap) throws SQLException {
        VtValue value = this.vtPlanValueList.get(0).resolveValue(bindVariableMap);
        List<ActualTable> actualTables = new ArrayList<>();
        for (LogicTable ltb : this.logicTables) {
            ActualTable actualTable = ltb.map(value);
            if (actualTable == null) {
                throw new SQLException("cannot calculate split table, logic table: " + ltb.getLogicTable());
            }
            actualTables.add(actualTable);
        }
        return new Engine.TableDestinationResponse(
            new ArrayList<List<ActualTable>>() {{
                add(actualTables);
            }},
            new ArrayList<Map<String, BindVariable>>() {{
                add(bindVariableMap);
            }});
    }

    private Engine.TableDestinationResponse paramsSelectIn(final Map<String, BindVariable> bindVariableMap) throws SQLException {
        List<VtValue> keys = this.vtPlanValueList.get(0).resolveList(bindVariableMap);
        List<List<ActualTable>> tables = new ArrayList<>();
        List<List<Query.Value>> planValuePerTableGroup = new ArrayList<>();
        Map<String, Integer> actualTableMap = new HashMap<>();
        for (VtValue key : keys) {
            List<ActualTable> actualTables = new ArrayList<>();
            StringBuilder tableGroup = new StringBuilder();
            for (LogicTable ltb : this.logicTables) {
                ActualTable actualTable = ltb.map(key);
                if (actualTable == null) {
                    throw new SQLException("cannot calculate split table, logic table: " + ltb.getLogicTable());
                }
                actualTables.add(actualTable);
                tableGroup.append(actualTable.getActualTableName());
            }
            if (actualTableMap.containsKey(tableGroup.toString())) {
                planValuePerTableGroup.get(actualTableMap.get(tableGroup.toString())).add(key.toQueryValue());
            } else {
                planValuePerTableGroup.add(new ArrayList<>());
                actualTableMap.put(tableGroup.toString(), planValuePerTableGroup.size() - 1);
                planValuePerTableGroup.get(actualTableMap.get(tableGroup.toString())).add(key.toQueryValue());
                tables.add(actualTables);
            }
        }
        return new Engine.TableDestinationResponse(tables, Engine.shardVars(bindVariableMap, planValuePerTableGroup));
    }

    /**
     * @param in
     * @return
     */
    private VtResultSet sort(final VtResultSet in) throws SQLException {
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
}
