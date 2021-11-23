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
import com.jd.jdbc.engine.DMLOpcode;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqltypes.VtPlanValue;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.tindexes.ActualTable;
import com.jd.jdbc.tindexes.LogicTable;
import com.jd.jdbc.vindexes.VKeyspace;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import vschema.Vschema;

@Getter
@Setter
public abstract class TableDMLEngine implements PrimitiveEngine {
    // Opcode is the execution opcode.
    protected DMLOpcode opcode;

    // Keyspace specifies the keyspace to send the query to.
    protected VKeyspace keyspace;

    // Query specifies the query to be executed.
    protected SQLStatement query;

    // Values specifies the vindex values to use for routing.
    // For now, only one value is specified.
    protected List<VtPlanValue> vtPlanValueList = new ArrayList<>();

    // Table specifies the table for the update.
    protected Vschema.Table table;

    // Option to override the standard behavior and allow a multi-shard update
    // to use single round trip autocommit.
    protected boolean multiShardAutocommit;

    // QueryTimeout contains the optional timeout (in milliseconds) to apply to this query
    protected int queryTimeout;

    protected PrimitiveEngine executeEngine;

    // LogicTable
    private List<LogicTable> logicTables = new ArrayList<>();

    protected Engine.TableDestinationResponse getResolvedShardsEqual(final Map<String, Query.BindVariable> bindValue) throws SQLException {
        VtValue value = this.vtPlanValueList.get(0).resolveValue(bindValue);
        List<ActualTable> actualTables = new ArrayList<>();
        for (LogicTable ltb : this.logicTables) {
            ActualTable actualTable = ltb.map(value);
            if (actualTable == null) {
                throw new SQLException("cannot calculate split table, logic table: " + ltb.getLogicTable());
            }
            actualTables.add(actualTable);
        }
        return new Engine.TableDestinationResponse(
            Collections.singletonList(actualTables),
            Collections.singletonList(bindValue)
        );
    }

    protected Engine.TableDestinationResponse resolveShardQueryIn(final Map<String, Query.BindVariable> bindValue) throws SQLException {
        List<VtValue> keys = this.vtPlanValueList.get(0).resolveList(bindValue);
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
        return new Engine.TableDestinationResponse(tables, Engine.shardVars(bindValue, planValuePerTableGroup));

    }

    protected Engine.TableDestinationResponse resolveAllShardQuery(final Map<String, Query.BindVariable> bindValue) throws SQLException {
        List<List<ActualTable>> allActualTableGroup = this.getAllActualTableGroup(this.logicTables);
        List<Map<String, Query.BindVariable>> bindVariableList = new ArrayList<>();
        for (int i = 0; i < allActualTableGroup.size(); i++) {
            bindVariableList.add(bindValue);
        }
        return new Engine.TableDestinationResponse(allActualTableGroup, bindVariableList);
    }

    private List<List<ActualTable>> getAllActualTableGroup(final List<LogicTable> logicTables) {
        // 将二维数组生成一个笛卡尔积
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
            for (List<ActualTable> tables : allActualTableGroup) {
                for (int k = 0; k < actualTables.size(); k++) {
                    List<ActualTable> actualTableGroup = new ArrayList<>(tables);
                    actualTableGroup.add(actualTables.get(k));
                    tempActualTableGroup.add(actualTableGroup);
                }
            }
            allActualTableGroup = tempActualTableGroup;
        }
        return allActualTableGroup;
    }

    public void addLogicTable(final LogicTable ltb) {
        if (this.logicTables == null) {
            this.logicTables = new ArrayList<>();
        }
        this.logicTables.add(ltb);
    }

    public abstract void setSQLStatement(SQLStatement query) throws SQLException;

    public abstract void setTableName(String tableName);
}