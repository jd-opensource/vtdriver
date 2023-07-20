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
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.engine.DMLOpcode;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.TableShardQuery;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqltypes.VtPlanValue;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.BoundQuery;
import com.jd.jdbc.srvtopo.ResolvedShard;
import com.jd.jdbc.tindexes.ActualTable;
import com.jd.jdbc.tindexes.LogicTable;
import com.jd.jdbc.tindexes.TableDestinationGroup;
import com.jd.jdbc.vindexes.VKeyspace;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import vschema.Vschema;

@Getter
@Setter
public abstract class TableDMLEngine implements PrimitiveEngine, TableShardQuery {
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

    protected TableEngine.TableDestinationResponse getResolvedTablesEqual(final Map<String, BindVariable> bindVariableMap) throws SQLException {
        VtValue value = this.vtPlanValueList.get(0).resolveValue(bindVariableMap);
        List<ActualTable> actualTables = new ArrayList<>();
        for (LogicTable ltb : this.logicTables) {
            ActualTable actualTable = ltb.map(value);
            if (actualTable == null) {
                throw new SQLException("cannot calculate split table, logic table: " + ltb.getLogicTable() + "； shardingColumnValue: " + value);
            }
            actualTables.add(actualTable);
        }
        TableDestinationGroup tableDestinationGroup = new TableDestinationGroup(actualTables);
        return new TableEngine.TableDestinationResponse(
            Lists.newArrayList(tableDestinationGroup),
            Lists.newArrayList(bindVariableMap)
        );
    }

    protected TableEngine.TableDestinationResponse resolveTablesQueryIn(final Map<String, BindVariable> bindVariableMap) throws SQLException {
        List<VtValue> keys = this.vtPlanValueList.get(0).resolveList(bindVariableMap);

        return TableEngine.resolveTablesQueryIn(this.logicTables, bindVariableMap, keys);
    }

    protected TableEngine.TableDestinationResponse resolveAllTablesQuery(final Map<String, BindVariable> bindVariableMap) throws SQLException {
        return TableEngine.paramsAllShard(this.logicTables, bindVariableMap);
    }

    public void addLogicTable(final LogicTable ltb) {
        if (this.logicTables == null) {
            this.logicTables = new ArrayList<>();
        }
        this.logicTables.add(ltb);
    }

    @Override
    public Map<ResolvedShard, List<BoundQuery>> getShardQueryList(final IContext ctx, final Vcursor vcursor, final Map<String, BindVariable> bindVariableMap) throws SQLException {
        TableEngine.TableDestinationResponse response;
        switch (opcode) {
            case Equal:
                response = getResolvedTablesEqual(bindVariableMap);
                break;
            case In:
                response = resolveTablesQueryIn(bindVariableMap);
                break;
            case Scatter:
                response = resolveAllTablesQuery(bindVariableMap);
                break;
            default:
                throw new SQLException("unsupported query route: " + opcode);
        }

        PrimitiveEngine primitiveEngine = TableEngine.buildTableQueryPlan(ctx, executeEngine, vcursor, bindVariableMap, response);
        if (!(primitiveEngine instanceof TableQueryEngine)) {
            throw new SQLException("error: primitiveEngine should be TableQueryEngine");
        }
        return ((TableQueryEngine) primitiveEngine).getResolvedShardListMap();
    }

    public abstract void setSQLStatement(SQLStatement query) throws SQLException;

    public abstract void setTableName(String tableName);
}