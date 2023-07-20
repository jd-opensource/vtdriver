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
import com.jd.jdbc.engine.TableShardQuery;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.BoundQuery;
import com.jd.jdbc.srvtopo.ResolvedShard;
import com.jd.jdbc.tindexes.ActualTable;
import com.jd.jdbc.tindexes.LogicTable;
import com.jd.jdbc.tindexes.TableDestinationGroup;
import com.jd.jdbc.tindexes.TableIndex;
import com.jd.jdbc.vindexes.VKeyspace;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
public class TableRouteEngine extends AbstractRouteEngine implements TableShardQuery {
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
    public Map<ResolvedShard, List<BoundQuery>> getShardQueryList(final IContext ctx, final Vcursor vcursor, final Map<String, BindVariable> bindVariableMap) throws SQLException {
        TableEngine.TableDestinationResponse tableDestinationResponse = this.getResolveDestinationResult(bindVariableMap);

        if (tableDestinationResponse == null) {
            return new HashMap<>();
        }
        if (!executeEngine.canResolveShardQuery()) {
            return new HashMap<>();
        }
        PrimitiveEngine primitiveEngine = TableEngine.buildTableQueryPlan(ctx, executeEngine, vcursor, bindVariableMap, tableDestinationResponse);
        if (!(primitiveEngine instanceof TableQueryEngine)) {
            throw new SQLException("error: primitiveEngine should be TableQueryEngine");
        }
        return ((TableQueryEngine) primitiveEngine).getResolvedShardListMap();
    }

    @Override
    public IExecute.ExecuteMultiShardResponse execute(final IContext ctx, final Vcursor vcursor, final Map<String, BindVariable> bindVariableMap, final boolean wantFields) throws SQLException {
        TableEngine.TableDestinationResponse tableDestinationResponse = this.getResolveDestinationResult(bindVariableMap);
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
            VtResultSet tableBatchExecuteResult = TableEngine.getTableBatchExecuteResult(ctx, executeEngine, vcursor, bindVariableMap, tableDestinationResponse);
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

    private TableEngine.TableDestinationResponse getResolveDestinationResult(final Map<String, BindVariable> bindVariableMap) throws SQLException {
        TableEngine.TableDestinationResponse tableDestinationResponse;
        switch (this.routeOpcode) {
            case SelectScatter:
                tableDestinationResponse = TableEngine.paramsAllShard(this.logicTables, bindVariableMap);
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

    private TableEngine.TableDestinationResponse paramsSelectEqual(final Map<String, BindVariable> bindVariableMap) throws SQLException {
        VtValue value = this.vtPlanValueList.get(0).resolveValue(bindVariableMap);
        List<ActualTable> actualTables = new ArrayList<>();
        for (LogicTable ltb : this.logicTables) {
            ActualTable actualTable = ltb.map(value);
            if (actualTable == null) {
                throw new SQLException("cannot calculate split table, logic table: " + ltb.getLogicTable());
            }
            actualTables.add(actualTable);
        }

        TableDestinationGroup tableDestinationGroup = new TableDestinationGroup(actualTables);
        return new TableEngine.TableDestinationResponse(
            Lists.newArrayList(tableDestinationGroup),
            Lists.newArrayList(bindVariableMap)
        );
    }

    private TableEngine.TableDestinationResponse paramsSelectIn(final Map<String, BindVariable> bindVariableMap) throws SQLException {
        List<VtValue> keys = this.vtPlanValueList.get(0).resolveList(bindVariableMap);

        return TableEngine.resolveTablesQueryIn(this.logicTables, bindVariableMap, keys);
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
