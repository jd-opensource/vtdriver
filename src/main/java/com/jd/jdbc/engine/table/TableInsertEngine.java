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

import com.google.protobuf.ByteString;
import com.jd.jdbc.IExecute;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.InsertEngine;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.TableShardQuery;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.engine.sequence.Generate;
import com.jd.jdbc.planbuilder.MultiQueryPlan;
import com.jd.jdbc.queryservice.util.RoleUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLInsertStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlInsertReplaceStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.jd.jdbc.sqlparser.utils.TableNameUtils;
import com.jd.jdbc.sqltypes.SqlTypes;
import com.jd.jdbc.sqltypes.VtPlanValue;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.BoundQuery;
import com.jd.jdbc.srvtopo.ResolvedShard;
import com.jd.jdbc.tindexes.ActualTable;
import com.jd.jdbc.tindexes.LogicTable;
import com.jd.jdbc.vindexes.VKeyspace;
import io.vitess.proto.Query;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TableInsertEngine implements PrimitiveEngine, TableShardQuery {
    /**
     * Opcode is the execution opcode.
     */
    private Engine.InsertOpcode insertOpcode;

    /**
     * Keyspace specifies the keyspace to send the query to.
     */
    private VKeyspace keyspace;

    /**
     * Query specifies the query to be executed.
     * For InsertSharded plans, this value is unused,
     * and Prefix, Mid and Suffix are used instead.
     */
    private MySqlInsertReplaceStatement insertReplaceStmt;

    /**
     * midExprList for sharded insert plans.
     */
    private List<SQLInsertStatement.ValuesClause> midExprList;

    /**
     * VindexValues specifies values for all the vindex columns.
     * This is a three-dimensional data structure:
     * Insert.Values[i] represents the values to be inserted for the i'th colvindex (i < len(Insert.Table.ColumnVindexes))
     * Insert.Values[i].Values[j] represents values for the j'th column of the given colVindex (j < len(colVindex[i].Columns)
     * Insert.Values[i].Values[j].Values[k] represents the value pulled from row k for that column: (k < len(ins.rows))
     */
    private List<VtPlanValue> vindexValueList;

    private LogicTable table;

    /**
     * Generate is only set for inserts where a sequence must be generated.
     */
    private Generate generate;

    /**
     * Option to override the standard behavior and allow a multi-shard insert
     * to use single round trip autocommit.
     * <p>
     * This is a clear violation of the SQL semantics since it means the statement
     * is not atomic in the presence of PK conflicts on one shard and not another.
     * However some application use cases would prefer that the statement partially
     * succeed in order to get the performance benefits of autocommit.
     */
    private Boolean multiShardAutocommit;

    private InsertEngine insertEngine;

    public TableInsertEngine(final Engine.InsertOpcode insertOpcode, final VKeyspace keyspace, final LogicTable table) {
        this.insertOpcode = insertOpcode;
        this.keyspace = keyspace;
        this.table = table;
        this.generate = null;
        this.multiShardAutocommit = Boolean.FALSE;
    }

    @Override
    public String getKeyspaceName() {
        return null;
    }

    @Override
    public String getTableName() {
        if (this.table != null) {
            return this.table.getLogicTable();
        }
        return "";
    }

    @Override
    public IExecute.ExecuteMultiShardResponse execute(final IContext ctx, final Vcursor vcursor, final Map<String, BindVariable> bindVariableMap, final boolean wantFields) throws SQLException {
        if (RoleUtils.notMaster(ctx)) {
            throw new SQLException("insert is not allowed for read only connection");
        }
        long insertId;
        PrimitiveEngine primitiveEngine;
        switch (this.insertOpcode) {
            case InsertByDestination:
            case InsertUnsharded:
                insertId = Generate.processGenerate(vcursor, generate, bindVariableMap);
                primitiveEngine = getInsertUnshardedEngine(ctx, vcursor, bindVariableMap);
                break;
            case InsertSharded:
            case InsertShardedIgnore:
                insertId = Generate.processGenerate(vcursor, generate, bindVariableMap);
                primitiveEngine = getInsertShardedEngine(ctx, vcursor, bindVariableMap);
                break;
            default:
                throw new SQLException("unsupported query route: " + this.insertOpcode);
        }

        if (primitiveEngine == null) {
            throw new SQLException("error: generate table insert engine failed");
        }
        VtResultSet resultSet = TableEngine.execCollectMultQueries(ctx, primitiveEngine, vcursor, wantFields);
        if (insertId != 0) {
            resultSet.setInsertID(insertId);
        }
        return new IExecute.ExecuteMultiShardResponse(resultSet).setUpdate();
    }

    private PrimitiveEngine getInsertUnshardedEngine(final IContext ctx, final Vcursor vcursor, final Map<String, BindVariable> bindVariableMap) throws SQLException {
        List<ActualTable> actualTables = new ArrayList<>();
        List<List<Query.Value>> indexesPerTable = new ArrayList<>();
        buildActualTables(bindVariableMap, actualTables, indexesPerTable);

        List<SQLInsertStatement.ValuesClause> innerEngineMidExprList = this.insertReplaceStmt.getValuesList();
        return getInsertEngine(ctx, vcursor, bindVariableMap, actualTables, indexesPerTable, null, innerEngineMidExprList);
    }

    private PrimitiveEngine getInsertShardedEngine(final IContext ctx, final Vcursor vcursor, final Map<String, BindVariable> bindVariableMap) throws SQLException {
        List<ActualTable> actualTables = new ArrayList<>();
        List<List<Query.Value>> indexesPerTable = new ArrayList<>();
        buildActualTables(bindVariableMap, actualTables, indexesPerTable);

        // temp store inner insert engine's insert data rows and their corresponding plan value list
        // as rows and their corresponding plan value will be redistributed to different tables
        List<VtPlanValue> innerEnginePlanValueList = this.insertEngine.getVindexValueList().get(0).getVtPlanValueList().get(0).getVtPlanValueList();
        List<SQLInsertStatement.ValuesClause> innerEngineMidExprList = this.insertEngine.getMidExprList();
        return getInsertEngine(ctx, vcursor, bindVariableMap, actualTables, indexesPerTable, innerEnginePlanValueList, innerEngineMidExprList);
    }

    private PrimitiveEngine getInsertEngine(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap,
                                            List<ActualTable> actualTables, List<List<Query.Value>> indexesPerTable, List<VtPlanValue> innerEnginePlanValueList,
                                            List<SQLInsertStatement.ValuesClause> innerEngineMidExprList) throws SQLException {
        List<IExecute.ResolvedShardQuery> shardQueryList = new ArrayList<>();
        List<PrimitiveEngine> sourceList = new ArrayList<>();
        List<Map<String, BindVariable>> batchBindVariableMap = new ArrayList<>();
        Map<String, String> shardTableLTMap = new HashMap<>();

        List<VtPlanValue> tempRouteValueList = new ArrayList<>();
        List<VtPlanValue> bList = new ArrayList<>();
        bList.add(new VtPlanValue());
        tempRouteValueList.add(new VtPlanValue(bList));

        for (int i = 0; i < actualTables.size(); i++) {
            ActualTable actualTable = actualTables.get(i);
            shardTableLTMap.put(actualTable.getActualTableName(), actualTable.getLogicTable().getLogicTable());
            List<Query.Value> indexes = indexesPerTable.get(i);
            List<VtPlanValue> tempInnerPlanValueList = new ArrayList<>();
            List<SQLInsertStatement.ValuesClause> valuesClauseList = new ArrayList<>();
            for (Query.Value idx : indexes) {
                long index = Long.parseLong(idx.getValue().toString(StandardCharsets.UTF_8));
                valuesClauseList.add(innerEngineMidExprList.get(Math.toIntExact(index)));
                if (innerEnginePlanValueList != null) {
                    tempInnerPlanValueList.add(innerEnginePlanValueList.get(Math.toIntExact(index)));
                }
            }
            Map<String, String> switchTables = new HashMap<>();
            switchTables.put(actualTable.getLogicTable().getLogicTable(), actualTable.getActualTableName());
            Map<String, BindVariable> bindVarClone = new HashMap<>(bindVariableMap);

            tempRouteValueList.get(0).getVtPlanValueList().get(0).setVtPlanValueList(tempInnerPlanValueList);
            shardQueryList.add(getChangedInsertQueries(ctx, vcursor, bindVarClone, switchTables, valuesClauseList, tempRouteValueList));
            sourceList.add(this.insertEngine);
            batchBindVariableMap.add(bindVariableMap);
        }
        return MultiQueryPlan.buildTableQueryPlan(sourceList, shardQueryList, batchBindVariableMap, shardTableLTMap);
    }

    private void buildActualTables(Map<String, BindVariable> bindVariableMap, List<ActualTable> actualTables, List<List<Query.Value>> indexesPerTable) throws SQLException {
        Map<ActualTable, List<Query.Value>> actualTableMap = new TreeMap<>();
        // compute target table partitions and their corresponding value's index
        for (int rowNum = 0; rowNum < this.vindexValueList.size(); rowNum++) {
            VtPlanValue planValue = this.vindexValueList.get(rowNum);
            VtValue rowResolvedValue = planValue.resolveValue(bindVariableMap);
            String col = this.table.getTindexCol().getColumnName();
            String name = Engine.insertVarName(col, rowNum);
            bindVariableMap.put(name, SqlTypes.valueBindVariable(rowResolvedValue));
            ActualTable actualTable = this.table.map(rowResolvedValue);
            if (actualTable == null) {
                throw new SQLException("cannot calculate split table, logic table: " + table.getLogicTable() + "； shardingColumnValue: " + rowResolvedValue);
            }
            Query.Value index = Query.Value.newBuilder().setValue(ByteString.copyFrom(String.valueOf(rowNum).getBytes())).build();
            if (actualTableMap.containsKey(actualTable)) {
                actualTableMap.get(actualTable).add(index);
            } else {
                actualTableMap.put(actualTable, new ArrayList<>());
                actualTableMap.get(actualTable).add(index);
            }
        }

        for (Map.Entry<ActualTable, List<Query.Value>> actualTableEntry : actualTableMap.entrySet()) {
            actualTables.add(actualTableEntry.getKey());
            indexesPerTable.add(actualTableEntry.getValue());
        }
    }

    private IExecute.ResolvedShardQuery getChangedInsertQueries(final IContext ctx, final Vcursor vcursor, final Map<String, BindVariable> bindValues, final Map<String, String> switchTables,
                                                                final List<SQLInsertStatement.ValuesClause> valuesList, final List<VtPlanValue> tempPlanValueList) throws SQLException {
        StringBuilder prefixBuf = new StringBuilder();
        StringBuilder suffixBuf = new StringBuilder();

        if (this.insertReplaceStmt instanceof MySqlInsertStatement) {
            prefixBuf.append("insert ");
        } else {
            prefixBuf.append("replace ");
        }
        if (this.insertReplaceStmt.isIgnore()) {
            prefixBuf.append("ignore ");
        }
        String tableName = TableNameUtils.getTableSimpleName(this.insertReplaceStmt.getTableSource()).toLowerCase();
        if (switchTables.containsKey(tableName)) {
            tableName = switchTables.get(tableName);
        }
        prefixBuf.append("into ").append(tableName).append(" ");

        List<SQLExpr> columnList = this.insertReplaceStmt.getColumns();
        if (columnList != null && !columnList.isEmpty()) {
            StringJoiner sj = new StringJoiner(", ", "(", ")");
            for (SQLExpr column : columnList) {
                sj.add(column.toString());
            }
            prefixBuf.append(sj).append(" ");
        }
        prefixBuf.append("values ");

        List<SQLExpr> duplicateKeyUpdateList = this.insertReplaceStmt.getDuplicateKeyUpdate();
        if (duplicateKeyUpdateList != null && !duplicateKeyUpdateList.isEmpty()) {
            suffixBuf.append("on duplicate key update ");
            StringJoiner sj = new StringJoiner(", ", "", "");
            for (SQLExpr expr : duplicateKeyUpdateList) {
                sj.add(expr.toString());
            }
            suffixBuf.append(sj);
        }

        return this.insertEngine.resolveShardQuery(ctx, vcursor, bindValues, tempPlanValueList, valuesList, prefixBuf.toString(), suffixBuf.toString(), duplicateKeyUpdateList, switchTables);
    }

    @Override
    public Boolean needsTransaction() {
        return true;
    }

    @Override
    public Map<ResolvedShard, List<BoundQuery>> getShardQueryList(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        PrimitiveEngine primitiveEngine;
        switch (this.insertOpcode) {
            case InsertByDestination:
            case InsertUnsharded:
                primitiveEngine = getInsertUnshardedEngine(ctx, vcursor, bindVariableMap);
                break;
            case InsertSharded:
            case InsertShardedIgnore:
                primitiveEngine = getInsertShardedEngine(ctx, vcursor, bindVariableMap);
                break;
            default:
                throw new SQLException("unsupported query route: " + this.insertOpcode);
        }

        if (primitiveEngine == null) {
            throw new SQLException("error: generate table insert engine failed");
        }

        if (!(primitiveEngine instanceof TableQueryEngine)) {
            throw new SQLException("error: primitiveEngine should be TableQueryEngine");
        }
        return ((TableQueryEngine) primitiveEngine).getResolvedShardListMap();
    }
}
