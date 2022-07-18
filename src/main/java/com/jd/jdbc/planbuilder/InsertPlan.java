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

package com.jd.jdbc.planbuilder;

import com.jd.jdbc.VSchemaManager;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.InsertEngine;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.table.TableInsertEngine;
import com.jd.jdbc.key.Bytes;
import com.jd.jdbc.key.DestinationKeyspaceID;
import com.jd.jdbc.planbuilder.tableplan.TableRoutePlan;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.SqlParser;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLDefaultExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLMethodInvokeExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNullExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLQueryExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLInsertStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlInsertReplaceStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.SwitchTableVisitor;
import com.jd.jdbc.sqltypes.VtPlanValue;
import com.jd.jdbc.tindexes.LogicTable;
import com.jd.jdbc.vindexes.VKeyspace;
import io.netty.util.internal.StringUtil;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import lombok.AllArgsConstructor;
import lombok.Getter;
import vschema.Vschema;

import static com.jd.jdbc.vindexes.Vschema.TYPE_PINNED_TABLE;

public class InsertPlan {
    public static PrimitiveEngine newBuildInsertPlan(MySqlInsertReplaceStatement stmt, VSchemaManager vm, String defaultKeyspace) throws SQLException {
        PrimitiveBuilder pb = new PrimitiveBuilder(vm, defaultKeyspace, Jointab.newJointab(SqlParser.getBindVars(stmt)));
        SQLExprTableSource tableSource = stmt.getTableSource().clone();
        AbstractRoutePlan arb = pb.processDmlTable(tableSource);
        if (pb.getSymtab().getTables().size() != 1) {
            throw new SQLException("unsupported: multi-table insert statement in sharded table");
        }
        if (arb instanceof TableRoutePlan) {
            return buildPartitionTableInsetPlan(stmt, vm, defaultKeyspace);
        }

        RoutePlan rb = (RoutePlan) arb;

        // The table might have been routed to a different one.
        stmt.setTableSource(tableSource);
        if (rb.getRouteEngine().getTargetDestination() != null) {
            throw new SQLException("unsupported: INSERT with a target destination");
        }

        String tableName = null;
        Vschema.Table vschemaTable = Vschema.Table.newBuilder().setType(TYPE_PINNED_TABLE).build();
        for (Map.Entry<String, Symtab.Table> entry : pb.getSymtab().getTables().entrySet()) {
            // There is only one table.
            tableName = entry.getKey();
            Symtab.Table tVal = entry.getValue();
            vschemaTable = tVal.getVschemaTable();
        }
        if (!rb.getRouteEngine().getKeyspace().getSharded()) {
            if (!pb.finalizeUnshardedDmlSubqueries(stmt)) {
                throw new SQLException("unsupported: sharded subquery in insert values");
            }
            return buildInsertUnshardedPlan(stmt, vm, defaultKeyspace, tableName, vschemaTable);
        }
        if (!StringUtil.isNullOrEmpty(vschemaTable.getPinned())) {
            return buildInsertUnshardedPlan(stmt, vm, defaultKeyspace, tableName, vschemaTable);
        }
        return buildInsertShardedPlan(stmt, vm, defaultKeyspace, tableName, vschemaTable);
    }

    /**
     * @param stmt
     * @param vm
     * @param defaultKeyspace
     * @param tableName
     * @return
     * @throws SQLException
     */
    private static PrimitiveEngine buildInsertUnshardedPlan(MySqlInsertReplaceStatement stmt, VSchemaManager vm, String defaultKeyspace, String tableName, Vschema.Table vschemaTable)
        throws SQLException {
        VKeyspace keyspace = new VKeyspace(defaultKeyspace, vm.getKeyspace(defaultKeyspace).getSharded());
        InsertEngine insertEngine = new InsertEngine(Engine.InsertOpcode.InsertUnsharded, keyspace, vschemaTable, tableName);
        if (!StringUtil.isNullOrEmpty(vschemaTable.getPinned())) {
            insertEngine.setInsertOpcode(Engine.InsertOpcode.InsertByDestination);
            insertEngine.setTargetDestination(new DestinationKeyspaceID(Bytes.decodeToByteArray(vschemaTable.getPinned())));
        }

        List<SQLInsertStatement.ValuesClause> rowList;
        List<SQLInsertStatement.ValuesClause> valuesList = stmt.getValuesList();
        if (stmt.getSelectQuery().getSubQuery() != null) {
            if (insertEngine.getTable().getAutoIncrement() != Vschema.AutoIncrement.getDefaultInstance()) {
                throw new SQLException("unsupported: auto-inc and select in insert");
            }
            insertEngine.setQuery(generateQuery(stmt));
            insertEngine.setInsertReplaceStmt(stmt);
            return insertEngine;
        } else if (valuesHasQuery(valuesList)) {
            if (insertEngine.getTable().getAutoIncrement() != Vschema.AutoIncrement.getDefaultInstance()) {
                throw new SQLException("unsupported: auto-inc and select in insert");
            }
            insertEngine.setQuery(generateQuery(stmt));
            insertEngine.setInsertReplaceStmt(stmt);
            return insertEngine;
        } else if (valuesList != null && !valuesList.isEmpty()) {
            rowList = valuesList;
        } else {
            throw new SQLException("BUG: unexpected construct in insert: " + valuesList);
        }

        if (insertEngine.getTable().getAutoIncrement() == Vschema.AutoIncrement.getDefaultInstance()) {
            insertEngine.setQuery(generateQuery(stmt));
            insertEngine.setInsertReplaceStmt(stmt);
        } else {
            // Table has auto-inc and has a VALUES clause.
            if (stmt.getColumns() == null || stmt.getColumns().isEmpty()) {
                if (vschemaTable.getColumnListAuthoritative()) {
                    populateInsertColumnlist(stmt, vschemaTable);
                } else {
                    throw new SQLException("column list required for tables with auto-inc columns");
                }
            }
            for (SQLInsertStatement.ValuesClause row : rowList) {
                if (stmt.getColumns().size() != row.getValues().size()) {
                    throw new SQLException("column list doesn't match values");
                }
            }
            modifyForAutoinc(stmt, vm, insertEngine, defaultKeyspace);
            insertEngine.setQuery(generateQuery(stmt));
            insertEngine.setInsertReplaceStmt(stmt);
        }
        return insertEngine;
    }

    /**
     * @param stmt
     * @param vm
     * @param defaultKeyspace
     * @param tableName
     * @return
     * @throws SQLException
     */
    public static PrimitiveEngine buildInsertShardedPlan(MySqlInsertReplaceStatement stmt, VSchemaManager vm, String defaultKeyspace, String tableName, Vschema.Table vschemaTable)
        throws SQLException {
        VKeyspace keyspace = new VKeyspace(defaultKeyspace, vm.getKeyspace(defaultKeyspace).getSharded());

        String columnName = vschemaTable.getColumnVindexes(0).getColumn();
        Vschema.ColumnVindex newColumnVindex = vschemaTable.getColumnVindexes(0).toBuilder().clearColumns().addColumns(columnName).buildPartial();
        vschemaTable = vschemaTable.toBuilder().clearColumnVindexes().addColumnVindexes(newColumnVindex).buildPartial();

        InsertEngine insertEngine = new InsertEngine(Engine.InsertOpcode.InsertSharded, keyspace, vschemaTable, tableName);
        if (stmt.isIgnore()) {
            insertEngine.setInsertOpcode(Engine.InsertOpcode.InsertShardedIgnore);
        }
        List<SQLExpr> duplicateKeyUpdateExprList = stmt.getDuplicateKeyUpdate();
        if (duplicateKeyUpdateExprList != null && !duplicateKeyUpdateExprList.isEmpty()) {
            if (isVindexChanging(duplicateKeyUpdateExprList, insertEngine.getTable().getColumnVindexesList())) {
                throw new SQLException("unsupported: DML cannot change vindex column");
            }
            insertEngine.setInsertOpcode(Engine.InsertOpcode.InsertShardedIgnore);
        }
        if (stmt.getColumns() == null || stmt.getColumns().isEmpty()) {
            if (vschemaTable.getColumnListAuthoritative()) {
                populateInsertColumnlist(stmt, vschemaTable);
            } else {
                throw new SQLException("no column list");
            }
        }

        List<SQLInsertStatement.ValuesClause> valuesList = stmt.getValuesList();
        if (stmt.getSelectQuery().getSubQuery() != null) {
            throw new SQLException("unsupported: insert into select");
        } else if (valuesList != null && !valuesList.isEmpty()) {
            if (valuesHasQuery(valuesList)) {
                throw new SQLException("unsupported: subquery in insert values");
            }
        } else {
            throw new SQLException("BUG: unexpected construct in insert: " + valuesList);
        }

        for (SQLInsertStatement.ValuesClause valuesClause : valuesList) {
            if (stmt.getColumns().size() != valuesClause.getValues().size()) {
                throw new SQLException("column list doesn't match values");
            }
        }

        if (insertEngine.getTable().getAutoIncrement() != Vschema.AutoIncrement.getDefaultInstance()) {
            modifyForAutoinc(stmt, vm, insertEngine, defaultKeyspace);
        }

        // Fill out the 3-d Values structure. Please see documentation of Insert.Values for details.
        List<VtPlanValue> routeValueList = new ArrayList<>();
        for (Vschema.ColumnVindex columnVindex : insertEngine.getTable().getColumnVindexesList()) {
            List<VtPlanValue> bList = new ArrayList<>();
            for (String colName : columnVindex.getColumnsList()) {
                List<VtPlanValue> cList = new ArrayList<>();
                Integer colNum = findOrAddColumn(stmt, colName);
                for (SQLInsertStatement.ValuesClause valuesClause : valuesList) {
                    SQLExpr expr = valuesClause.getValues().get(colNum);
                    VtPlanValue innerPv;
                    try {
                        innerPv = SqlParser.newPlanValue(expr);
                    } catch (SQLException e) {
                        throw new SQLException("could not compute value for vindex or auto-inc column: " + e.getMessage());
                    }
                    cList.add(innerPv);
                }
                bList.add(new VtPlanValue(cList));
            }
            routeValueList.add(new VtPlanValue(bList));
        }
        for (Vschema.ColumnVindex columnVindex : insertEngine.getTable().getColumnVindexesList()) {
            for (String col : columnVindex.getColumnsList()) {
                Integer colNum = findOrAddColumn(stmt, col);
                for (int i = 0; i < valuesList.size(); i++) {
                    String name = ":" + Engine.insertVarName(col, i);
                    SQLExpr expr = valuesList.get(i).getValues().get(colNum);
                    SQLVariantRefExpr variantRefExpr = new SQLVariantRefExpr(name);
                    if (expr instanceof SQLVariantRefExpr) {
                        variantRefExpr.setParent(expr.getParent());
                        variantRefExpr.setIndex(((SQLVariantRefExpr) expr).getIndex());
                    }
                    valuesList.get(i).getValues().set(colNum, variantRefExpr);
                }
            }
        }
        insertEngine.setVindexValueList(routeValueList);
        insertEngine.setQuery(generateQuery(stmt));
        insertEngine.setInsertReplaceStmt(stmt);
        generateInsertShardedQuery(stmt, insertEngine, valuesList);
        return insertEngine;
    }

    /**
     * @param stmt
     * @param insertEngine
     * @param valuesList
     */
    private static void generateInsertShardedQuery(MySqlInsertReplaceStatement stmt, InsertEngine insertEngine, List<SQLInsertStatement.ValuesClause> valuesList) {
        StringBuilder prefixBuf = new StringBuilder();
        List<String> midList = new ArrayList<>();
        StringBuilder suffixBuf = new StringBuilder();

        if (stmt instanceof MySqlInsertStatement) {
            prefixBuf.append("insert ");
        } else {
            prefixBuf.append("replace ");
        }

        if (stmt.isIgnore()) {
            prefixBuf.append("ignore ");
        }
        prefixBuf.append("into ").append(stmt.getTableSource()).append(" ");

        List<SQLExpr> columnList = stmt.getColumns();
        if (columnList != null && !columnList.isEmpty()) {
            StringJoiner sj = new StringJoiner(", ", "(", ")");
            for (SQLExpr column : columnList) {
                sj.add(column.toString());
            }
            prefixBuf.append(sj).append(" ");
        }
        prefixBuf.append("values ");

        for (SQLInsertStatement.ValuesClause valuesClause : valuesList) {
            StringJoiner sj = new StringJoiner(", ", "(", ")");
            for (SQLExpr value : valuesClause.getValues()) {
                sj.add(value.toString());
            }
            midList.add(sj.toString());
        }

        List<SQLExpr> duplicateKeyUpdateList = stmt.getDuplicateKeyUpdate();
        if (duplicateKeyUpdateList != null && !duplicateKeyUpdateList.isEmpty()) {
            suffixBuf.append("on duplicate key update ");
            StringJoiner sj = new StringJoiner(", ", "", "");
            for (SQLExpr expr : duplicateKeyUpdateList) {
                sj.add(expr.toString());
            }
            suffixBuf.append(sj);
        }

        insertEngine.setPrefix(prefixBuf.toString());
        insertEngine.setMidList(midList);
        insertEngine.setMidExprList(valuesList);
        insertEngine.setSuffix(suffixBuf.toString());
        insertEngine.setSuffixExpr(duplicateKeyUpdateList);
    }

    private static PrimitiveEngine buildPartitionTableInsetPlan(MySqlInsertReplaceStatement stmt, VSchemaManager vm, String defaultKeyspace) throws SQLException {
        VKeyspace keyspace = new VKeyspace(defaultKeyspace, vm.getKeyspace(defaultKeyspace).getSharded());
        List<LogicTable> logicTables = PlanBuilder.getLogicTables(defaultKeyspace, stmt);
        if (logicTables.size() != 1) {
            throw new SQLException("can only have one logic table in insert/replace statement");
        }
        LogicTable ltb = logicTables.get(0);

        TableInsertEngine insertEngine = new TableInsertEngine(Engine.InsertOpcode.InsertSharded, keyspace, ltb);
        if (stmt.isIgnore()) {
            insertEngine.setInsertOpcode(Engine.InsertOpcode.InsertShardedIgnore);
        }
        List<SQLExpr> duplicateKeyUpdateExprList = stmt.getDuplicateKeyUpdate();
        if (duplicateKeyUpdateExprList != null && !duplicateKeyUpdateExprList.isEmpty()) {
            if (isTindexChangine(duplicateKeyUpdateExprList, insertEngine.getTable())) {
                throw new SQLException("unsupported: DML cannot change tindex column");
            }
            insertEngine.setInsertOpcode(Engine.InsertOpcode.InsertShardedIgnore);
        }
        if (stmt.getColumns() == null || stmt.getColumns().isEmpty()) {
            throw new SQLException("no column list");
        }

        List<SQLInsertStatement.ValuesClause> valuesList = stmt.getValuesList();
        if (stmt.getSelectQuery().getSubQuery() != null) {
            throw new SQLException("unsupported: insert into select");
        } else if (valuesList != null && !valuesList.isEmpty()) {
            if (valuesHasQuery(valuesList)) {
                throw new SQLException("unsupported: subquery in insert values");
            }
        } else {
            throw new SQLException("BUG: unexpected construct in insert: " + valuesList);
        }

        for (SQLInsertStatement.ValuesClause valuesClause : valuesList) {
            if (stmt.getColumns().size() != valuesClause.getValues().size()) {
                throw new SQLException("column list doesn't match values");
            }
        }

        List<VtPlanValue> routeTableValueList = new ArrayList<>();
        Integer colNum = findColumn(stmt, insertEngine.getTable().getTindexCol().getColumnName());
        if (colNum == -1) {
            throw new SQLException("Missing tindex column: insert into sharded table must have tindex column");
        }
        for (int i = 0; i < valuesList.size(); i++) {
            SQLInsertStatement.ValuesClause valuesClause = valuesList.get(i);
            SQLExpr expr = valuesClause.getValues().get(colNum);
            VtPlanValue innerPv;
            try {
                innerPv = SqlParser.newPlanValue(expr);
            } catch (SQLException e) {
                throw new SQLException("could not compute value for vindex or auto-inc column: " + e.getMessage());
            }
            routeTableValueList.add(innerPv);
            String name = ":" + Engine.insertVarName(insertEngine.getTable().getTindexCol().getColumnName(), i);
            SQLVariantRefExpr variantRefExpr = new SQLVariantRefExpr(name);
            if (expr instanceof SQLVariantRefExpr) {
                variantRefExpr.setParent(expr.getParent());
                variantRefExpr.setIndex(((SQLVariantRefExpr) expr).getIndex());
            }
            valuesList.get(i).getValues().set(colNum, variantRefExpr);
        }
        insertEngine.setVindexValueList(routeTableValueList);
        insertEngine.setInsertReplaceStmt(stmt);
        MySqlInsertReplaceStatement stmtClone = (MySqlInsertReplaceStatement) stmt.clone();
        SwitchTableVisitor visitor = new SwitchTableVisitor(new HashMap<String, String>() {{
            put(ltb.getLogicTable(), ltb.getFirstActualTableName());
        }});
        stmtClone.accept(visitor);
        InsertEngine innerInsertEigine = (InsertEngine) newBuildInsertPlan(stmtClone, vm, defaultKeyspace);
        insertEngine.setInsertOpcode(innerInsertEigine.getInsertOpcode());
        insertEngine.setInsertEngine(innerInsertEigine);
        return insertEngine;
    }

    /**
     * @param valuesClauseList
     * @return
     */
    private static Boolean valuesHasQuery(List<SQLInsertStatement.ValuesClause> valuesClauseList) {
        if (valuesClauseList != null && !valuesClauseList.isEmpty()) {
            for (SQLInsertStatement.ValuesClause valuesClause : valuesClauseList) {
                for (SQLExpr expr : valuesClause.getValues()) {
                    if (expr instanceof SQLQueryExpr) {
                        return Boolean.TRUE;
                    }
                }
            }
        }
        return Boolean.FALSE;
    }

    /**
     * @param insertStmt
     * @param columnName
     * @return
     */
    private static Integer findOrAddColumn(MySqlInsertReplaceStatement insertStmt, String columnName) {
        int i = findColumn(insertStmt, columnName);
        if (i != -1) {
            return i;
        }
        insertStmt.addColumn(new SQLIdentifierExpr(columnName));
        for (SQLInsertStatement.ValuesClause valuesClause : insertStmt.getValuesList()) {
            valuesClause.getValues().add(new SQLNullExpr());
        }
        return insertStmt.getColumns().size() - 1;
    }

    /**
     * @param statement
     * @return
     */
    private static String generateQuery(SQLStatement statement) {
        return SQLUtils.toMySqlString(statement, SQLUtils.NOT_FORMAT_OPTION);
    }

    public static Integer findColumn(MySqlInsertReplaceStatement insertStmt, String columnName) {
        for (int i = 0; i < insertStmt.getColumns().size(); i++) {
            if (SQLUtils.nameEquals(columnName, insertStmt.getColumns().get(i).toString())) {
                return i;
            }
            String columnNameWithBackQuota = "`" + columnName + "`";
            if (SQLUtils.nameEquals(columnNameWithBackQuota, insertStmt.getColumns().get(i).toString())) {
                return i;
            }
        }
        return -1;
    }

    private static ColValResponse findColValFromBinaryExpr(SQLBinaryOpExpr expr) {
        SQLExpr left = expr.getLeft();
        SQLExpr right = expr.getRight();
        String colName;
        SQLExpr valueExpr;
        if (left instanceof SQLName) {
            colName = ((SQLName) left).getSimpleName();
            valueExpr = right;
        } else {
            colName = ((SQLName) right).getSimpleName();
            valueExpr = left;
        }
        return new ColValResponse(colName, valueExpr);
    }

    /**
     * isVindexChanging returns true if any of the update
     * expressions modify a vindex column.
     *
     * @param duplicateKeyUpdateExprList
     * @param columnVindexesList
     * @return
     */
    static Boolean isVindexChanging(List<SQLExpr> duplicateKeyUpdateExprList, List<Vschema.ColumnVindex> columnVindexesList) {
        for (SQLExpr expr : duplicateKeyUpdateExprList) {
            if (expr instanceof SQLBinaryOpExpr) {
                ColValResponse response = findColValFromBinaryExpr((SQLBinaryOpExpr) expr);
                String colName = response.getColName();
                SQLExpr valueExpr = response.getValueExpr();
                for (Vschema.ColumnVindex columnVindex : columnVindexesList) {
                    for (String col : columnVindex.getColumnsList()) {
                        if (SQLUtils.nameEquals(col, colName)) {
                            if (!(valueExpr instanceof SQLMethodInvokeExpr
                                && SQLUtils.nameEquals("VALUES", ((SQLMethodInvokeExpr) valueExpr).getMethodName()))) {
                                return Boolean.TRUE;
                            }
                            // update on duplicate key is changing the vindex column, not supported.
                            SQLExpr paramExpr = ((SQLMethodInvokeExpr) valueExpr).getArguments().get(0);
                            if (paramExpr instanceof SQLName
                                && !SQLUtils.nameEquals(((SQLName) paramExpr).getSimpleName(), colName)) {
                                return Boolean.TRUE;
                            }
                        }
                    }
                }
            }
        }
        return Boolean.FALSE;
    }

    static Boolean isTindexChangine(List<SQLExpr> duplicateKeyUpdateExprList, LogicTable logicTable) {
        for (SQLExpr expr : duplicateKeyUpdateExprList) {
            if (expr instanceof SQLBinaryOpExpr) {
                ColValResponse response = findColValFromBinaryExpr((SQLBinaryOpExpr) expr);
                String colName = response.getColName();
                SQLExpr valueExpr = response.getValueExpr();
                com.jd.jdbc.tindexes.Column column = logicTable.getTindexCol();
                if (column.getColumnName().equalsIgnoreCase(colName)) {
                    if (!(valueExpr instanceof SQLMethodInvokeExpr
                        && SQLUtils.nameEquals("VALUES", ((SQLMethodInvokeExpr) valueExpr).getMethodName()))) {
                        return Boolean.TRUE;
                    }
                    // update on duplicate key is changing the vindex column, not supported.
                    SQLExpr paramExpr = ((SQLMethodInvokeExpr) valueExpr).getArguments().get(0);
                    if (paramExpr instanceof SQLName
                        && !SQLUtils.nameEquals(((SQLName) paramExpr).getSimpleName(), colName)) {
                        return Boolean.TRUE;
                    }
                }
            }
        }
        return Boolean.FALSE;
    }

    /**
     * modifyForAutoinc modfies the AST and the plan to generate
     * necessary autoinc values. It must be called only if eins.Table.AutoIncrement
     * is set. Bind variable names are generated using baseName.
     *
     * @param insertStmt
     * @param insertEngine
     * @param defaultKeyspace
     */
    static void modifyForAutoinc(MySqlInsertReplaceStatement insertStmt, VSchemaManager vm, InsertEngine insertEngine, String defaultKeyspace) throws SQLException {
        Integer autoIncColIndex = findOrAddColumn(insertStmt, insertEngine.getTable().getAutoIncrement().getColumn());
        VtPlanValue autoIncValues = new VtPlanValue();
        for (int i = 0; i < insertStmt.getValuesList().size(); i++) {
            List<SQLExpr> row = insertStmt.getValuesList().get(i).getValues();
            SQLExpr autoIncExpr = row.get(autoIncColIndex);
            // Support the DEFAULT keyword by treating it as null
            if (autoIncExpr instanceof SQLDefaultExpr) {
                SQLNullExpr nullExpr = new SQLNullExpr();
                nullExpr.setParent(autoIncExpr.getParent());
                row.set(autoIncColIndex, nullExpr);
            }
            VtPlanValue planValue;
            try {
                planValue = SqlParser.newPlanValue(autoIncExpr);
            } catch (SQLException e) {
                throw new SQLException("could not compute value for vindex or auto-inc column: " + e.getMessage());
            }
            autoIncValues.getVtPlanValueList().add(planValue);
            SQLVariantRefExpr variantRefExpr = new SQLVariantRefExpr(InsertEngine.Generate.SEQ_VAR_NAME + i);
            variantRefExpr.setIndex(InsertEngine.Generate.SEQ_VAR_REFINDEX);
            variantRefExpr.setParent(autoIncExpr.getParent());
            row.set(autoIncColIndex, variantRefExpr);
        }
        String sequenceTableName = insertEngine.getTable().getAutoIncrement().getSequence();
        if (sequenceTableName.contains(".")) {
            sequenceTableName = sequenceTableName.split("\\.")[1];
        }
        Vschema.Table table = vm.getTable(defaultKeyspace, sequenceTableName);
        insertEngine.setGenerate(
            new InsertEngine.Generate(
                new VKeyspace(defaultKeyspace),
                autoIncValues,
                sequenceTableName,
                table.getPinned()
            )
        );
    }

    /**
     * @param insertStmt
     * @param vschemaTable
     */
    static void populateInsertColumnlist(MySqlInsertReplaceStatement insertStmt, Vschema.Table vschemaTable) {
        for (Vschema.Column column : vschemaTable.getColumnsList()) {
            insertStmt.addColumn(new SQLIdentifierExpr(column.getName()));
        }
    }

    @Getter
    @AllArgsConstructor
    private static class ColValResponse {
        private final String colName;

        private final SQLExpr valueExpr;
    }
}
