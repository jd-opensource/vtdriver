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
import com.jd.jdbc.engine.DMLOpcode;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.UpdateEngine;
import com.jd.jdbc.engine.table.TableDMLEngine;
import com.jd.jdbc.engine.table.TableUpdateEngine;
import com.jd.jdbc.planbuilder.tableplan.TableDMLPlan;
import com.jd.jdbc.planbuilder.tableplan.TableRoutePlan;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.SqlParser;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.statement.SQLUpdateSetItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLUpdateStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.SwitchTableVisitor;
import com.jd.jdbc.sqltypes.VtPlanValue;
import com.jd.jdbc.tindexes.Column;
import com.jd.jdbc.tindexes.LogicTable;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Setter;
import vschema.Vschema;

public class UpdatePlan implements Builder {
    @Setter
    private UpdateEngine eUpdate;

    private Integer order;

    /**
     * extractValueFromUpdate given an UpdateExpr attempts to extracts the Value
     * it's holding. At the moment it only supports: StrVal, HexVal, IntVal, ValArg.
     * If a complex expression is provided (e.g set name = name + 1), the update will be rejected.
     */
    private static VtPlanValue extractValueFromUpdate(SQLUpdateSetItem upd) throws SQLException {
        VtPlanValue vtPlanValue = SqlParser.newPlanValue(upd.getValue());
        if (SqlParser.isSimpleTuple(upd.getValue())) {
            throw new SQLFeatureNotSupportedException("unsupported: Only values are supported. Invalid update on column: " + upd.getColumn().toString());
        }
        return vtPlanValue;
    }

    public static PrimitiveEngine newBuildUpdatePlan(SQLUpdateStatement stmt, VSchemaManager vm, String defaultKeyspace) throws SQLException {
        PrimitiveBuilder pb = new PrimitiveBuilder(vm, defaultKeyspace, Jointab.newJointab(SqlParser.getBindVars(stmt)));
        AbstractRoutePlan rb = pb.processDmlTable(stmt.getTableSource());

        if (rb instanceof TableRoutePlan) {
            return buildTableUpdatePlan(stmt, vm, defaultKeyspace, pb, (TableRoutePlan) rb);
        }
        return buildUpdatePlan(stmt, vm, defaultKeyspace, pb, (RoutePlan) rb);
    }

    private static TableUpdateEngine buildTableUpdatePlan(SQLUpdateStatement stmt, VSchemaManager vm, String defaultKeyspace, PrimitiveBuilder pb, TableRoutePlan rb) throws SQLException {
        TableDMLEngine tableDMLEngine = TableDMLPlan.buildTableDMLPlan(pb, rb, vm.getKeyspace(defaultKeyspace), DMLPlan.DMLType.UPDATE, stmt, stmt.getWhere(), defaultKeyspace);
        if (!(tableDMLEngine instanceof TableUpdateEngine)) {
            throw new SQLException("dml engine is not update");
        }

        TableUpdateEngine tableUpdateEngine = (TableUpdateEngine) tableDMLEngine;
        List<LogicTable> ltbs = tableUpdateEngine.getLogicTables();
        buildChangedTindexesVlaues(stmt, ltbs.get(0));
        Map<String, String> switchTable = new HashMap<>();
        for (LogicTable ltb : ltbs) {
            switchTable.put(ltb.getLogicTable(), ltb.getFirstActualTableName());
        }
        SwitchTableVisitor visitor = new SwitchTableVisitor(switchTable);
        SQLUpdateStatement updateclone = (SQLUpdateStatement) tableUpdateEngine.getQuery();
        updateclone.accept(visitor);
        tableUpdateEngine.setExecuteEngine(newBuildUpdatePlan(updateclone, vm, defaultKeyspace));
        return tableUpdateEngine;
    }

    private static UpdateEngine buildUpdatePlan(SQLUpdateStatement stmt, VSchemaManager vm, String defaultKeyspace, PrimitiveBuilder pb, RoutePlan rb) throws SQLException {
        DMLPlan.DMLPlanResult dmlPlanResult = DMLPlan.buildDMLPlan(pb, rb, vm.getKeyspace(defaultKeyspace), DMLPlan.DMLType.UPDATE, stmt,
            stmt.getWhere(), /*stmt.getOrderBy(), null, stmt.getHeadHintsDirect(),*/ stmt.getChildren(), defaultKeyspace);

        UpdateEngine updateEngine;
        if (!(dmlPlanResult.dmlEngine instanceof UpdateEngine)) {
            throw new SQLException("dml engine is not update");
        }
        updateEngine = (UpdateEngine) dmlPlanResult.dmlEngine;
        if (updateEngine.getOpcode() == DMLOpcode.Unsharded) {
            return updateEngine;
        }
        if (dmlPlanResult.dmlEngine.getKeyspace().getSharded()) {
            buildChangedVindexesValues(stmt, updateEngine.getTable(), dmlPlanResult.ksidCol);
        }

        return updateEngine;
    }

    private static void buildChangedTindexesVlaues(SQLUpdateStatement update, LogicTable ltb) throws SQLException {
        Map<String, VtPlanValue> vindexValueMap = new HashMap<>();
        Column tindexCol = ltb.getTindexCol();
        boolean found = false;
        for (SQLUpdateSetItem assignment : update.getItems()) {
            if (!SQLUtils.nameEquals(tindexCol.getColumnName(), assignment.getColumn().toString())) {
                continue;
            }
            if (found) {
                throw new SQLFeatureNotSupportedException("column has duplicate set values: '" + assignment.getColumn().toString() + "'");
            }
            found = true;
            VtPlanValue pv = extractValueFromUpdate(assignment);
            vindexValueMap.put(tindexCol.getColumnName(), pv);
        }
        if (vindexValueMap.size() != 0) {
            // Vindex not changing, continue
            throw new SQLFeatureNotSupportedException("unsupported: You can't update primary tindex columns. Invalid update on tindex: " + tindexCol.getColumnName());
        }
    }

    /**
     * buildChangedVindexesValues adds to the plan all the lookup vindexes that are changing.
     * Updates can only be performed to secondary lookup vindexes with no complex expressions
     * in the set clause.
     * <p>
     * planbuilder/update.go --> buildChangedVindexesValues
     */
    public static void buildChangedVindexesValues(SQLUpdateStatement update, Vschema.Table table, String ksidCol) throws SQLException {
        for (Vschema.ColumnVindex vindex : table.getColumnVindexesList()) {
            Map<String, VtPlanValue> vindexValueMap = new HashMap<>();
            boolean first = true;
            List<String> columns = new ArrayList<>();
            if (vindex.getColumnsCount() > 0) {
                columns.addAll(vindex.getColumnsList());
            } else {
                columns.add(vindex.getColumn());
            }
            for (String vcol : columns) {
                // Searching in order of columns in colvindex.
                boolean found = false;
                for (SQLUpdateSetItem assignment : update.getItems()) {
                    if (!SQLUtils.nameEquals(vcol, assignment.getColumn().toString())) {
                        continue;
                    }
                    if (found) {
                        throw new SQLFeatureNotSupportedException("column has duplicate set values: '" + assignment.getColumn().toString() + "'");
                    }
                    found = true;
                    VtPlanValue pv = extractValueFromUpdate(assignment);
                    vindexValueMap.put(vcol, pv);
                    if (first) {
                        first = false;
                    }
                }
            }
            if (vindexValueMap.size() != 0) {
                // Vindex not changing, continue
                throw new SQLFeatureNotSupportedException("unsupported: You can't update primary vindex columns. Invalid update on vindex: " + columns.get(0));
            }
        }
    }

    @Override
    public Integer order() {
        return this.order;
    }

    @Override
    public List<ResultColumn> resultColumns() {
        return null;
    }

    @Override
    public void reorder(Integer order) {

    }

    @Override
    public Builder first() {
        return this;
    }

    @Override
    public void makeDistinct() {

    }

    @Override
    public void setUpperLimit(SQLExpr count) {

    }

    @Override
    public void pushMisc(MySqlSelectQueryBlock sel) {

    }

    @Override
    public void wireup(Builder bldr, Jointab jt) {

    }

    @Override
    public void supplyVar(Integer from, Integer to, SQLName colName, String varName) throws SQLException {

    }

    @Override
    public SupplyColResponse supplyCol(SQLName col) {
        return null;
    }

    @Override
    public Integer supplyWeightString(Integer colNumber) {
        return null;
    }

    @Override
    public void pushLock(String lock) throws SQLException {

    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() {
        return eUpdate;
    }

    @Override
    public boolean isSplitTablePlan() {
        return false;
    }
}
