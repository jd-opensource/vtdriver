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
import com.jd.jdbc.engine.DeleteEngine;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.table.TableDMLEngine;
import com.jd.jdbc.engine.table.TableDeleteEngine;
import com.jd.jdbc.planbuilder.tableplan.TableDMLPlan;
import com.jd.jdbc.planbuilder.tableplan.TableRoutePlan;
import com.jd.jdbc.sqlparser.SqlParser;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.statement.SQLDeleteStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.SwitchTableVisitor;
import com.jd.jdbc.sqlparser.utils.TableNameUtils;
import com.jd.jdbc.tindexes.LogicTable;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Setter;

public class DeletePlan implements Builder {
    @Setter
    private DeleteEngine eDelete;

    private Integer order;

    public static PrimitiveEngine newBuildDeletePlan(SQLDeleteStatement stmt, VSchemaManager vm, String defaultKeyspace) throws SQLException {
        SQLTableSource from = stmt.getFrom();
        if (from == null) {
            from = stmt.getTableSource();
        }

        PrimitiveBuilder pb = new PrimitiveBuilder(vm, defaultKeyspace, Jointab.newJointab(SqlParser.getBindVars(stmt)));
        AbstractRoutePlan rb = pb.processDmlTable(from);

        if (rb instanceof TableRoutePlan) {
            return buildTableDeletePlan(stmt, vm, defaultKeyspace, pb, (TableRoutePlan) rb);
        }

        return buildDeletePlan(stmt, vm, defaultKeyspace, pb, (RoutePlan) rb);
    }

    private static TableDeleteEngine buildTableDeletePlan(SQLDeleteStatement stmt, VSchemaManager vm, String defaultKeyspace, PrimitiveBuilder pb, TableRoutePlan rb) throws SQLException {
        TableDMLEngine tableDMLEngine = TableDMLPlan.buildTableDMLPlan(pb, rb, vm.getKeyspace(defaultKeyspace), DMLPlan.DMLType.DELETE, stmt, stmt.getWhere(), defaultKeyspace);
        if (!(tableDMLEngine instanceof TableDeleteEngine)) {
            throw new SQLException("dml engine is not delete");
        }
        TableDeleteEngine tableDeleteEngine = (TableDeleteEngine) tableDMLEngine;
        List<LogicTable> ltbs = tableDeleteEngine.getLogicTables();
        Map<String, String> switchTable = new HashMap<>();
        for (LogicTable ltb : ltbs) {
            switchTable.put(ltb.getLogicTable(), ltb.getFirstActualTableName());
        }
        SwitchTableVisitor visitor = new SwitchTableVisitor(switchTable);
        SQLDeleteStatement deleteclone = (SQLDeleteStatement) tableDeleteEngine.getQuery();
        deleteclone.accept(visitor);
        tableDeleteEngine.setExecuteEngine(newBuildDeletePlan(stmt, vm, defaultKeyspace));

        return tableDeleteEngine;
    }

    private static DeleteEngine buildDeletePlan(SQLDeleteStatement stmt, VSchemaManager vm, String defaultKeyspace, PrimitiveBuilder pb, RoutePlan rb) throws SQLException {
        DMLPlan.DMLPlanResult dmlPlanResult = DMLPlan.buildDMLPlan(pb, rb, vm.getKeyspace(defaultKeyspace), DMLPlan.DMLType.DELETE, stmt,
            stmt.getWhere(), stmt.getChildren(), defaultKeyspace);
        DeleteEngine edel;
        if (!(dmlPlanResult.dmlEngine instanceof DeleteEngine)) {
            throw new SQLException("expect delete engine");
        }
        edel = (DeleteEngine) dmlPlanResult.dmlEngine;
        if (edel.getOpcode() == DMLOpcode.Unsharded) {
            return edel;
        }
        if (stmt.getTableSource() instanceof SQLExprTableSource) {
            SQLExprTableSource target = (SQLExprTableSource) stmt.getTableSource();
            if (!TableNameUtils.getTableSimpleName(target).equalsIgnoreCase(edel.getTableName())) {
                throw new SQLException("Unknown table '" + TableNameUtils.getTableSimpleName(target) + "' in MULTI DELETE");
            }
        } else {
            throw new SQLFeatureNotSupportedException("unsupported: multi-table delete statement in sharded keyspace");
        }
        return edel;
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
    public void pushFilter(PrimitiveBuilder pb, SQLExpr filter, String whereType, Builder origin) {

    }

    @Override
    public void makeDistinct() {

    }

    @Override
    public void pushGroupBy(SQLSelectGroupByClause groupBy) {

    }

    @Override
    public Builder pushOrderBy(SQLOrderBy orderBy) throws SQLException {
        return null;
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
        return eDelete;
    }

    @Override
    public boolean isSplitTablePlan() {
        return false;
    }
}
