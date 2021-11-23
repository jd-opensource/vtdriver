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

package com.jd.jdbc.planbuilder.tableplan;

import com.google.common.collect.Lists;
import com.jd.jdbc.engine.DMLOpcode;
import com.jd.jdbc.engine.table.TableDMLEngine;
import com.jd.jdbc.engine.table.TableDeleteEngine;
import com.jd.jdbc.engine.table.TableUpdateEngine;
import com.jd.jdbc.planbuilder.DMLPlan;
import com.jd.jdbc.planbuilder.PlanBuilder;
import com.jd.jdbc.planbuilder.PrimitiveBuilder;
import com.jd.jdbc.planbuilder.Symtab;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRemoveDbNameVisitor;
import com.jd.jdbc.sqltypes.VtPlanValue;
import com.jd.jdbc.tindexes.Column;
import com.jd.jdbc.tindexes.LogicTable;
import com.jd.jdbc.vitess.VitessDataSource;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import vschema.Vschema;

public class TableDMLPlan {
    /**
     * @param pb
     * @param rb
     * @param keyspace
     * @param dmlType
     * @param stmt
     * @param where
     * @param defaultKeyspace
     * @return
     * @throws SQLException
     */
    public static TableDMLEngine buildTableDMLPlan(final PrimitiveBuilder pb, final TableRoutePlan rb, final Vschema.Keyspace keyspace, final DMLPlan.DMLType dmlType, final SQLStatement stmt,
                                                   final SQLExpr where, final String defaultKeyspace)
        throws SQLException {
        TableDMLEngine tedml;
        switch (dmlType) {
            case UPDATE:
                tedml = new TableUpdateEngine();
                break;
            case DELETE:
                tedml = new TableDeleteEngine();
                break;
            default:
                throw new RuntimeException("expect update or delete");
        }
        tedml.setKeyspace(rb.getTableRouteEngine().getKeyspace());

        if (PlanBuilder.hasSubquery(stmt)) {
            throw new SQLFeatureNotSupportedException("unsupported: subqueries in sharded DML");
        }

        // Generate query after all the analysis. Otherwise table name substitutions for
        // routed tables won't happen.
        tedml.setSQLStatement(dmlFormat(stmt));

        if (pb.getSymtab().getTables().size() != 1) {
            throw new SQLFeatureNotSupportedException("unsupported: multi-table " + dmlType + " statement in sharded keyspace");
        }

        for (Map.Entry<String, Symtab.Table> entry : pb.getSymtab().getTables().entrySet()) {
            // There is only one table.
            String tableName = rb.getTableRouteEngine().getTableName();
            tedml.setTableName(tableName);
            tedml.setTable(entry.getValue().getVschemaTable());
            LogicTable ltb = VitessDataSource.getLogicTable(defaultKeyspace, tableName);
            if (ltb == null) {
                throw new SQLException("cannot find logic table:" + tableName);
            }
            tedml.addLogicTable(ltb);
        }

        TableDMLPlan.TableDMLRoutingResult routingResult = getTbleDMLRouting(where, tedml.getLogicTables().get(0), keyspace);

        tedml.setOpcode(routingResult.getRoutingType());
        tedml.setVtPlanValueList(routingResult.getValues());
        return tedml;
    }

    /**
     * getTbleDMLRouting returns the tindex and values for the TableDML,
     * If it cannot find a unique vindex match, it returns an error.
     *
     * @param where
     * @param logicTable
     * @param keyspace
     * @return
     * @throws SQLException
     */
    private static TableDMLRoutingResult getTbleDMLRouting(final SQLExpr where, final LogicTable logicTable, final Vschema.Keyspace keyspace) throws SQLException {
        Column tindexColumn = logicTable.getTindexCol();

        if (null == where) {
            return new TableDMLRoutingResult(DMLOpcode.Scatter, null);
        }

        PlanBuilder.MatchResult matchResult = PlanBuilder.getMatch(where, tindexColumn.getColumnName());
        if (matchResult.isOk()) {
            DMLOpcode opcode = DMLOpcode.Equal;
            if (matchResult.getPv().isList()) {
                opcode = DMLOpcode.In;
            }
            return new TableDMLRoutingResult(opcode, Lists.newArrayList(matchResult.getPv()));
        }

        return new TableDMLRoutingResult(DMLOpcode.Scatter, null);
    }

    private static SQLStatement dmlFormat(final SQLStatement statement) {
        VtRemoveDbNameVisitor visitor = new VtRemoveDbNameVisitor();
        statement.accept(visitor);
        return statement;
    }

    @AllArgsConstructor
    @Getter
    private static class TableDMLRoutingResult {
        private final DMLOpcode routingType;

        private final List<VtPlanValue> values;
    }
}
