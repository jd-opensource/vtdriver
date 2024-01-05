/*
Copyright 2023 JD Project Authors. Licensed under Apache-2.0.

Copyright 2022 The Vitess Authors.

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

package com.jd.jdbc.planbuilder.gen4;

import com.jd.jdbc.context.PlanningContext;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.gen4.RouteGen4Engine;
import com.jd.jdbc.planbuilder.gen4.logical.LogicalPlan;
import com.jd.jdbc.planbuilder.gen4.logical.RouteGen4Plan;
import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.planbuilder.semantics.TableInfo;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRemoveDbNameExpectSystemDbVisitor;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRemoveDbNameInColumnVisitor;
import com.jd.jdbc.vindexes.VKeyspace;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import vschema.Vschema;

public class SingleShardedShortcut {

    public static LogicalPlan unshardedShortcut(PlanningContext ctx, SQLSelectQuery stmt, VKeyspace keyspace) throws SQLException {
        // remove keyspace name
        VtRemoveDbNameExpectSystemDbVisitor visitor = new VtRemoveDbNameExpectSystemDbVisitor();
        stmt.accept(visitor);

        VtRemoveDbNameInColumnVisitor visitor1 = new VtRemoveDbNameInColumnVisitor();
        stmt.accept(visitor1);

        List<String> tableNames = getTableNames(ctx.getSemTable());

        RouteGen4Engine engine = new RouteGen4Engine(Engine.RouteOpcode.SelectUnsharded, keyspace);
        engine.setTableName(String.join(",", tableNames));

        RouteGen4Plan plan = new RouteGen4Plan();
        plan.eroute = engine;
        plan.select = stmt;

        plan.wireupGen4(ctx);
        return plan;
    }

    public static List<String> getTableNames(SemTable semTable) {
        Set<String> tableNameSet = new HashSet<>();
        for (TableInfo tableInfo : semTable.getTables()) {
            Vschema.Table tblObj = tableInfo.getVindexTable();
            if (tblObj == null) {
                // probably a derived table
                continue;
            }

            String name = "";
            if (tableInfo.isInfSchema()) {
                name = "tableName";
            } else {
                SQLTableSource tbl = tableInfo.getExpr();
                if (tbl instanceof SQLExprTableSource) {
                    name = ((SQLExprTableSource) tbl).getName().getSimpleName();
                } else {
                    name = tbl.getAlias();
                }
            }
            tableNameSet.add(name);
        }
        List<String> tableNames = new ArrayList<>(tableNameSet.size());
        for (String name : tableNameSet) {
            tableNames.add(name);
        }
        Collections.sort(tableNames);
        return tableNames;
    }

}
