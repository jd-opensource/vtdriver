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

package com.jd.jdbc.planbuilder.semantics;

import com.jd.jdbc.sqlparser.SqlParser;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Vtable {

    public static VTableInfo createVTableInfoForExpressions(List<SQLSelectItem> expressions, List<TableInfo> tables, Originable org) throws SQLException {
        return selectExprsToInfos(expressions, tables, org);
    }

    private static VTableInfo selectExprsToInfos(List<SQLSelectItem> expressions, List<TableInfo> tables, Originable org) throws SQLException {
        List<SQLExpr> cols = new ArrayList<>(expressions.size());
        List<String> columnNames = new ArrayList<>(expressions.size());
        TableSet ts = new TableSet();

        for (SQLSelectItem selectItem : expressions) {
            SQLExpr expr = selectItem.getExpr();
            cols.add(expr);
            if (StringUtils.isNotEmpty(selectItem.getAlias())) {
                columnNames.add(selectItem.getAlias());
                continue;
            }

            SqlParser.SelectExpr selectExpr = SqlParser.SelectExpr.type(selectItem);
            if (SqlParser.SelectExpr.AliasedExpr.equals(selectExpr)) {
                if (expr instanceof SQLName) {
                    columnNames.add(((SQLName) expr).getSimpleName());
                } else {
                    columnNames.add(expr.toString());
                }
            } else if (SqlParser.SelectExpr.StarExpr.equals(selectExpr)) {
                for (TableInfo table : tables) {
                    ts.mergeInPlace(table.getTableSet(org));
                }
            } else {
                throw new RuntimeException();
            }
        }
        return new VTableInfo(columnNames, cols, ts);
    }
}
