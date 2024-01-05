/*
Copyright 2021 JD Project Authors.

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

package com.jd.jdbc.sqlparser.utils;

import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLSubqueryTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import io.netty.util.internal.StringUtil;
import java.sql.SQLException;

public class TableNameUtils {

    public static String getTableSimpleName(SQLExprTableSource tableSource) throws SQLException {
        SQLName tableName = tableSource.getName();
        if (tableName == null || StringUtil.isNullOrEmpty(tableName.getSimpleName())) {
            throw new SQLException("Table name is not found");
        }
        return tableName.getSimpleName();
    }

    public static String getQualifier(SQLTableSource tableSource) {
        if (tableSource instanceof SQLExprTableSource) {
            SQLExpr tableSourceExpr = ((SQLExprTableSource) tableSource).getExpr();
            if (tableSourceExpr instanceof SQLPropertyExpr) {
                return ((SQLPropertyExpr) tableSourceExpr).getOwnernName();
            }
            return tableSource.getAlias();
        }
        if (tableSource instanceof SQLSubqueryTableSource) {
            return tableSource.getAlias();
        }
        return "";
    }

    public static String getAlias(SQLTableSource tableSource) {
        if (tableSource instanceof SQLExprTableSource) {
            return tableSource.getAlias();
        }
        return tableSource.getAlias();
    }

    public static String getDatabaseName(SQLExprTableSource tableSource) {
        SQLExpr tableExpr = tableSource.getExpr();
        if (!(tableExpr instanceof SQLPropertyExpr)) {
            return null;
        }

        SQLExpr schemaExpr = ((SQLPropertyExpr) tableExpr).getOwner();
        if (schemaExpr instanceof SQLIdentifierExpr) {
            String schemaName = ((SQLIdentifierExpr) schemaExpr).getSimpleName();
//            if (RoutePlan.systemTable(schemaName)) {
//                return null;
//            }
            return schemaName;
        }
        return null;
    }
}