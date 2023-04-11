/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jd.jdbc.sqlparser.dialect.mysql.visitor;

import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.SqlParser;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;


public class VtRemoveBacktickVisitor extends MySqlASTVisitorAdapter {

    @Override
    public boolean visit(final SQLPropertyExpr x) {
        SQLExpr owner = x.getOwner();
        if (owner instanceof SQLIdentifierExpr) {
            visit((SQLIdentifierExpr) owner);
        } else if (owner instanceof SQLPropertyExpr) {
            visit((SQLPropertyExpr) owner);
        }

        String originalName = x.getName();
        String normalizedName = SQLUtils.normalize(x.getName());

        if (originalName.equalsIgnoreCase(normalizedName)) {
            return false;
        }

        if (!SqlParser.MYSQL_KEYWORDS.contains(normalizedName.toUpperCase())) {
            x.setName(normalizedName);
        }
        return false;
    }

    @Override
    public boolean visit(final SQLIdentifierExpr x) {
        String originalName = x.getName();
        String normalizedName = x.normalizedName();

        if (originalName.equalsIgnoreCase(normalizedName)) {
            return false;
        }

        if (!SqlParser.MYSQL_KEYWORDS.contains(normalizedName.toUpperCase())) {
            x.setName(normalizedName);
        }
        return false;
    }
}
