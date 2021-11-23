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

import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import java.util.Map;

public class SwitchTableVisitor extends MySqlASTVisitorAdapter {
    private final Map<String, String> switchTables;

    public SwitchTableVisitor(final Map<String, String> switchTables) {
        this.switchTables = switchTables;
    }

    @Override
    public boolean visit(final SQLExprTableSource x) {
        String originTableName = x.getName().getSimpleName().toLowerCase();
        if (switchTables.containsKey(originTableName)) {
            if (x.getExpr() instanceof SQLPropertyExpr) {
                ((SQLPropertyExpr) x.getExpr()).setName(switchTables.get(originTableName));
            } else {
                x.setExpr(switchTables.get(originTableName));
            }
        }

        return false;
    }

    @Override
    public boolean visit(final SQLPropertyExpr x) {
        SQLExpr owner = x.getOwner();
        if (owner instanceof SQLIdentifierExpr) {
            String ownerName = ((SQLIdentifierExpr) owner).getName().toLowerCase();
            if (switchTables.containsKey(ownerName)) {
                ((SQLIdentifierExpr) owner).setName(switchTables.get(ownerName));
            }
        } else if (owner instanceof SQLPropertyExpr) {
            owner.accept(this);
        }
        return false;
    }
}
