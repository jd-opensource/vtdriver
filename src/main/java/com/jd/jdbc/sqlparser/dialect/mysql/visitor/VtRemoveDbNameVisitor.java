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
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VtRemoveDbNameVisitor extends MySqlASTVisitorAdapter {
    private final Map<String, List<SQLTableSource>> keySpaceName;

    private final Map<SQLExprTableSource, String> tableKeyspaceMap;

    public VtRemoveDbNameVisitor() {
        this.keySpaceName = new HashMap<>();
        this.tableKeyspaceMap = new HashMap<>();
    }

    @Override
    public boolean visit(final SQLExprTableSource x) {
        SQLExpr tableExpr = x.getExpr();
        if (tableExpr instanceof SQLPropertyExpr) {
            String tableName = ((SQLPropertyExpr) tableExpr).getName();
            String keySpace = ((SQLPropertyExpr) tableExpr).getOwnernName();
            x.setExpr(tableName);
            if (!this.keySpaceName.containsKey(keySpace)) {
                this.keySpaceName.put(keySpace, new ArrayList<>());
            }
            this.keySpaceName.get(keySpace).add(x);
            this.tableKeyspaceMap.put(x, keySpace);
        }
        return false;
    }
}
