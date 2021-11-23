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

package com.jd.jdbc.sqlparser.ast.statement;

import com.jd.jdbc.sqlparser.ast.SQLHint;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.expr.SQLListExpr;
import com.jd.jdbc.sqlparser.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class SQLValuesTableSource extends SQLTableSourceImpl {
    private List<SQLListExpr> values = new ArrayList<>();
    private List<SQLName> columns = new ArrayList<>();

    public List<SQLListExpr> getValues() {
        return values;
    }

    public List<SQLName> getColumns() {
        return columns;
    }

    @Override
    public void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, values);
            acceptChild(visitor, columns);
        }
        visitor.endVisit(this);
    }

    @Override
    public SQLValuesTableSource clone() {

        SQLValuesTableSource x = new SQLValuesTableSource();

        x.setAlias(this.alias);

        for (SQLListExpr e : this.values) {
            SQLListExpr e2 = e.clone();
            e2.setParent(x);
            x.getValues().add(e2);
        }

        for (SQLName e : this.columns) {
            SQLName e2 = e.clone();
            e2.setParent(x);
            x.getColumns().add(e2);
        }

        if (this.flashback != null) {
            x.setFlashback(this.flashback.clone());
        }

        if (this.hints != null) {
            for (SQLHint e : this.hints) {
                SQLHint e2 = e.clone();
                e2.setParent(x);
                x.getHints().add(e2);
            }
        }

        return x;
    }
}
