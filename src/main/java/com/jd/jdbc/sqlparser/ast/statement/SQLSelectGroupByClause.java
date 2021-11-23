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

import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLObjectImpl;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class SQLSelectGroupByClause extends SQLObjectImpl {

    private final List<SQLExpr> items = new ArrayList<SQLExpr>();
    private SQLExpr having;
    private boolean withRollUp = false;
    private boolean withCube = false;

    public SQLSelectGroupByClause() {

    }

    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, this.items);
            acceptChild(visitor, this.having);
        }

        visitor.endVisit(this);
    }

    public boolean isWithRollUp() {
        return withRollUp;
    }

    public void setWithRollUp(boolean withRollUp) {
        this.withRollUp = withRollUp;
    }


    public boolean isWithCube() {
        return withCube;
    }

    public void setWithCube(boolean withCube) {
        this.withCube = withCube;
    }

    public SQLExpr getHaving() {
        return this.having;
    }

    public void setHaving(SQLExpr having) {
        if (having != null) {
            having.setParent(this);
        }

        this.having = having;
    }

    public void addHaving(SQLExpr expr) {
        if (this.having == null) {
            this.setHaving(expr);
        } else {
            this.having = SQLBinaryOpExpr.and(this.having, expr);
        }
    }

    public List<SQLExpr> getItems() {
        return this.items;
    }

    public void addItem(SQLExpr sqlExpr) {
        if (sqlExpr != null) {
            sqlExpr.setParent(this);
            this.items.add(sqlExpr);
        }
    }

    public SQLSelectGroupByClause clone() {
        SQLSelectGroupByClause x = new SQLSelectGroupByClause();
        for (SQLExpr item : items) {
            SQLExpr item2 = item.clone();
            item2.setParent(x);
            x.items.add(item2);
        }
        if (having != null) {
            x.setHaving(having.clone());
        }
        x.withRollUp = withRollUp;
        x.withCube = withCube;
        return x;
    }
}
