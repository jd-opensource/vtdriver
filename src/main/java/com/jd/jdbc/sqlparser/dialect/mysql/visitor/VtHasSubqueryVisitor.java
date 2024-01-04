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

import com.jd.jdbc.sqlparser.ast.expr.SQLInSubQueryExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLQueryExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelect;
import com.jd.jdbc.sqlparser.ast.statement.SQLSubqueryTableSource;
import lombok.Getter;


public class VtHasSubqueryVisitor extends MySqlASTVisitorAdapter {
    @Getter
    private Boolean hasSubquery = Boolean.FALSE;

    @Override
    public boolean visit(final SQLSubqueryTableSource x) {
        this.hasSubquery = Boolean.TRUE;
        return false;
    }

    @Override
    public boolean visit(final SQLQueryExpr x) {
        this.hasSubquery = Boolean.TRUE;
        return false;
    }

    @Override
    public boolean visit(final SQLSelect x) {
        this.hasSubquery = Boolean.TRUE;
        return false;
    }

    @Override
    public boolean visit(final SQLInSubQueryExpr x) {
        this.hasSubquery = Boolean.TRUE;
        return false;
    }
}
