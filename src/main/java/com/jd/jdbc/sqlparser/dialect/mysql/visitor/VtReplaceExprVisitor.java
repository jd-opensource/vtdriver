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
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLAggregateExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLAllColumnExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLAllExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLAnyExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLArrayExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBetweenExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExprGroup;
import com.jd.jdbc.sqlparser.ast.expr.SQLBooleanExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLCaseExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLCastExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLCharExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLContainsExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLCurrentOfCursorExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLDateExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLDefaultExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLExistsExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLFlashbackExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLGroupingSetExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLHexExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLInListExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLInSubQueryExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntegerExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntervalExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLListExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLLiteralExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLMethodInvokeExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNCharExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNotExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNullExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNumberExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLQueryExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLRealExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLSequenceExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLSomeExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLTimestampExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLUnaryExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLValuesExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelect;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.expr.MySqlCharExpr;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.expr.MySqlExtractExpr;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.expr.MySqlMatchAgainstExpr;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.expr.MySqlOrderingExpr;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.expr.MySqlOutFileExpr;


public class VtReplaceExprVisitor extends MySqlASTVisitorAdapter {
    private final SQLSelect from;

    private final SQLExpr to;

    public VtReplaceExprVisitor(final SQLSelect from, final SQLExpr to) {
        this.from = from;
        this.to = to;
    }

    private boolean replacecExpr(final SQLExpr x) {
        if (x instanceof SQLQueryExpr) {
            if (((SQLQueryExpr) x).getSubQuery() == this.from) {
                SQLUtils.replaceInParent(x, this.to);
            }
        }

        return !(x instanceof SQLExistsExpr)
            && !(x instanceof SQLLiteralExpr)
            && !(x instanceof SQLQueryExpr)
            && !(x instanceof SQLMethodInvokeExpr);
    }

    @Override
    public boolean visit(final MySqlCharExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final MySqlExtractExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final MySqlMatchAgainstExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final MySqlOrderingExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final MySqlOutFileExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLAggregateExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLAllColumnExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLAllExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLAnyExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLArrayExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLBetweenExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLBinaryExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLBinaryOpExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLBinaryOpExprGroup x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLBooleanExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLCaseExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLCastExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLCharExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLContainsExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLCurrentOfCursorExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLDateExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLDefaultExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLExistsExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLFlashbackExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLGroupingSetExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLHexExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLIdentifierExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLInListExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLInSubQueryExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLIntegerExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLIntervalExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLListExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLMethodInvokeExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLNCharExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLNotExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLNullExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLNumberExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLPropertyExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLQueryExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLRealExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLSequenceExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLSomeExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLTimestampExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLUnaryExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLValuesExpr x) {
        return this.replacecExpr(x);
    }

    @Override
    public boolean visit(final SQLVariantRefExpr x) {
        return this.replacecExpr(x);
    }
}
