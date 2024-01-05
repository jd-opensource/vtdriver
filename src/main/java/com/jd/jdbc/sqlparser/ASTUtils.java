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

package com.jd.jdbc.sqlparser;

import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator;
import com.jd.jdbc.sqlparser.ast.expr.SQLExprUtils;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;

public class ASTUtils {
    private ASTUtils() {
    }

    public static SQLObject getOrderByGroupByParent(SQLObject cursor) {
        SQLObject result = cursor;
        if (result instanceof SQLSelectQuery) {
            return null;
        }
        if (result != null && !(result instanceof SQLOrderBy || result instanceof SQLSelectGroupByClause)) {
            result = getOrderByGroupByParent(result.getParent());
        }
        return result;
    }

    public static SQLOrderBy getOrderByParent(SQLObject cursor) {
        SQLObject result = cursor;
        if (result instanceof SQLSelectQuery || result instanceof SQLSelectGroupByClause) {
            return null;
        }
        if (result != null && !(result instanceof SQLOrderBy)) {
            result = getOrderByParent(result.getParent());
        }
        return result != null ? (SQLOrderBy) result : null;
    }

    public static SQLSelectGroupByClause getGroupByParent(SQLObject cursor) {
        SQLObject result = cursor;
        if (result instanceof SQLSelectQuery || result instanceof SQLOrderBy) {
            return null;
        }
        if (result != null && !(result instanceof SQLSelectGroupByClause)) {
            result = getGroupByParent(result.getParent());
        }
        return result != null ? (SQLSelectGroupByClause) result : null;
    }


    /**
     * AndExpressions ands together two or more expressions, minimising the expr when possible
     */
    public static SQLExpr andExpressions(SQLExpr... exprs) {
        switch (exprs.length) {
            case 0:
                return null;
            case 1:
                return exprs[0];
            default:
                SQLExpr result = null;
                // we'll loop and remove any duplicates
                for (int i = 0; i < exprs.length; i++) {
                    SQLExpr expr = exprs[i];
                    if (expr == null) {
                        continue;
                    }
                    if (result == null) {
                        result = expr;
                        continue;
                    }
                    boolean continueFlag = false;
                    for (int j = 0; j < i; j++) {
                        if (SQLExprUtils.equals(expr, exprs[j])) {
                            continueFlag = true;
                            continue;
                        }
                    }
                    if (continueFlag) {
                        continue;
                    }
                    result = new SQLBinaryOpExpr(result, SQLBinaryOperator.BooleanAnd, expr);
                }
                return result;
        }
    }
}
