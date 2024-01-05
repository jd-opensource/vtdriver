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

package com.jd.jdbc.sqlparser.visitor;

import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectOrderByItem;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.MySqlASTVisitorAdapter;

public class OrderByGroupByNumberRewriteVisitor extends MySqlASTVisitorAdapter {
    private final SQLObject cursor;

    private final SQLExpr newNode;

    public OrderByGroupByNumberRewriteVisitor(SQLObject cursor, SQLExpr newNode) {
        this.cursor = cursor;
        this.newNode = newNode;
    }

    @Override
    public boolean visit(SQLSelectGroupByClause groupByClause) {
        int num = -1;
        for (int i = 0; i < groupByClause.getItems().size(); i++) {
            if (groupByClause.getItems().get(i) == cursor) {
                num = i;
                break;
            }
        }
        if (num != -1) {
            groupByClause.getItems().set(num, newNode);
            return false;
        }
        return true;
    }

    @Override
    public boolean visit(SQLOrderBy orderBy) {
        int num = -1;
        boolean collateFlag = false;
        for (int i = 0; i < orderBy.getItems().size(); i++) {
            if (orderBy.getItems().get(i).getExpr() == cursor) {
                num = i;
                break;
            }
            // support order by id collate utf8_general_ci rewrite
            if (orderBy.getItems().get(i).getExpr() instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) orderBy.getItems().get(i).getExpr();
                if (binaryOpExpr.getOperator() == SQLBinaryOperator.COLLATE) {
                    if (binaryOpExpr.getLeft() == cursor) {
                        num = i;
                        collateFlag = true;
                        break;
                    }
                }
            }
        }
        if (num != -1) {
            SQLSelectOrderByItem sqlSelectOrderByItem = orderBy.getItems().get(num);
            if (!collateFlag) {
                sqlSelectOrderByItem.setExpr(newNode);
            } else {
                SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) sqlSelectOrderByItem.getExpr();
                binaryOpExpr.setLeft(newNode);
            }
            return false;
        }
        return true;
    }
}
