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

import com.jd.jdbc.planbuilder.RoutePlan;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntegerExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import java.util.List;


public class VtRouteWireupFixUpAstVisitor extends MySqlASTVisitorAdapter {
    private final RoutePlan rb;

    public VtRouteWireupFixUpAstVisitor(final RoutePlan rb) {
        this.rb = rb;
    }

    @Override
    public boolean visit(final MySqlSelectQueryBlock x) {
        List<SQLSelectItem> selectList = x.getSelectList();
        if (selectList == null || selectList.isEmpty()) {
            x.addSelectItem(new SQLIntegerExpr(1));
        }
        return true;
    }

    @Override
    public boolean visit(final SQLBinaryOpExpr x) {
        SQLExpr leftValue = x.getLeft();
        SQLExpr rightValue = x.getRight();
        if (SQLBinaryOperator.Equality.equals(x.getOperator())) {
            if (this.rb.exprIsValue(leftValue) && !this.rb.exprIsValue(rightValue)) {
                x.setRight(leftValue);
                x.setLeft(rightValue);
            }
        }
        return true;
    }
}
