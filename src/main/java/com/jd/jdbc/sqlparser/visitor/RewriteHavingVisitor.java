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


import com.google.common.collect.Sets;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLAggregateExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.CheckNodeTypesVisitor;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import java.util.Map;
import lombok.Getter;

public class RewriteHavingVisitor extends MySqlASTVisitorAdapter {
    private Map<String, SQLExpr> selectExprMap;

    @Getter
    private boolean hasAggr;

    public RewriteHavingVisitor(Map<String, SQLExpr> selectExprMap) {
        this.selectExprMap = selectExprMap;
        hasAggr = false;
    }

    @Override
    public boolean visit(SQLIdentifierExpr x) {
        SQLExpr originalExpr = selectExprMap.get(x.getLowerName());
        if (originalExpr != null) {
            CheckNodeTypesVisitor visitor = new CheckNodeTypesVisitor(Sets.newHashSet(CheckNodeTypesVisitor.CheckNodeType.AGGREGATE));
            originalExpr.accept(visitor);
            if (visitor.getCheckResult()) {
                hasAggr = true;
            } else {
                SQLUtils.replaceInParent(x, originalExpr);
            }
        }
        return false;
    }

    @Override
    public boolean visit(final SQLAggregateExpr x) {
        hasAggr = true;
        return true;
    }
}
