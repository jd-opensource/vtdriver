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

package com.jd.jdbc.sqlparser.dialect.mysql.visitor;

import com.jd.jdbc.planbuilder.gen4.QueryProjection;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.expr.SQLAggregateExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLExprUtils;
import com.jd.jdbc.sqlparser.ast.expr.VtOffset;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import java.sql.SQLException;

public class RewriteHavingAggrWithOffsetVisitor extends MySqlASTVisitorAdapter {

    private final QueryProjection queryProjection;

    private SQLException err;

    public RewriteHavingAggrWithOffsetVisitor(QueryProjection qp) {
        this.queryProjection = qp;
    }

    @Override
    public boolean visit(final SQLAggregateExpr x) {
        if (err != null) {
            return false;
        }
        for (int offset = 0; offset < queryProjection.getSelectExprs().size(); offset++) {
            QueryProjection.SelectExpr expr = queryProjection.getSelectExprs().get(offset);
            SQLSelectItem ae;
            try {
                ae = expr.getAliasedExpr();
            } catch (SQLException exception) {
                this.err = exception;
                return false;
            }
            if (SQLExprUtils.equals(ae.getExpr(), x)) {
                VtOffset vtOffset = new VtOffset(offset, x.toString());
                SQLUtils.replaceInParent(x, vtOffset);
                return false;
            }
        }

        QueryProjection.SelectExpr col = new QueryProjection.SelectExpr(true, new SQLSelectItem(x.clone()));
        queryProjection.setHasAggr(true);

        VtOffset vtOffset = new VtOffset(queryProjection.getSelectExprs().size(), x.toString());
        SQLUtils.replaceInParent(x, vtOffset);
        queryProjection.getSelectExprs().add(col);
        queryProjection.setAddedColumn(queryProjection.getAddedColumn() + 1);
        return true;
    }
}
