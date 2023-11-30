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

package com.jd.jdbc.planbuilder.gen4;

import com.jd.jdbc.planbuilder.PlanBuilder;
import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.sqlparser.SqlParser;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqlparser.visitor.RewriteHavingVisitor;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Rewriter {
    private SemTable semTable;

    private Object reservedVars;

    private int inSubquery;

    private String err;

    public Rewriter(SemTable semTable, Object reservedVars) {
        this.semTable = semTable;
        this.reservedVars = reservedVars;
    }

    public static void queryRewrite(SemTable semTable, Object reservedVars, SQLSelectStatement statement) throws SQLException {
        Rewriter rewriter = new Rewriter(semTable, reservedVars);
        if (statement.getSelect().getQuery() instanceof MySqlSelectQueryBlock) {
            rewriter.rewriteHavingClause((MySqlSelectQueryBlock) statement.getSelect().getQuery());
        }
    }

    private void rewriteHavingClause(MySqlSelectQueryBlock query) {
        if (query.getGroupBy() == null) {
            return;
        }
        if (query.getGroupBy() != null && query.getGroupBy().getHaving() == null) {
            return;
        }

        Map<String, SQLExpr> selectExprMap = new HashMap<>(query.getSelectList().size());
        for (SQLSelectItem selectItem : query.getSelectList()) {
            SqlParser.SelectExpr selectExpr = SqlParser.SelectExpr.type(selectItem);
            if (!Objects.equals(SqlParser.SelectExpr.AliasedExpr, selectExpr) || StringUtils.isEmpty(selectItem.getAlias())) {
                continue;
            }
            selectExprMap.put(selectItem.getAlias().toLowerCase(), selectItem.getExpr());
        }

        // for each expression in the having clause, we check if it contains aggregation.
        // if it does, we keep the expression in the having clause ; and if it does not
        // and the expression is in the select list, we replace the expression by the one
        // used in the select list and add it to the where clause instead of the having clause.
        List<SQLExpr> filters = PlanBuilder.splitAndExpression(new ArrayList<>(), query.getGroupBy().getHaving());
        query.getGroupBy().setHaving(null);
        for (SQLExpr expr : filters) {
            RewriteHavingVisitor rewriteHavingVisitor = new RewriteHavingVisitor(selectExprMap);
            expr.accept(rewriteHavingVisitor);

            if (rewriteHavingVisitor.isHasAggr()) {
                query.getGroupBy().addHaving(expr);
            } else {
                query.addWhere(expr);
            }
        }
    }
}
