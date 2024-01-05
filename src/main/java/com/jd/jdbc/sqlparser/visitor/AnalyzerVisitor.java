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

import com.jd.jdbc.planbuilder.semantics.Analyzer;
import com.jd.jdbc.sqlparser.ASTUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.expr.SQLAggregateExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLCharExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntegerExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNumberExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLJoinTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectOrderByItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSubqueryTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQuery;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.MySqlASTVisitorAdapter;

public class AnalyzerVisitor extends MySqlASTVisitorAdapter {

    private final Analyzer analyzer;

    public AnalyzerVisitor(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    @Override
    public boolean visit(MySqlSelectQueryBlock x) {
        analyzer.analyzeDown(x);
        SQLTableSource from = x.getFrom();
        if (from == null) {
            from = new SQLExprTableSource(new SQLIdentifierExpr("dual"));
        }
        from.accept(this);
        x.getAfterCommentsDirect();
        for (SQLSelectItem sqlSelectItem : x.getSelectList()) {
            sqlSelectItem.accept(this);
        }
        if (x.getWhere() != null) {
            x.getWhere().accept(this);
        }
        if (x.getGroupBy() != null) {
            x.getGroupBy().accept(this);
            if (x.getGroupBy().getHaving() != null) {
                x.getGroupBy().getHaving().accept(this);
            }
        }
        if (x.getOrderBy() != null) {
            x.getOrderBy().accept(this);
        }
        return false;
    }

    @Override
    public void endVisit(SQLUnionQuery x) {
        analyzer.analyzeUp(x);
    }

    @Override
    public boolean visit(SQLUnionQuery x) {
        analyzer.analyzeDown(x);
        return true;
    }

    @Override
    public void endVisit(MySqlSelectQueryBlock x) {
        analyzer.analyzeUp(x);
    }

    @Override
    public boolean visit(SQLExprTableSource x) {
        analyzer.analyzeDown(x);
        return false;
    }

    @Override
    public void endVisit(SQLExprTableSource x) {
        analyzer.analyzeUp(x);
    }

    @Override
    public boolean visit(SQLJoinTableSource x) {
        analyzer.analyzeDown(x);
        return true;
    }

    @Override
    public void endVisit(SQLJoinTableSource x) {
        analyzer.analyzeUp(x);
    }

    @Override
    public boolean visit(SQLSubqueryTableSource x) {
        analyzer.analyzeDown(x);
        x.getSelect().accept(this);
        return false;
    }

    @Override
    public void endVisit(SQLSubqueryTableSource x) {
        analyzer.analyzeUp(x);
    }

    @Override
    public boolean visit(SQLSelectItem x) {
        analyzer.analyzeDown(x);
        return true;
    }

    @Override
    public void endVisit(SQLSelectItem x) {
        analyzer.analyzeUp(x);
    }

    @Override
    public boolean visit(SQLSelectGroupByClause x) {
        analyzer.analyzeDown(x);
        for (SQLExpr expr : x.getItems()) {
            expr.accept(this);
        }
        return false;
    }

    @Override
    public void endVisit(SQLSelectGroupByClause x) {
        analyzer.analyzeUp(x);
    }

    @Override
    public boolean visit(SQLOrderBy x) {
        analyzer.analyzeDown(x);
        return true;
    }

    @Override
    public void endVisit(SQLOrderBy x) {
        analyzer.analyzeUp(x);
    }

    @Override
    public boolean visit(SQLIdentifierExpr x) {
        analyzer.analyzeDown(x);
        return false;
    }

    @Override
    public void endVisit(SQLIdentifierExpr x) {
        analyzer.analyzeUp(x);
    }

    @Override
    public boolean visit(SQLPropertyExpr x) {
        analyzer.analyzeDown(x);
        return false;
    }

    @Override
    public void endVisit(SQLPropertyExpr x) {
        analyzer.analyzeUp(x);
    }

    @Override
    public boolean visit(SQLBinaryOpExpr x) {
        if (x.getParent() instanceof SQLSelectGroupByClause) {
            SQLSelectGroupByClause groupBy = (SQLSelectGroupByClause) x.getParent();
            if (groupBy.getHaving() == x) {
                analyzer.analyzeDown(x);
                return true;
            }
        }
        return true;
    }

    @Override
    public void endVisit(SQLBinaryOpExpr x) {
        if (x.getParent() instanceof SQLSelectGroupByClause) {
            SQLSelectGroupByClause groupBy = (SQLSelectGroupByClause) x.getParent();
            if (groupBy.getHaving() == x) {
                analyzer.analyzeUp(x);
            }
        }
    }

    @Override
    public boolean visit(SQLIntegerExpr x) {
        analyzer.analyzeDown(x);
        SQLOrderBy orderByParent = ASTUtils.getOrderByParent(x);
        if (orderByParent != null) {
            for (SQLSelectOrderByItem item : orderByParent.getItems()) {
                if (x == item.getExpr()) {
                    continue;
                }
                analyzer.analyzeDown(item.getExpr());
            }
        }
        SQLSelectGroupByClause groupByClause = ASTUtils.getGroupByParent(x);
        if (groupByClause != null) {
            for (SQLExpr item : groupByClause.getItems()) {
                if (x == item) {
                    continue;
                }
                analyzer.analyzeDown(item);
            }
        }
        return false;
    }

    @Override
    public void endVisit(SQLIntegerExpr x) {
        analyzer.analyzeUp(x);
        SQLOrderBy orderByParent = ASTUtils.getOrderByParent(x);
        if (orderByParent != null) {
            for (SQLSelectOrderByItem item : orderByParent.getItems()) {
                if (x == item.getExpr()) {
                    continue;
                }
                analyzer.analyzeUp(item.getExpr());
            }
        }
        SQLSelectGroupByClause groupByClause = ASTUtils.getGroupByParent(x);
        if (groupByClause != null) {
            for (SQLExpr item : groupByClause.getItems()) {
                if (x == item) {
                    continue;
                }
                analyzer.analyzeUp(item);
            }
        }
    }

    @Override
    public boolean visit(SQLCharExpr x) {
        analyzer.analyzeDown(x);
        return false;
    }

    @Override
    public void endVisit(SQLCharExpr x) {
        analyzer.analyzeUp(x);
    }

    @Override
    public boolean visit(SQLNumberExpr x) {
        analyzer.analyzeDown(x);
        return false;
    }

    @Override
    public void endVisit(SQLNumberExpr x) {
        analyzer.analyzeUp(x);
    }

    @Override
    public boolean visit(SQLAggregateExpr x) {
        analyzer.analyzeDown(x);
        return true;
    }

    @Override
    public void endVisit(SQLAggregateExpr x) {
        analyzer.analyzeUp(x);
    }
}
