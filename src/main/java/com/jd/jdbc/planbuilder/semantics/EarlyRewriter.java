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

package com.jd.jdbc.planbuilder.semantics;

import com.jd.jdbc.sqlparser.ASTUtils;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.SqlParser;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntegerExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLLiteralExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLJoinTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqlparser.visitor.OrderByGroupByNumberRewriteVisitor;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;

public class EarlyRewriter {
    private Binder binder;

    private Scoper scoper;

    private String clause;

    @Getter
    private String warning;

    public EarlyRewriter(Scoper scoper, Binder binder) {
        this.binder = binder;
        this.scoper = scoper;
    }

    public void down(SQLObject cursor) throws SQLException {
        if (cursor instanceof SQLSelectGroupByClause) {
            this.clause = "group statement";
            return;
        }
        if (cursor instanceof SQLOrderBy) {
            this.clause = "order clause";
            return;
        }
        if (cursor instanceof SQLLiteralExpr) {
            SQLExpr newNode = rewriteOrderByExpr((SQLLiteralExpr) cursor);
            if (newNode != null) {
                replace(cursor, newNode);
            }
            return;
        }
        if (cursor instanceof SQLSelectItem) {
            SqlParser.SelectExpr selectExpr = SqlParser.SelectExpr.type((SQLSelectItem) cursor);
            if (SqlParser.SelectExpr.StarExpr.equals(selectExpr)) {
                // expandStar
                this.expandStar((SQLSelectItem) cursor);
            }
            return;
        }
        if (cursor instanceof SQLJoinTableSource) {
            // straight join is converted to normal join
            if (((SQLJoinTableSource) cursor).getJoinType() == SQLJoinTableSource.JoinType.STRAIGHT_JOIN) {
                ((SQLJoinTableSource) cursor).setJoinType(SQLJoinTableSource.JoinType.JOIN);
            }
        }
    }

    private void replace(SQLObject cursor, SQLExpr newNode) {
        OrderByGroupByNumberRewriteVisitor rewriteVisitor = new OrderByGroupByNumberRewriteVisitor(cursor, newNode);
        SQLObject parent = ASTUtils.getOrderByGroupByParent(cursor);
        if (parent != null) {
            parent.accept(rewriteVisitor);
        }
    }

    private SQLExpr rewriteOrderByExpr(SQLLiteralExpr node) throws SQLException {
        Scope currScope = this.scoper.getSpecialExprScopes().get(node);
        if (currScope == null) {
            return null;
        }
        int num;
        if (node instanceof SQLIntegerExpr) {
            if (!(((SQLIntegerExpr) node).getNumber() instanceof Integer)) {
                throw new SQLException("error parsing column number: " + node);
            }
            num = ((SQLIntegerExpr) node).getNumber().intValue();
        } else {
            throw new SQLException("error parsing column number: " + node);
        }
        MySqlSelectQueryBlock stmt;
        if (currScope.getStmt() instanceof MySqlSelectQueryBlock) {
            stmt = (MySqlSelectQueryBlock) currScope.getStmt();
        } else {
            throw new SQLException("error invalid statement type, expect Select, got:" + currScope.getStmt().getClass().getSimpleName());
        }
        if (num < 1 || num > stmt.getSelectList().size()) {
            throw new SQLException("Unknown column '" + num + "' in '" + this.clause + "'");
        }
        for (int i = 0; i < num; i++) {
            SQLSelectItem selectItem = stmt.getSelectList().get(i);
            SqlParser.SelectExpr selectExpr = SqlParser.SelectExpr.type(selectItem);
            if (!SqlParser.SelectExpr.AliasedExpr.equals(selectExpr)) {
                throw new SQLException("cannot use column offsets in " + this.clause + " when using `" + selectItem + "`");
            }
        }
        SQLSelectItem aliasedExpr = stmt.getSelectList().get(num - 1);
        if (StringUtils.isNotEmpty(aliasedExpr.getAlias())) {
            return new SQLIdentifierExpr(aliasedExpr.getAlias());
        }
        SQLExpr expr = realCloneOfColNames(aliasedExpr.getExpr(), currScope.isUnion());
        return expr;
    }

    /**
     * realCloneOfColNames clones all the expressions including ColName.
     * Since sqlparser.CloneRefOfColName does not clone col names, this method is needed.
     *
     * @param expr
     * @param union
     * @return
     */
    private SQLExpr realCloneOfColNames(SQLExpr expr, boolean union) {
        if (expr instanceof SQLName) {
            if (union) {
                if (expr instanceof SQLPropertyExpr) {
                    String name = ((SQLPropertyExpr) expr).getName();
                    return new SQLIdentifierExpr(name);
                }
            }
            return expr.clone();
        }
        return expr.clone();
    }

    public static void rewriteJoinUsing(Scope current, List<SQLExpr> usings, Originable org) throws SQLException {
        Map<TableSet, Map<String, TableSet>> joinUsing = current.prepareUsingMap();
        List<SQLExpr> predicates = new ArrayList<>();
        for (SQLExpr column : usings) {
            List<SQLExprTableSource> foundTables = new ArrayList<>();
            String colName = ((SQLName) column).getSimpleName();
            for (TableInfo tbl : current.getTables()) {
                if (!tbl.authoritative()) {
                    throw new SQLException("can't handle JOIN USING without authoritative tables");
                }
                SQLExprTableSource tblName = tbl.name();
                TableSet currTable = tbl.getTableSet(org);
                Map<String, TableSet> usingCols = joinUsing.get(currTable);
                if (usingCols == null) {
                    usingCols = new HashMap<>();
                }
                for (ColumnInfo col : tbl.getColumns()) {
                    TableSet found = usingCols.get(col.getName());
                    if (found != null) {
                        foundTables.add(tblName);
                    }
                }
            }
            for (int i = 0; i < foundTables.size(); i++) {
                SQLExprTableSource lft = foundTables.get(i);
                for (int j = i + 1; j < foundTables.size(); j++) {
                    SQLExprTableSource rgt = foundTables.get(j);
                    SQLExpr left = new SQLPropertyExpr((SQLExpr) lft, colName);
                    SQLExpr right = new SQLPropertyExpr((SQLExpr) rgt, colName);
                    predicates.add(new SQLBinaryOpExpr(left, SQLBinaryOperator.Equality, right));
                }
            }
        }

        // now, we go up the scope until we find a SELECT with a where clause we can add this predicate to

        while (current != null) {
            SQLObject sel = current.getStmt();
            if (sel instanceof MySqlSelectQueryBlock) {
                SQLExpr where = SQLBinaryOpExpr.combine(predicates, SQLBinaryOperator.BooleanAnd);
                SQLUtils.addCondition((SQLStatement) sel, SQLBinaryOperator.BooleanAnd, where, false);
                return;
            }
            current = current.getParent();
        }
        throw new SQLException("did not find WHERE clause");

    }

    private void expandStar(SQLSelectItem node) {
        // TOOD
    }
}
