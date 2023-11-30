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

import com.google.common.collect.Lists;
import com.jd.jdbc.common.util.CollectionUtils;
import com.jd.jdbc.sqlparser.ASTUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntegerExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLLiteralExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectOrderByItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLUpdateStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/**
 * scoper is responsible for figuring out the scoping for the query,
 * and keeps the current scope when walking the tree
 */
public class Scoper {
    @Getter
    private Map<SQLObject, Scope> rScope;

    @Getter
    private Map<SQLObject, Scope> wScope;

    private List<Scope> scopes;

    @Setter
    private Originable org;

    @Setter
    private Binder binder;

    /**
     * These scopes are only used for rewriting ORDER BY 1 and GROUP BY 1
     */
    @Getter
    private Map<SQLLiteralExpr, Scope> specialExprScopes;

    public Scoper() {
        this.rScope = new HashMap<>(16);
        this.wScope = new HashMap<>(16);
        this.specialExprScopes = new HashMap<>(16);
        this.scopes = new ArrayList<>(10);
    }

    public static boolean validAsMapKey(SQLExpr sqlExpr) {
        // todo
        return true;
    }

    public void down(SQLObject cursor) throws SQLException {
        if (cursor instanceof SQLUpdateStatement) {
            throw new SQLException();
        }
        if (cursor instanceof MySqlSelectQueryBlock) {
            Scope currScope = new Scope(this.currentScope());
            this.push(currScope);

            // Needed for order by with Literal to find the Expression.
            currScope.setStmt(cursor);
            this.rScope.put(cursor, currScope);
            this.wScope.put(cursor, new Scope(null));
            return;
        }
        if (cursor instanceof SQLTableSource) {
            if (cursor.getParent() instanceof MySqlSelectQueryBlock) {
                // when checking the expressions used in JOIN conditions, special rules apply where the ON expression
                // can only see the two tables involved in the JOIN, and no other tables.
                // To create this special context, we create a special scope here that is then merged with
                // the surrounding scope when we come back out from the JOIN
                Scope nScope = new Scope(null);
                nScope.setStmt(cursor.getParent());
                this.push(nScope);
            }
            return;
        }
        if (cursor instanceof SQLSelectItem) {
            if (!(cursor.getParent() instanceof MySqlSelectQueryBlock)) {
                return;
            }
            if (cursor != ((MySqlSelectQueryBlock) cursor.getParent()).getSelectList().get(0)) {
                return;
            }
            List<SQLSelectItem> selectItemList = ((MySqlSelectQueryBlock) cursor.getParent()).getSelectList();
            // adding a vTableInfo for each SELECT, so it can be used by GROUP BY, HAVING, ORDER BY
            // the vTableInfo we are creating here should not be confused with derived tables' vTableInfo
            Scope wScope = this.wScope.get(cursor.getParent());
            if (wScope == null) {
                return;
            }
            wScope.setTables(Lists.newArrayList(Vtable.createVTableInfoForExpressions(selectItemList, this.currentScope().getTables(), this.org)));
            return;
        }
        if (cursor instanceof SQLSelectGroupByClause) {
            SQLSelectGroupByClause groupBy = (SQLSelectGroupByClause) cursor;
            if (CollectionUtils.isNotEmpty(groupBy.getItems())) {
                // groupBy
                createSpecialScopePostProjection(cursor.getParent());
                for (SQLExpr expr : ((SQLSelectGroupByClause) cursor).getItems()) {
                    SQLIntegerExpr lit = keepIntLiteral(expr);
                    if (lit != null) {
                        this.specialExprScopes.put(lit, this.currentScope());
                    }
                }
            }
            return;
        }
        if (cursor instanceof SQLBinaryOpExpr) {
            // having
            if (cursor.getParent() instanceof SQLSelectGroupByClause) {
                SQLSelectGroupByClause groupBy = (SQLSelectGroupByClause) cursor.getParent();
                if (groupBy.getHaving() == cursor) {
                    createSpecialScopePostProjection(cursor.getParent().getParent());
                }
            }
            return;
        }
        if (cursor instanceof SQLOrderBy) {
            if (!(cursor.getParent() instanceof MySqlSelectQueryBlock || cursor.getParent() instanceof SQLUnionQuery)) {
                return;
            }
            createSpecialScopePostProjection(cursor.getParent());
            for (SQLSelectOrderByItem expr : ((SQLOrderBy) cursor).getItems()) {
                SQLIntegerExpr lit = keepIntLiteral(expr.getExpr());
                if (lit != null) {
                    this.specialExprScopes.put(lit, this.currentScope());
                }
            }
        }
    }

    public void up(SQLObject cursor) throws SQLException {
        if (cursor instanceof SQLOrderBy) {
            if (cursor.getParent() instanceof MySqlSelectQueryBlock || cursor.getParent() instanceof SQLUnionQuery) {
                this.popScope();
            }
            return;
        }
        if (cursor instanceof SQLSelectGroupByClause) {
            SQLSelectGroupByClause groupBy = (SQLSelectGroupByClause) cursor;
            if (CollectionUtils.isNotEmpty(groupBy.getItems())) {
                this.popScope();
            }
            return;
        }
        if (cursor instanceof SQLBinaryOpExpr) {
            // having
            if (cursor.getParent() instanceof SQLSelectGroupByClause) {
                SQLSelectGroupByClause groupBy = (SQLSelectGroupByClause) cursor.getParent();
                if (groupBy.getHaving() == cursor) {
                    this.popScope();
                }
            }
            return;
        }
        if (cursor instanceof MySqlSelectQueryBlock) {
            this.popScope();
            return;
        }
        if (cursor instanceof SQLTableSource) {
            if (cursor.getParent() instanceof MySqlSelectQueryBlock) {
                Scope curScope = this.currentScope();
                this.popScope();
                Scope earlierScope = this.currentScope();
                if (CollectionUtils.isEmpty(curScope.getTables())) {
                    return;
                }
                // copy curScope into the earlierScope
                for (TableInfo table : curScope.getTables()) {
                    earlierScope.addTable(table);
                }
            }
        }

    }

    public Scope currentScope() {
        if (scopes.isEmpty()) {
            return null;
        }
        return scopes.get(scopes.size() - 1);
    }

    private void push(Scope scope) {
        scopes.add(scope);
    }

    private void popScope() {
        scopes.remove(scopes.size() - 1);
    }

    /**
     * createSpecialScopePostProjection is used for the special projection in ORDER BY, GROUP BY and HAVING
     *
     * @param parent
     */
    private void createSpecialScopePostProjection(SQLObject parent) throws SQLException {
        if (parent instanceof MySqlSelectQueryBlock) {
            // In ORDER BY, GROUP BY and HAVING, we can see both the scope in the FROM part of the query, and the SELECT columns created
            // so before walking the rest of the tree, we change the scope to match this behaviour
            Scope incomingScope = this.currentScope();
            Scope nScope = new Scope(incomingScope);
            nScope.setTables(this.wScope.get(parent).getTables());
            nScope.setStmt(incomingScope.getStmt());
            this.push(nScope);
            if (this.rScope.get(parent) != incomingScope) {
                throw new SQLException("BUG: scope counts did not match");
            }
        }
        if (parent instanceof SQLUnionQuery) {
            Scope nScope = new Scope(null);
            nScope.setUnion(true);
            VTableInfo tableInfo = null;
            List<MySqlSelectQueryBlock> allSelects = this.getAllSelects((SQLUnionQuery) parent);
            for (int i = 0; i < allSelects.size(); i++) {
                MySqlSelectQueryBlock sel = allSelects.get(i);
                if (i == 0) {
                    nScope.setStmt(sel);
                    tableInfo = Vtable.createVTableInfoForExpressions(sel.getSelectList(), null /*needed for star expressions*/, this.org);
                    nScope.getTables().add(tableInfo);
                }
                VTableInfo thisTableInfo = Vtable.createVTableInfoForExpressions(sel.getSelectList(), null /*needed for star expressions*/, this.org);
                if (tableInfo.getCols().size() != thisTableInfo.getCols().size()) {
                    throw new SQLException("The used SELECT statements have a different number of columns");
                }
                for (int j = 0; j < tableInfo.getCols().size(); j++) {
                    // at this stage, we don't store the actual dependencies, we only store the expressions.
                    // only later will we walk the expression tree and figure out the deps. so, we need to create a
                    // composite expression that contains all the expressions in the SELECTs that this UNION consists of
                    SQLExpr col = tableInfo.getCols().get(j);
                    tableInfo.getCols().set(j, ASTUtils.andExpressions(col, thisTableInfo.getCols().get(j)));
                }
            }
            this.push(nScope);
        }
    }

    private SQLIntegerExpr keepIntLiteral(SQLExpr e) {
        if (e instanceof SQLBinaryOpExpr) {
            if (((SQLBinaryOpExpr) e).getLeft() instanceof SQLIntegerExpr) {
                return (SQLIntegerExpr) ((SQLBinaryOpExpr) e).getLeft();
            }
        }
        if (e instanceof SQLIntegerExpr) {
            return (SQLIntegerExpr) e;
        }
        return null;
    }

    public static List<MySqlSelectQueryBlock> getAllSelects(SQLSelectQuery selStmt) {
        List<MySqlSelectQueryBlock> sqlSelectItems = new ArrayList<>();
        if (selStmt instanceof MySqlSelectQueryBlock) {
            sqlSelectItems.add((MySqlSelectQueryBlock) selStmt);
        }
        if (selStmt instanceof SQLUnionQuery) {
            sqlSelectItems.addAll(getAllSelects(((SQLUnionQuery) selStmt).getLeft()));
            sqlSelectItems.addAll(getAllSelects(((SQLUnionQuery) selStmt).getRight()));
        }
        return sqlSelectItems;
    }
}
