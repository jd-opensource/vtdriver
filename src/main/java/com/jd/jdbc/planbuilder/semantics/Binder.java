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

import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.expr.SQLAggregateExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLAllColumnExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLExprUtils;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLJoinTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;

/**
 * binder is responsible for finding all the column references in
 * the query and bind them to the table that they belong to.
 * While doing this, it will also find the types for columns and
 * store these in the typer:s expression map
 */
public class Binder {
    private static final SQLAggregateExpr COUNT_STAR;

    static {
        COUNT_STAR = new SQLAggregateExpr("COUNT");
        COUNT_STAR.addArgument(new SQLAllColumnExpr());
    }

    @Getter
    private ExprDependencies recursive;

    @Getter
    private ExprDependencies direct;

    private Scoper scoper;

    private TableCollector tc;

    private Originable org;

    private Typer typer;

    @Getter
    private Map<MySqlSelectQueryBlock, Object[]> subqueryMap;

    @Getter
    private Map<MySqlSelectQueryBlock, Object> subqueryRef;

    /**
     * every table will have an entry in the outer map. it will point to a map with all the columns
     * that this map is joined with using USING.
     * This information is used to expand `*` correctly, and is not available post-analysis
     */
    private Map<TableSet, Map<String, TableSet>> usingJoinInfo;

    public Binder(Scoper scoper, Originable org, TableCollector tc, Typer typer) {
        this.scoper = scoper;
        this.tc = tc;
        this.org = org;
        this.typer = typer;
        this.recursive = new ExprDependencies();
        this.direct = new ExprDependencies();
        this.subqueryMap = new HashMap<>(16);
        this.subqueryRef = new HashMap<>(16);
        this.usingJoinInfo = new HashMap<>(16);
    }

    public void up(SQLObject cursor) throws SQLException {
        if (cursor instanceof SQLJoinTableSource) {
            Scope currentScope = this.scoper.currentScope();
            // using
            List<SQLExpr> usings = ((SQLJoinTableSource) cursor).getUsing();
            for (SQLExpr expr : usings) {
                Dependency deps = this.resolveColumn((SQLName) expr, currentScope, true);
                currentScope.getJoinUsing().put(((SQLName) expr).getSimpleName().toLowerCase(), deps.getDirect());
            }
            if (usings.size() > 0) {
                EarlyRewriter.rewriteJoinUsing(currentScope, usings, this.org);
                // TODO reset using ..
            }

            //  throw new SQLException();
        }
        if (cursor instanceof SQLName) {
            Scope currentScope = this.scoper.currentScope();
            Dependency deps = new Dependency();
            try {
                deps = this.resolveColumn((SQLName) cursor, currentScope, false);
            } catch (SQLException err) {
                String errMsg = err.getMessage();
                boolean checkNumberOfTables = deps.getDirect().numberOfTables() == 0;
                if (checkNumberOfTables
                    || !errMsg.endsWith("is ambiguous")
                    || !this.canRewriteUsingJoin(deps, (SQLName) cursor)) {
                    throw err;
                }

                // if we got here it means we are dealing with a ColName that is involved in a JOIN USING.
                // we do the rewriting of these ColName structs here because it would be difficult to copy all the
                // needed state over to the earlyRewriter
                deps = rewriteJoinUsingColName(deps, (SQLName) cursor, currentScope);
            }
            this.recursive.put(cursor, deps.getRecursive());
            this.direct.put(cursor, deps.getDirect());
            if (deps.getTyp() != null) {
                this.typer.setTypeFor(cursor, deps.getTyp());
            }
        }
        // count(*)
        if (cursor instanceof SQLAggregateExpr && SQLExprUtils.equals(COUNT_STAR, (SQLAggregateExpr) cursor)) {
            TableSet ts = new TableSet();
            Scope scope = this.scoper.currentScope();
            for (TableInfo table : scope.getTables()) {
                SQLTableSource expr = table.getExpr();
                if (expr != null) {
                    ts.mergeInPlace(this.tc.tableSetFor(expr));
                }
            }
            this.recursive.put(cursor, ts);
            this.direct.put(cursor, ts);
        }
    }

    private Dependency resolveColumn(SQLName colName, Scope current, boolean allowMulti) throws SQLException {
        Dependencies thisDeps = null;
        while (current != null) {
            SQLException err = null;
            try {
                thisDeps = this.resolveColumnInScope(colName, current, allowMulti);
            } catch (SQLException e) {
                err = makeAmbiguousError(colName, e);
                if (thisDeps == null) {
                    throw err;
                }
            }
            if (!thisDeps.empty()) {
                try {
                    Dependency deps = thisDeps.get();
                    return deps;
                } catch (SQLException e) {
                    err = makeAmbiguousError(colName, e);
                    throw err;
                }
            }
            if (err != null) {
                throw err;
            }
            current = current.getParent();
        }
        throw new SQLException("symbol " + colName + " not found");
    }

    private Dependencies resolveColumnInScope(SQLName expr, Scope current, boolean allowMulti) throws SQLException {
        Dependencies deps = new Nothing();
        for (TableInfo table : current.getTables()) {
            if (expr instanceof SQLPropertyExpr && !table.matches(expr)) {
                continue;
            }
            Dependencies thisDeps = table.dependencies(expr.getSimpleName(), this.org);
            deps = thisDeps.merge(deps, allowMulti);
        }
        if (deps instanceof Uncertain && ((Uncertain) deps).isFail()) {
            // if we have a failure from uncertain, we matched the column to multiple non-authoritative tables
            throw new Exception.ProjErrorException("Column '" + expr + "' in field list is ambiguous");
        }
        return deps;
    }

    private SQLException makeAmbiguousError(SQLName colName, SQLException err) {
        if (err instanceof Exception.AmbiguousException) {
            return new SQLException("Column '" + colName + "' in field list is ambiguous");
        }
        return err;
    }

    /**
     * canRewriteUsingJoin will return true when this ColName is safe to rewrite since it can only belong to a USING JOIN
     *
     * @param deps
     * @param node
     * @return
     */
    private boolean canRewriteUsingJoin(Dependency deps, SQLName node) {
        List<TableSet> tbls = deps.getDirect().constituents();
        String colName = node.getSimpleName().toLowerCase();
        for (TableSet tbl : tbls) {
            Map<String, TableSet> m = this.usingJoinInfo.get(tbl);
            if (m == null || m.get(colName) == null) {
                return false;
            }
        }
        return true;
    }

    private Dependency rewriteJoinUsingColName(Dependency deps, SQLName node, Scope currentScope) throws SQLException {
        //TODO;
        return deps;
    }
}
