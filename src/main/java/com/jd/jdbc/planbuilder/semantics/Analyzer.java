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

import com.jd.jdbc.common.tuple.ImmutableTriple;
import com.jd.jdbc.common.tuple.Triple;
import com.jd.jdbc.planbuilder.PlanBuilder;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLJoinTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelect;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLUpdateStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.visitor.AnalyzerVisitor;
import com.jd.jdbc.sqlparser.visitor.CheckUnionVisitor;
import java.sql.SQLException;
import java.util.HashMap;
import lombok.Getter;
import lombok.Setter;

/**
 * analyzer controls the flow of the analysis.
 * It starts the tree walking and controls which part of the analysis sees which parts of the tree
 */
public class Analyzer implements Originable {
    @Getter
    private Scoper scoper;

    @Getter
    private TableCollector tables;

    @Getter
    @Setter
    private Binder binder;

    @Getter
    @Setter
    private Typer typer;

    @Getter
    @Setter
    private EarlyRewriter rewriter;

    private SQLException err;

    private int inProjection;

    private SQLException projErr;
    //private String projErr;

    private SQLException unshardedErr;
    //private String unshardedErr;

    private String warning;

    public Analyzer(Scoper scoper, TableCollector tables, Typer typer) {
        this.scoper = scoper;
        this.tables = tables;
        this.typer = typer;
    }

    /**
     * newAnalyzer create the semantic analyzer
     *
     * @return
     */
    public static Analyzer newAnalyzer(String dbName, SchemaInformation si) {
        // TODO  dependencies between these components are a little tangled. We should try to clean up
        Scoper s = new Scoper();
        TableCollector tables = new TableCollector(s, si, dbName);
        Typer typer = new Typer();
        Analyzer a = new Analyzer(s, tables, typer);
        s.setOrg(a);
        a.getTables().setOrg(a);
        Binder b = new Binder(s, a, a.getTables(), a.getTyper());

        a.setBinder(b);
        EarlyRewriter earlyRewriter = new EarlyRewriter(s, b);
        a.setRewriter(earlyRewriter);
        s.setBinder(b);
        return a;
    }

    /**
     * Analyze analyzes the parsed query.
     *
     * @param statement
     * @param currentDb
     * @param si
     * @return
     */
    public static SemTable analyze(SQLSelectStatement statement, String currentDb, SchemaInformation si) throws SQLException {
        Analyzer analyzer = newAnalyzer(currentDb, si);

        // Analysis for initial scope
        analyzer.analyze(statement);

        // Creation of the semantic table
        return analyzer.newSemTable(statement, null);
    }

    private void analyze(SQLSelectStatement statement) throws SQLException {
        AnalyzerVisitor analyzerVisitor = new AnalyzerVisitor(this);
        statement.accept(analyzerVisitor);
        if (this.err != null) {
            throw this.err;
        }
    }

    private SemTable newSemTable(SQLSelectStatement statement, Object coll) {
        String comments = "";
        String projErr = this.projErr == null ? null : this.projErr.getMessage();
        String unshardedErr = this.unshardedErr == null ? null : this.unshardedErr.getMessage();

        return new SemTable(this.binder.getRecursive(), this.binder.getDirect(), this.typer.getExprTypes(), this.tables.getTables(), this.getScoper().getRScope(),
            projErr, unshardedErr, this.warning, comments, this.binder.getSubqueryMap(), this.binder.getSubqueryRef(), new HashMap<>(16), coll);
    }

    @Override
    public TableSet tableSetFor(SQLTableSource t) throws SQLException {
        return this.tables.tableSetFor(t);
    }

    @Override
    public Triple<TableSet, TableSet, Type> depsForExpr(SQLExpr expr) {
        TableSet direct = this.binder.getDirect().dependencies(expr);
        TableSet recursive = this.binder.getRecursive().dependencies(expr);
        Type qt = this.typer.getExprTypes().get(expr);
        return new ImmutableTriple<>(direct, recursive, qt);
    }

    private void setError(SQLException error) {
        if (error instanceof Exception.ProjErrorException) {
            this.projErr = error;
        } else if (error instanceof Exception.UnshardedErrorException) {
            this.unshardedErr = error;
        } else {
            this.err = error;
        }
    }

    public boolean analyzeDown(SQLObject x) {
        // If we have an error we keep on going down the tree without checking for anything else
        // this way we can abort when we come back up.
        if (!this.shouldContinue()) {
            return true;
        }

        try {
            this.scoper.down(x);
            this.checkForInvalidConstruct(x);
            this.rewriter.down(x);
        } catch (SQLException e) {
            this.setError(e);
            return true;
        }
        this.warning = this.rewriter.getWarning();
        this.enterProjection(x);
        // this is the visitor going down the tree. Returning false here would just not visit the children
        // to the current node, but that is not what we want if we have encountered an error.
        // In order to abort the whole visitation, we have to return true here and then return false in the `analyzeUp` method
        return true;
    }

    public boolean analyzeUp(SQLObject x) {
        if (!this.shouldContinue()) {
            return true;
        }
        try {
            this.binder.up(x);
        } catch (SQLException e) {
            this.setError(e);
            return true;
        }
        try {
            this.scoper.up(x);
            this.tables.up(x);
            this.typer.up(x);
        } catch (SQLException e) {
            this.setError(e);
            return false;
        }
        this.leaveProjection(x);
        return this.shouldContinue();
    }

    private boolean shouldContinue() {
        return this.err == null;
    }

    private void checkForInvalidConstruct(SQLObject x) throws SQLException {
        if (x instanceof SQLUpdateStatement) {
            if (!(((SQLUpdateStatement) x).getFrom() instanceof SQLExprTableSource)) {
                throw new SQLException("unsupported: only support single simple table in update");
            }
        } else if (x instanceof MySqlSelectQueryBlock) {
            SQLObject parent = x.getParent();
            if (parent instanceof SQLUnionQuery && ((MySqlSelectQueryBlock) x).isCalcFoundRows()) {
                throw new SQLException("SQL_CALC_FOUND_ROWS not supported with union");
            }
            if (!(parent instanceof SQLSelect) && ((MySqlSelectQueryBlock) x).isCalcFoundRows()) {
                throw new SQLException("Incorrect usage/placement of 'SQL_CALC_FOUND_ROWS'");
            }
            if (((MySqlSelectQueryBlock) x).getInto() == null) {
                return;
            }
            throw new SQLException("unsupported: doesn't support into expr in select");
        } else if (x instanceof SQLJoinTableSource) {
            // vitess其实支持排除了natural join类型，现在先不支持所有JOIN
            SQLJoinTableSource.JoinType joinType = ((SQLJoinTableSource) x).getJoinType();
            if (joinType == SQLJoinTableSource.JoinType.NATURAL_JOIN || joinType == SQLJoinTableSource.JoinType.NATURAL_INNER_JOIN) {
                throw new SQLException("unsupported: " + ((SQLJoinTableSource) x).getJoinType());
            }
        } else if (x instanceof SQLUnionQuery) {
            CheckUnionVisitor visitor = new CheckUnionVisitor();
            if (((SQLUnionQuery) x).getOrderBy() != null) {
                ((SQLUnionQuery) x).getOrderBy().accept(visitor);
            }
            if (visitor.getErr() != null) {
                throw new SQLException(visitor.getErr());
            }
            checkUnionColumns((SQLUnionQuery) x);
        }
    }

    private void checkUnionColumns(SQLUnionQuery union) throws SQLException {
        MySqlSelectQueryBlock firstProj = PlanBuilder.getFirstSelect(union);
        if (firstProj.selectItemHasAllColumn()) {
            // if we still have *, we can't figure out if the query is invalid or not
            // we'll fail it at run time instead
            return;
        }

        MySqlSelectQueryBlock secondProj = PlanBuilder.getFirstSelect(union.getRight());
        if (secondProj.selectItemHasAllColumn()) {
            return;
        }

        if (firstProj.getSelectList().size() != secondProj.getSelectList().size()) {
            throw new SQLException("The used SELECT statements have a different number of columns");
        }
    }

    /**
     * errors that happen when we are evaluating SELECT expressions are saved until we know
     * if we can merge everything into a single route or not
     *
     * @param cursor
     */
    private void enterProjection(SQLObject cursor) {
        if (cursor instanceof SQLSelectItem) {
            if (cursor.getParent() instanceof MySqlSelectQueryBlock) {
                this.inProjection++;
            }
        }
    }

    private void leaveProjection(SQLObject cursor) {
        if (cursor instanceof SQLSelectItem) {
            if (cursor.getParent() instanceof MySqlSelectQueryBlock) {
                this.inProjection--;
            }
        }
    }
}
