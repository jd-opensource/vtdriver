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

package com.jd.jdbc.planbuilder.gen4.operator.logical;

import com.jd.jdbc.planbuilder.PlanBuilder;
import com.jd.jdbc.planbuilder.gen4.InnerJoin;
import com.jd.jdbc.planbuilder.gen4.QueryTable;
import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

/**
 * QueryGraph represents the FROM and WHERE parts of a query.
 * It is an intermediate representation of the query that makes it easier for the planner
 * to find all possible join combinations. Instead of storing the query information in a form that is close
 * to the syntax (AST), we extract the interesting parts into a graph form with the nodes being tables in the FROM
 * clause and the edges between them being predicates. We keep predicates in a hash map keyed by the dependencies of
 * the predicate. This makes it very fast to look up connections between tables in the query.
 */
@Getter
@Setter
public class QueryGraph implements LogicalOperator {
    private List<QueryTable> tables;

    private List<InnerJoin> innerJoins;

    private SQLExpr noDeps;

    public QueryGraph() {
        tables = new ArrayList<>();
        innerJoins = new ArrayList<>();
    }

    public QueryGraph(List<QueryTable> tables, List<InnerJoin> innerJoins, SQLExpr noDeps) {
        this.tables = tables;
        this.innerJoins = innerJoins;
        this.noDeps = noDeps;
    }

    @Override
    public TableSet tableID() {
        TableSet ts = new TableSet();
        for (QueryTable table : this.tables) {
            ts = ts.merge(table.getId());
        }
        return ts;
    }

    /**
     * GetPredicates returns the predicates that are applicable for the two given TableSets
     *
     * @param lhs
     * @param rhs
     * @return
     */
    public List<SQLExpr> getPredicates(TableSet lhs, TableSet rhs) {
        List<SQLExpr> allExprs = new ArrayList<>();
        for (InnerJoin join : this.innerJoins) {
            if (join.getDeps().isSolvedBy(lhs.merge(rhs)) &&
                join.getDeps().isOverlapping(rhs) &&
                join.getDeps().isOverlapping(lhs)) {
                allExprs.addAll(join.getExprs());
            }
        }
        return allExprs;
    }

    @Override
    public SQLSelectItem unsolvedPredicates(SemTable semTable) {
        return null;
    }

    @Override
    public void checkValid() throws SQLException {
    }

    @Override
    public LogicalOperator pushPredicate(SQLExpr expr, SemTable semTable) throws SQLException {
        List<SQLExpr> subExpr = PlanBuilder.splitAndExpression(null, expr);
        for (SQLExpr sqlExpr : subExpr) {
            collectPredicate(sqlExpr, semTable);
        }
        return this;
    }

    @Override
    public LogicalOperator compact(SemTable semTable) throws SQLException {
        return this;
    }

    public void collectPredicate(SQLExpr predicate, SemTable semTable) throws SQLException {
        TableSet deps = semTable.recursiveDeps(predicate);
        switch (deps.numberOfTables()) {
            case 0:
                addNoDepsPredicate(predicate);
                break;
            case 1:
                boolean found = addToSingleTable(deps, predicate);
                if (!found) {
                    // this could be a predicate that only has dependencies from outside this QG
                    addJoinPredicates(deps, predicate);
                }
                break;
            default:
                addJoinPredicates(deps, predicate);
        }
    }

    public void addNoDepsPredicate(SQLExpr predicate) {
        if (this.noDeps == null) {
            this.setNoDeps(predicate);
        } else {
            SQLBinaryOpExpr andexpr = new SQLBinaryOpExpr(this.noDeps, SQLBinaryOperator.BooleanAnd, predicate);
            this.setNoDeps(andexpr);
        }
    }

    public boolean addToSingleTable(TableSet tableSet, SQLExpr predicate) {
        for (int i = 0; i < this.getTables().size(); i++) {
            if (this.getTables().get(i).getId().equals(tableSet)) {
                this.getTables().get(i).getPredicates().add(predicate);
                return true;
            }
        }
        return false;
    }

    public void addJoinPredicates(TableSet ts, SQLExpr predicate) {
        for (InnerJoin join : this.innerJoins) {
            if (join.getDeps() == ts) {
                join.getExprs().add(predicate);
                return;
            }
        }
        this.innerJoins.add(new InnerJoin(ts, Arrays.asList(new SQLExpr[] {predicate})));
    }

}
