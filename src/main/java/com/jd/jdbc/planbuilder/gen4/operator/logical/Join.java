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

import com.jd.jdbc.planbuilder.gen4.InnerJoin;
import com.jd.jdbc.planbuilder.gen4.QueryTable;
import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator;
import com.jd.jdbc.sqlparser.ast.statement.SQLNotNullConstraint;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

/**
 * Join represents a join. If we have a predicate, this is an inner join. If no predicate exists, it is a cross join
 */
@Getter
@Setter
public class Join implements LogicalOperator {

    private LogicalOperator lHS;

    private LogicalOperator rHS;

    private SQLExpr predicate;

    private Boolean leftJoin = false;


    public Join() {

    }

    public Join(LogicalOperator lHS, LogicalOperator rHS) {
        this.lHS = lHS;
        this.rHS = rHS;
    }

    @Override
    public TableSet tableID() {
        return this.rHS.tableID().merge(this.lHS.tableID());
    }

    @Override
    public SQLSelectItem unsolvedPredicates(SemTable semTable) {

        //TODO

        return null;
    }

    @Override
    public void checkValid() throws SQLException {
        this.lHS.checkValid();
        this.rHS.checkValid();
    }

    @Override
    public LogicalOperator pushPredicate(SQLExpr expr, SemTable semTable) throws SQLException {
        TableSet deps = semTable.recursiveDeps(expr);
        if (deps.isSolvedBy(this.lHS.tableID())) {
            this.lHS = this.lHS.pushPredicate(expr, semTable);
            return this;
        } else if (deps.isSolvedBy(this.rHS.tableID())) {
            this.tryConvertToInnerJoin(expr, semTable);
            if (!this.leftJoin) {
                this.rHS = this.rHS.pushPredicate(expr, semTable);
                return this;
            }

            LogicalOperator op = new Filter(this, Collections.singletonList(expr));
            return op;
        } else if (deps.isSolvedBy(this.lHS.tableID().merge(this.rHS.tableID()))) {
            this.tryConvertToInnerJoin(expr, semTable);
            if (!this.leftJoin) {
                this.predicate = SQLBinaryOpExpr.and(this.predicate, expr);
                return this;
            }
            LogicalOperator op = new Filter(this, Collections.singletonList(expr));
            return op;
        }

        throw new SQLException("Cannot push predicate: " + expr.toString());
    }

    @Override
    public LogicalOperator compact(SemTable semTable) throws SQLException {
        if (this.leftJoin) {
            return this;
        }

        Boolean lok = this.lHS instanceof QueryGraph;
        Boolean rok = this.rHS instanceof QueryGraph;

        if (!lok || !rok) {
            return this;
        }

        List<QueryTable> tables = new ArrayList<>();
        tables.addAll(((QueryGraph) this.lHS).getTables());
        tables.addAll(((QueryGraph) this.rHS).getTables());

        List<InnerJoin> innerJoins = new ArrayList<>();
        innerJoins.addAll(((QueryGraph) this.lHS).getInnerJoins());
        innerJoins.addAll(((QueryGraph) this.rHS).getInnerJoins());

        SQLExpr noDeps = SQLBinaryOpExpr.and(((QueryGraph) this.lHS).getNoDeps(), ((QueryGraph) this.rHS).getNoDeps());

        QueryGraph op = new QueryGraph(tables, innerJoins, noDeps);
        if (this.predicate != null) {
            op.collectPredicate(this.predicate, semTable);
        }

        return op;
    }

    /**
     * When a predicate uses information from an outer table, we can convert from an outer join to an inner join
     * if the predicate is "null-intolerant".
     * <p>
     * Null-intolerant in this context means that the predicate will not be true if the table columns are null.
     * <p>
     * Since an outer join is an inner join with the addition of all the rows from the left-hand side that
     * matched no rows on the right-hand, if we are later going to remove all the rows where the right-hand
     * side did not match, we might as well turn the join into an inner join.
     * <p>
     * This is based on the paper "Canonical Abstraction for Outerjoin Optimization" by J Rao et al
     *
     * @param expr
     * @param semTable
     */
    private void tryConvertToInnerJoin(SQLExpr expr, SemTable semTable) {
        if (!this.leftJoin) {
            return;
        }

        if (expr instanceof SQLBinaryOpExpr) {
            SQLBinaryOperator op = ((SQLBinaryOpExpr) expr).getOperator();
            if (op == SQLBinaryOperator.LessThanOrEqualOrGreaterThan) {
                return;
            }

            if (op == SQLBinaryOperator.Is || op == SQLBinaryOperator.IsNot) {
                if (!(((SQLBinaryOpExpr) expr).getRight() instanceof SQLNotNullConstraint)) {
                    return;
                }
                Boolean checkLeft = ((SQLBinaryOpExpr) expr).getLeft() instanceof SQLName;
                checkLeft = checkLeft && semTable.recursiveDeps(((SQLBinaryOpExpr) expr).getLeft()).isSolvedBy(this.rHS.tableID());

                if (checkLeft) {
                    this.leftJoin = false;
                    return;
                }
            }

            Boolean checkLeft = ((SQLBinaryOpExpr) expr).getLeft() instanceof SQLName;
            checkLeft = checkLeft && semTable.recursiveDeps(((SQLBinaryOpExpr) expr).getLeft()).isSolvedBy(this.rHS.tableID());

            if (checkLeft) {
                this.leftJoin = false;
                return;
            }

            Boolean checkRight = ((SQLBinaryOpExpr) expr).getRight() instanceof SQLName;
            checkRight = checkRight && semTable.recursiveDeps(((SQLBinaryOpExpr) expr).getRight()).isSolvedBy(this.rHS.tableID());

            if (checkRight) {
                this.leftJoin = false;
                return;
            }

        }


    }
}
