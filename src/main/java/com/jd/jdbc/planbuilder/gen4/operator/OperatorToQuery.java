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

package com.jd.jdbc.planbuilder.gen4.operator;

import com.jd.jdbc.context.PlanningContext;
import com.jd.jdbc.planbuilder.gen4.operator.physical.ApplyJoin;
import com.jd.jdbc.planbuilder.gen4.operator.physical.Filter;
import com.jd.jdbc.planbuilder.gen4.operator.physical.PhysicalOperator;
import com.jd.jdbc.planbuilder.gen4.operator.physical.Table;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLJoinTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class OperatorToQuery {

    PlanningContext ctx;

    MySqlSelectQueryBlock sel;

    List<String> tableNames = new ArrayList<>();

    public OperatorToQuery(PlanningContext ctx) {
        this.ctx = ctx;
    }

    public static SQLSelectQuery toSQL(PlanningContext ctx, PhysicalOperator op) throws SQLException {
        OperatorToQuery q = new OperatorToQuery(ctx);
        q.buildQuery(op);
        q.sortTables();
        return q.sel;
    }

    private void buildQuery(PhysicalOperator op) throws SQLException {
        if (op instanceof Table) {
            this.buildQueryForTable((Table) op);
        } else if (op instanceof Filter) {
            this.buildQuery(((Filter) op).getSource());
            for (SQLExpr pred : ((Filter) op).getPredicates()) {
                this.addPredicate(pred);
            }
        } else if (op instanceof ApplyJoin) {
            this.buildQuery(((ApplyJoin) op).getLHS());
            // If we are going to add the predicate used in join here
            // We should not add the predicate's copy of when it was split into
            // two parts. To avoid this, we use the SkipPredicates map.
            List<SQLExpr> joinPredicates = this.ctx.getJoinPredicates().get(((ApplyJoin) op).getPredicate());
            if (joinPredicates != null) {
                for (SQLExpr expr : joinPredicates) {
                    this.ctx.getSkipPredicates().remove(expr);
                }
            }

            OperatorToQuery qbR = new OperatorToQuery(this.ctx);
            qbR.buildQuery(((ApplyJoin) op).getRHS());
            if (((ApplyJoin) op).getLeftJoin()) {
                this.joinOuterWith(qbR, ((ApplyJoin) op).getPredicate());
            } else {
                this.joinInnerWith(qbR, ((ApplyJoin) op).getPredicate());
            }
        } else {
            throw new SQLException("gen4 planner unsupported !! " + op.getClass().toString());
        }
    }

    private void buildQueryForTable(Table op) {
        String dbName = "";
        this.addTable(op.getQTable().getTable().getName(), op.getQTable().getAlias().computeAlias(), op.tableID(), op.getQTable().getAlias());
        for (SQLExpr expr : op.getQTable().getPredicates()) {
            this.addPredicate(expr);  // add where
        }

        // Add Projection
        for (SQLName columnExpr : op.getColumns()) {
            this.sel.addSelectItem(columnExpr);
        }

    }


    private void addPredicate(SQLExpr expr) {
        if (this.ctx.getSkipPredicates().get(expr) != null) {
            // This is a predicate that was added to the RHS of an ApplyJoin.
            // The original predicate will be added, so we don't have to add this here
            return;
        }

        /*
        * addWhere 已经处理过多个条件的情况，不需要再拆分
        *
        if (this.sel.getWhere() == null){
            this.sel.addWhere(expr);
            return;
        }
        List<SQLExpr> filters = PlanBuilder.splitAndExpression(null, expr);
        for(SQLExpr filter: filters){
            this.sel.addWhere(filter);
        }
        */

        this.sel.addWhere(expr);

    }

    private void sortTables() {

    }

    private void addTable(String tableName, String alias, TableSet tableID, SQLExprTableSource source) {
        this.addTableExpr(tableName, source, tableID);
    }

    private void addTableExpr(String tableName, SQLExprTableSource tableSource, TableSet tableID) {
        if (this.sel == null) {
            this.sel = new MySqlSelectQueryBlock();
        }

        //  SQLExprTableSource tableSource = new SQLExprTableSource();
        try {
            this.ctx.getSemTable().replaceTableSetFor(tableID, tableSource);
        } catch (SQLException ex) {

        }

        this.sel.setFrom(tableSource);
        this.tableNames.add(tableName);
    }

    private void joinInnerWith(OperatorToQuery other, SQLExpr onCondition) {
        SQLTableSource lhs = this.sel.getFrom();
        SQLTableSource rhs = other.sel.getFrom();
        SQLTableSource newTableSource = new SQLJoinTableSource(lhs, SQLJoinTableSource.JoinType.COMMA, rhs, null);
        this.sel.setFrom(newTableSource);

        this.sel.getSelectList().addAll(other.sel.getSelectList());

        SQLExpr predicate = null;

        if (this.sel.getWhere() != null) {
            predicate = this.sel.getWhere();
        }

        if (other.sel.getWhere() != null) {
            predicate = SQLBinaryOpExpr.and(predicate, other.sel.getWhere());
        }

        if (predicate != null) {
            this.sel.setWhere(predicate);
        }
        this.addPredicate(onCondition);
    }

    private void joinOuterWith(OperatorToQuery other, SQLExpr onCondition) {
        SQLTableSource lhs = this.sel.getFrom();
        SQLTableSource rhs = other.sel.getFrom();

        SQLTableSource newTableSource = new SQLJoinTableSource(lhs, SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN, rhs, onCondition);
        this.sel.setFrom(newTableSource);

        this.sel.getSelectList().addAll(other.sel.getSelectList());

        SQLExpr predicate = null;

        if (this.sel.getWhere() != null) {
            predicate = this.sel.getWhere();
        }

        if (other.sel.getWhere() != null) {
            predicate = SQLBinaryOpExpr.and(predicate, other.sel.getWhere());
        }

        if (predicate != null) {
            this.sel.setWhere(predicate);
        }
    }
}
