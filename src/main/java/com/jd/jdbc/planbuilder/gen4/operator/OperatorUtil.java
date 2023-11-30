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

import com.google.common.collect.Lists;
import com.jd.jdbc.planbuilder.PlanBuilder;
import com.jd.jdbc.planbuilder.gen4.InnerJoin;
import com.jd.jdbc.planbuilder.gen4.QueryTable;
import com.jd.jdbc.planbuilder.gen4.TableName;
import com.jd.jdbc.planbuilder.gen4.operator.logical.Concatenate;
import com.jd.jdbc.planbuilder.gen4.operator.logical.Join;
import com.jd.jdbc.planbuilder.gen4.operator.logical.LogicalOperator;
import com.jd.jdbc.planbuilder.gen4.operator.logical.QueryGraph;
import com.jd.jdbc.planbuilder.gen4.operator.logical.SubQuery;
import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.planbuilder.semantics.TableInfo;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.planbuilder.semantics.VindexTable;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLLimit;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLJoinTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSubqueryTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionOperator;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLUpdateStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRemoveDbNameVisitor;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class OperatorUtil {
    /**
     * CreateLogicalOperatorFromAST creates an operator tree that represents the input SELECT or UNION query
     *
     * @return
     * @throws SQLException
     */
    public static LogicalOperator createLogicalOperatorFromAST(SQLStatement stmt, SemTable semTable) throws SQLException {
        LogicalOperator op;
        if (stmt instanceof SQLSelectStatement) {
            SQLSelectQuery query = ((SQLSelectStatement) stmt).getSelect().getQuery();
            op = createLogicalOperatorFromAST(query, semTable);
        } else if (stmt instanceof SQLUpdateStatement) {
            throw new SQLFeatureNotSupportedException();
        } else {
            throw new SQLException("BUG: unexpected statement type: " + SQLUtils.toMySqlString(stmt, SQLUtils.NOT_FORMAT_OPTION));
        }
        return op.compact(semTable);
    }

    private static LogicalOperator createLogicalOperatorFromAST(SQLSelectQuery query, SemTable semTable) throws SQLException {
        LogicalOperator op;
        if (query instanceof MySqlSelectQueryBlock) {
            op = createOperatorFromSelect((MySqlSelectQueryBlock) query, semTable);
        } else if (query instanceof SQLUnionQuery) {
            op = createOperatorFromUnion((SQLUnionQuery) query, semTable);
        } else {
            throw new SQLFeatureNotSupportedException();
        }
        return op.compact(semTable);
    }

    /**
     * createOperatorFromSelect creates an operator tree that represents the input SELECT query
     *
     * @param query
     * @param semTable
     * @return
     * @throws SQLException
     */
    private static LogicalOperator createOperatorFromSelect(MySqlSelectQueryBlock query, SemTable semTable) throws SQLException {
        SubQuery subQuery = createSubqueryFromStatement(query, semTable);
        LogicalOperator op = crossJoin(query.getFrom(), semTable);

        if (query.getWhere() != null) {
            List<SQLExpr> exprs = PlanBuilder.splitAndExpression(null, query.getWhere());
            for (SQLExpr expr : exprs) {
                op = op.pushPredicate(PlanBuilder.removeKeyspaceFromColName(expr), semTable);
                addColumnEquality(semTable, expr);
            }
        }

        if (subQuery == null) {
            return op;
        }
        subQuery.setOuter(op);
        return subQuery;
    }

    private static LogicalOperator createOperatorFromUnion(SQLUnionQuery node, SemTable semTable) throws SQLException {
        LogicalOperator opLHS = createLogicalOperatorFromAST(node.getLeft(), semTable);
        if (node.getRight() instanceof SQLUnionQuery) {
            throw new SQLException("nesting of unions at the right-hand side is not yet supported");
        }
        LogicalOperator opRHS = createLogicalOperatorFromAST(node.getRight(), semTable);

        SQLOrderBy orderBy = node.getOrderBy();
        SQLLimit limit = node.getLimit();
        boolean distinct;
        if (Objects.equals(node.getOperator(), SQLUnionOperator.UNION_ALL)) {
            distinct = false;
        } else if (Objects.equals(node.getOperator(), SQLUnionOperator.UNION)) {
            distinct = true;
        } else {
            throw new SQLFeatureNotSupportedException(node.getOperator().toString());
        }
        return new Concatenate(distinct, Lists.newArrayList(node.getLeft(), node.getRight()), Lists.newArrayList(opLHS, opRHS), orderBy, limit);
    }

    private static SubQuery createSubqueryFromStatement(MySqlSelectQueryBlock selectStatement, SemTable semTable) throws SQLException {
        if (semTable.getSubqueryMap().size() == 0) {
            return null;
        }
        SubQuery subQuery = new SubQuery();
        return subQuery;
    }

    private static LogicalOperator crossJoin(SQLTableSource exprs, SemTable semTable) throws SQLException {
        LogicalOperator output;
        if (exprs instanceof SQLExprTableSource) {
            output = getOperatorFromTableExpr((SQLExprTableSource) exprs, semTable);
        } else if (exprs instanceof SQLJoinTableSource) {
            LogicalOperator leftOutput = crossJoin(((SQLJoinTableSource) exprs).getLeft(), semTable);
            LogicalOperator rightOutput = crossJoin(((SQLJoinTableSource) exprs).getRight(), semTable);


            SQLJoinTableSource.JoinType joinType = ((SQLJoinTableSource) exprs).getJoinType();
            if (joinType == SQLJoinTableSource.JoinType.COMMA
                || joinType == SQLJoinTableSource.JoinType.INNER_JOIN
                || joinType == SQLJoinTableSource.JoinType.NATURAL_INNER_JOIN
                || joinType == SQLJoinTableSource.JoinType.JOIN) {    //normal join

                output = createJoin(leftOutput, rightOutput);
                if (((SQLJoinTableSource) exprs).getCondition() != null) {
                    output.pushPredicate(((SQLJoinTableSource) exprs).getCondition(), semTable);
                }
            } else if (joinType == SQLJoinTableSource.JoinType.LEFT_OUTER_JOIN
                || joinType == SQLJoinTableSource.JoinType.RIGHT_OUTER_JOIN) {

                if (joinType == SQLJoinTableSource.JoinType.RIGHT_OUTER_JOIN) {
                    LogicalOperator tmp = leftOutput;
                    leftOutput = rightOutput;
                    rightOutput = tmp;
                }
                output = new Join(leftOutput, rightOutput);
                ((Join) output).setLeftJoin(true);
                VtRemoveDbNameVisitor visitor = new VtRemoveDbNameVisitor();
                SQLExpr predicate = ((SQLJoinTableSource) exprs).getCondition();
                predicate.accept(visitor);
                ((Join) output).setPredicate(predicate);
            } else {
                throw new SQLException("unsupported: " + joinType.toString());
            }
        } else if (exprs instanceof SQLSubqueryTableSource) {
            throw new SQLException("BUG: unexpected statement type: " + SQLUtils.toMySqlString(exprs, SQLUtils.NOT_FORMAT_OPTION));
        } else {
            throw new SQLException("BUG: unexpected statement type: " + SQLUtils.toMySqlString(exprs, SQLUtils.NOT_FORMAT_OPTION));
        }
        return output;
    }

    private static LogicalOperator getOperatorFromTableExpr(SQLExprTableSource tableSource, SemTable semTable) throws SQLException {
        if (tableSource.getExpr() instanceof SQLName) {
            String qualifier = null;
            String tableName = null;
            if (tableSource.getExpr() instanceof SQLIdentifierExpr) {
                SQLIdentifierExpr identifierExpr = (SQLIdentifierExpr) tableSource.getExpr();
                qualifier = "";
                tableName = identifierExpr.getName();
            } else if (tableSource.getExpr() instanceof SQLPropertyExpr) {
                SQLPropertyExpr propertyExpr = (SQLPropertyExpr) tableSource.getExpr();
                qualifier = propertyExpr.getOwnernName();
                tableName = propertyExpr.getName();
            }
            TableSet tableID = semTable.tableSetFor(tableSource);
            TableInfo tableInfo = semTable.tableInfoFor(tableID);
            if (tableInfo instanceof VindexTable) {
                throw new SQLFeatureNotSupportedException("unsupported TableInfo.VindexTable");
            }

            boolean isInfSchema = tableInfo.isInfSchema();
            QueryGraph qg = new QueryGraph();
            QueryTable qt = new QueryTable(tableID, tableSource, new TableName(qualifier, tableName), isInfSchema);
            qg.getTables().add(qt);
            return qg;
        } else {
            // todo: case tableSource type
            throw new SQLException("BUG: unexpected statement type: " + SQLUtils.toMySqlString(tableSource));
        }
    }

    private static LogicalOperator createJoin(LogicalOperator lhs, LogicalOperator rhs) {
        Boolean lok = lhs instanceof QueryGraph;
        Boolean rok = rhs instanceof QueryGraph;
        if (lok && rok) {
            List<QueryTable> tables = new ArrayList<>();
            tables.addAll(((QueryGraph) lhs).getTables());
            tables.addAll(((QueryGraph) rhs).getTables());

            List<InnerJoin> innerJoins = new ArrayList<>();
            innerJoins.addAll(((QueryGraph) lhs).getInnerJoins());
            innerJoins.addAll(((QueryGraph) rhs).getInnerJoins());

            SQLExpr noDeps = SQLBinaryOpExpr.and(((QueryGraph) lhs).getNoDeps(), ((QueryGraph) rhs).getNoDeps());

            return new QueryGraph(tables, innerJoins, noDeps);
        }

        return new Join(lhs, rhs);
    }

    private static void addColumnEquality(SemTable semTable, SQLExpr expr) {
        if (expr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) expr;
            if (sqlBinaryOpExpr.getOperator() != SQLBinaryOperator.Equality) {
                return;
            }
            // todo 需要考虑a.b情况，判断是否为列应该有方法
            if (sqlBinaryOpExpr.getLeft() instanceof SQLName) {
                semTable.addColumnEquality((SQLName) sqlBinaryOpExpr.getLeft(), sqlBinaryOpExpr.getRight());
            }
            if (sqlBinaryOpExpr.getRight() instanceof SQLName) {
                semTable.addColumnEquality((SQLName) sqlBinaryOpExpr.getRight(), sqlBinaryOpExpr.getLeft());
            }
            if (sqlBinaryOpExpr.getLeft() instanceof SQLPropertyExpr) {

            }
            if (sqlBinaryOpExpr.getRight() instanceof SQLPropertyExpr) {

            }
        }
    }
}

