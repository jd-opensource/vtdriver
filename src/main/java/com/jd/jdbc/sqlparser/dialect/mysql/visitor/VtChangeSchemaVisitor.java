/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jd.jdbc.sqlparser.dialect.mysql.visitor;

import com.jd.jdbc.planbuilder.RoutePlan;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.expr.SQLAggregateExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBetweenExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLCaseExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLInListExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLMethodInvokeExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLQueryExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLInsertStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLJoinTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelect;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectOrderByItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLUpdateSetItem;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.jd.jdbc.util.SchemaUtil;
import java.util.List;


public class VtChangeSchemaVisitor extends MySqlASTVisitorAdapter {

    private final String defaultKeyspace;

    private boolean isReplace;

    private String newDefaultKeyspace;

    public VtChangeSchemaVisitor(final String defaultKeyspace) {
        this.defaultKeyspace = defaultKeyspace;
        this.isReplace = false;
    }

    @Override
    public boolean visit(final SQLSelectStatement stmt) {
        if (stmt == null) {
            return false;
        }

        SQLSelect select = stmt.getSelect();
        if (select != null) {
            visit(select);
        }

        return false;
    }

    @Override
    public boolean visit(final MySqlInsertStatement stmt) {
        if (stmt == null) {
            return false;
        }

        List<SQLExpr> columnExprList = stmt.getColumns();
        if (columnExprList != null && !columnExprList.isEmpty()) {
            for (SQLExpr expr : columnExprList) {
                this.visitExprByType(expr);
            }
        }

        SQLExprTableSource exprTableSource = stmt.getTableSource();
        if (exprTableSource != null) {
            visit(exprTableSource);
        }

        List<SQLInsertStatement.ValuesClause> valuesClauseList = stmt.getValuesList();
        if (valuesClauseList != null && !valuesClauseList.isEmpty()) {
            for (SQLInsertStatement.ValuesClause valuesClause : valuesClauseList) {
                visit(valuesClause);
            }
        }

        SQLSelect select = stmt.getQuery();
        if (select != null) {
            visit(select);
        }

        List<SQLExpr> duplicateKeyUpdateExprList = stmt.getDuplicateKeyUpdate();
        if (duplicateKeyUpdateExprList != null && !duplicateKeyUpdateExprList.isEmpty()) {
            for (SQLExpr expr : duplicateKeyUpdateExprList) {
                this.visitExprByType(expr);
            }
        }

        return false;
    }

    @Override
    public boolean visit(final MySqlUpdateStatement stmt) {
        if (stmt == null) {
            return false;
        }

        SQLTableSource tableSource = stmt.getTableSource();
        if (tableSource instanceof SQLExprTableSource) {
            visit((SQLExprTableSource) tableSource);
        } else if (tableSource instanceof SQLJoinTableSource) {
            visit((SQLJoinTableSource) tableSource);
        }

        List<SQLUpdateSetItem> updateSetItemList = stmt.getItems();
        if (updateSetItemList != null && !updateSetItemList.isEmpty()) {
            for (SQLUpdateSetItem updateSetItem : updateSetItemList) {
                visit(updateSetItem);
            }
        }

        SQLExpr whereExpr = stmt.getWhere();
        if (whereExpr != null) {
            this.visitExprByType(whereExpr);
        }

        SQLOrderBy orderBy = stmt.getOrderBy();
        if (orderBy != null) {
            visit(orderBy);
        }

        return false;
    }

    @Override
    public boolean visit(final MySqlDeleteStatement stmt) {
        if (stmt == null) {
            return false;
        }

        SQLTableSource tableSource = stmt.getTableSource();
        if (tableSource instanceof SQLExprTableSource) {
            visit((SQLExprTableSource) tableSource);
        } else if (tableSource instanceof SQLJoinTableSource) {
            visit((SQLJoinTableSource) tableSource);
        }

        SQLTableSource fromTableSource = stmt.getFrom();
        if (fromTableSource instanceof SQLExprTableSource) {
            visit((SQLExprTableSource) fromTableSource);
        } else if (fromTableSource instanceof SQLJoinTableSource) {
            visit((SQLJoinTableSource) fromTableSource);
        }

        SQLTableSource usingTableSource = stmt.getUsing();
        if (usingTableSource instanceof SQLExprTableSource) {
            visit((SQLExprTableSource) usingTableSource);
        } else if (usingTableSource instanceof SQLJoinTableSource) {
            visit((SQLJoinTableSource) usingTableSource);
        }

        SQLExpr whereExpr = stmt.getWhere();
        if (whereExpr != null) {
            this.visitExprByType(whereExpr);
        }

        SQLOrderBy orderBy = stmt.getOrderBy();
        if (orderBy != null) {
            visit(orderBy);
        }

        return false;
    }

    @Override
    public boolean visit(final SQLSelect x) {
        if (x == null) {
            return false;
        }

        SQLSelectQuery selectQuery = x.getQuery();
        if (selectQuery instanceof MySqlSelectQueryBlock) {
            visit((MySqlSelectQueryBlock) selectQuery);
        }

        SQLOrderBy orderBy = x.getOrderBy();
        if (orderBy != null) {
            visit(orderBy);
        }

        return false;
    }

    @Override
    public boolean visit(final MySqlSelectQueryBlock x) {
        if (x == null) {
            return false;
        }

        List<SQLSelectItem> selectList = x.getSelectList();
        if (selectList != null && !selectList.isEmpty()) {
            for (SQLSelectItem selectItem : selectList) {
                visit(selectItem);
            }
        }

        SQLTableSource tableSource = x.getFrom();
        if (tableSource instanceof SQLExprTableSource) {
            visit((SQLExprTableSource) tableSource);
        } else if (tableSource instanceof SQLJoinTableSource) {
            visit((SQLJoinTableSource) tableSource);
        }

        SQLExpr whereExpr = x.getWhere();
        if (whereExpr != null) {
            this.visitExprByType(whereExpr);
        }

        SQLOrderBy orderBy = x.getOrderBy();
        if (orderBy != null) {
            visit(orderBy);
        }

        SQLSelectGroupByClause selectGroupByClause = x.getGroupBy();
        if (selectGroupByClause != null) {
            visit(selectGroupByClause);
        }

        return false;
    }

    @Override
    public boolean visit(final SQLSelectItem x) {
        if (x == null) {
            return false;
        }

        SQLExpr expr = x.getExpr();
        if (expr != null) {
            this.visitExprByType(expr);
        }

        return false;
    }

    @Override
    public boolean visit(final SQLPropertyExpr x) {
        if (x == null) {
            return false;
        }

        SQLExpr tableExpr = x.getOwner();
        if (!(tableExpr instanceof SQLPropertyExpr)) {
            return false;
        }

        SQLExpr schemaExpr = ((SQLPropertyExpr) tableExpr).getOwner();
        if (schemaExpr instanceof SQLIdentifierExpr) {
            String schemaName = ((SQLIdentifierExpr) schemaExpr).getSimpleName();
            if (RoutePlan.systemTable(schemaName)) {
                return false;
            }
            this.replaceSchema((SQLPropertyExpr) tableExpr);
        }

        return false;
    }

    @Override
    public boolean visit(final SQLAggregateExpr x) {
        if (x == null) {
            return false;
        }

        this.visitArgumentList(x.getArguments());

        return false;
    }

    @Override
    public boolean visit(final SQLMethodInvokeExpr x) {
        if (x == null) {
            return false;
        }

        this.visitArgumentList(x.getArguments());

        return false;
    }

    @Override
    public boolean visit(final SQLBinaryOpExpr x) {
        if (x == null) {
            return false;
        }

        SQLExpr left = x.getLeft();
        if (left != null) {
            this.visitExprByType(left);
        }

        SQLExpr right = x.getRight();
        if (right != null) {
            this.visitExprByType(right);
        }

        return false;
    }

    @Override
    public boolean visit(final SQLCaseExpr x) {
        if (x == null) {
            return false;
        }

        List<SQLCaseExpr.Item> itemExprList = x.getItems();
        if (itemExprList != null && !itemExprList.isEmpty()) {
            for (SQLCaseExpr.Item itemExpr : itemExprList) {
                visit(itemExpr);
            }
        }

        SQLExpr valueExpr = x.getValueExpr();
        if (valueExpr != null) {
            this.visitExprByType(valueExpr);
        }

        SQLExpr elseExpr = x.getElseExpr();
        if (elseExpr != null) {
            this.visitExprByType(elseExpr);
        }

        return false;
    }

    @Override
    public boolean visit(final SQLCaseExpr.Item x) {
        if (x == null) {
            return false;
        }

        SQLExpr conditionExpr = x.getConditionExpr();
        if (conditionExpr != null) {
            this.visitExprByType(conditionExpr);
        }

        SQLExpr valueExpr = x.getValueExpr();
        if (valueExpr != null) {
            this.visitExprByType(valueExpr);
        }

        return false;
    }

    @Override
    public boolean visit(final SQLExprTableSource x) {
        if (x == null) {
            return false;
        }

        SQLExpr tableExpr = x.getExpr();
        if (!(tableExpr instanceof SQLPropertyExpr)) {
            return false;
        }

        SQLExpr schemaExpr = ((SQLPropertyExpr) tableExpr).getOwner();
        if (schemaExpr instanceof SQLIdentifierExpr) {
            String schemaName = ((SQLIdentifierExpr) schemaExpr).getSimpleName();
            if (RoutePlan.systemTable(schemaName)) {
                return false;
            }
            this.replaceSchema((SQLPropertyExpr) tableExpr);
        }

        return false;
    }

    @Override
    public boolean visit(final SQLJoinTableSource x) {
        if (x == null) {
            return false;
        }

        SQLTableSource leftTableSource = x.getLeft();
        if (leftTableSource instanceof SQLExprTableSource) {
            visit((SQLExprTableSource) leftTableSource);
        } else if (leftTableSource instanceof SQLJoinTableSource) {
            visit((SQLJoinTableSource) leftTableSource);
        }

        SQLTableSource rightTableSource = x.getRight();
        if (rightTableSource instanceof SQLExprTableSource) {
            visit((SQLExprTableSource) rightTableSource);
        } else if (rightTableSource instanceof SQLJoinTableSource) {
            visit((SQLJoinTableSource) rightTableSource);
        }

        SQLExpr conditionExpr = x.getCondition();
        if (conditionExpr != null) {
            this.visitExprByType(conditionExpr);
        }

        List<SQLExpr> usingExprList = x.getUsing();
        if (usingExprList != null && !usingExprList.isEmpty()) {
            for (SQLExpr expr : usingExprList) {
                this.visitExprByType(expr);
            }
        }

        return false;
    }

    @Override
    public boolean visit(final SQLInsertStatement.ValuesClause x) {
        if (x == null) {
            return false;
        }

        List<SQLExpr> valueExprList = x.getValues();
        if (valueExprList != null && !valueExprList.isEmpty()) {
            for (SQLExpr expr : valueExprList) {
                this.visitExprByType(expr);
            }
        }

        return false;
    }

    @Override
    public boolean visit(final SQLQueryExpr x) {
        SQLSelect select = x.getSubQuery();
        if (select != null) {
            visit(select);
        }

        return false;
    }

    @Override
    public boolean visit(final SQLUpdateSetItem x) {
        if (x == null) {
            return false;
        }

        SQLExpr columnExpr = x.getColumn();
        if (columnExpr != null) {
            this.visitExprByType(columnExpr);
        }

        SQLExpr valueExpr = x.getValue();
        if (valueExpr != null) {
            this.visitExprByType(valueExpr);
        }

        return false;
    }

    @Override
    public boolean visit(final SQLBetweenExpr x) {
        if (x == null) {
            return false;
        }

        SQLExpr testExpr = x.getTestExpr();
        if (testExpr != null) {
            this.visitExprByType(testExpr);
        }

        SQLExpr beginExpr = x.getBeginExpr();
        if (beginExpr != null) {
            this.visitExprByType(beginExpr);
        }

        SQLExpr endExpr = x.getEndExpr();
        if (endExpr != null) {
            this.visitExprByType(endExpr);
        }

        return false;
    }

    @Override
    public boolean visit(final SQLInListExpr x) {
        if (x == null) {
            return false;
        }

        SQLExpr expr = x.getExpr();
        if (expr != null) {
            this.visitExprByType(expr);
        }

        List<SQLExpr> targetExprList = x.getTargetList();
        if (targetExprList != null && !targetExprList.isEmpty()) {
            for (SQLExpr target : targetExprList) {
                this.visitExprByType(target);
            }
        }

        return false;
    }

    @Override
    public boolean visit(final SQLOrderBy x) {
        if (x == null) {
            return false;
        }

        List<SQLSelectOrderByItem> selectOrderByItemList = x.getItems();
        if (selectOrderByItemList != null && !selectOrderByItemList.isEmpty()) {
            for (SQLSelectOrderByItem selectOrderByItem : selectOrderByItemList) {
                visit(selectOrderByItem);
            }
        }

        return false;
    }

    @Override
    public boolean visit(final SQLSelectOrderByItem x) {
        if (x == null) {
            return false;
        }

        SQLExpr expr = x.getExpr();
        if (expr != null) {
            this.visitExprByType(expr);
        }

        return false;
    }

    @Override
    public boolean visit(final SQLSelectGroupByClause x) {
        if (x == null) {
            return false;
        }

        List<SQLExpr> itemExprList = x.getItems();
        if (itemExprList != null && !itemExprList.isEmpty()) {
            for (SQLExpr expr : itemExprList) {
                this.visitExprByType(expr);
            }
        }

        SQLExpr havingExpr = x.getHaving();
        if (havingExpr != null) {
            this.visitExprByType(havingExpr);
        }

        return false;
    }

    private void visitExprByType(final SQLExpr expr) {
        if (expr == null) {
            return;
        }

        if (expr instanceof SQLPropertyExpr) {
            visit((SQLPropertyExpr) expr);
        } else if (expr instanceof SQLAggregateExpr) {
            visit((SQLAggregateExpr) expr);
        } else if (expr instanceof SQLMethodInvokeExpr) {
            visit((SQLMethodInvokeExpr) expr);
        } else if (expr instanceof SQLBinaryOpExpr) {
            visit((SQLBinaryOpExpr) expr);
        } else if (expr instanceof SQLCaseExpr) {
            visit((SQLCaseExpr) expr);
        } else if (expr instanceof SQLQueryExpr) {
            visit((SQLQueryExpr) expr);
        } else if (expr instanceof SQLBetweenExpr) {
            visit((SQLBetweenExpr) expr);
        } else if (expr instanceof SQLInListExpr) {
            visit((SQLInListExpr) expr);
        }
    }

    private void visitArgumentList(final List<SQLExpr> exprList) {
        if (exprList != null && !exprList.isEmpty()) {
            for (SQLExpr expr : exprList) {
                this.visitExprByType(expr);
            }
        }
    }

    private void replaceSchema(final SQLPropertyExpr tableExpr) {
        String keyspaceInSql = tableExpr.getOwnernName();
        if (this.defaultKeyspace.equalsIgnoreCase(keyspaceInSql)) {
            this.newDefaultKeyspace = SchemaUtil.getRealSchema(this.defaultKeyspace);
        } else {
            this.newDefaultKeyspace = SchemaUtil.getRealSchema(keyspaceInSql);
        }
        this.isReplace = true;
        tableExpr.setOwner(this.newDefaultKeyspace);
    }

    public String getNewDefaultKeyspace() {
        if (this.isReplace) {
            return SchemaUtil.getLogicSchema(this.newDefaultKeyspace);
        }
        this.newDefaultKeyspace = SchemaUtil.getRealSchema(this.defaultKeyspace);
        return SchemaUtil.getLogicSchema(this.newDefaultKeyspace);
    }
}
