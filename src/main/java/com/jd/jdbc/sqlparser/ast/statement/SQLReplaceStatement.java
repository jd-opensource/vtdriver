/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
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
package com.jd.jdbc.sqlparser.ast.statement;

import com.jd.jdbc.sqlparser.ast.*;
import com.jd.jdbc.sqlparser.ast.expr.SQLQueryExpr;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlInsertReplaceStatement;
import com.jd.jdbc.sqlparser.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SQLReplaceStatement extends SQLStatementImpl implements MySqlInsertReplaceStatement {
    protected boolean lowPriority = false;
    protected boolean delayed = false;

    protected SQLExprTableSource tableSource;
    protected final List<SQLExpr> columns = new ArrayList<>();
    protected List<SQLInsertStatement.ValuesClause> valuesList = new ArrayList<>();
    protected SQLQueryExpr query;


    public SQLName getTableName() {
        if (tableSource == null) {
            return null;
        }

        return (SQLName) tableSource.getExpr();
    }

    public void setTableName(SQLName tableName) {
        this.setTableSource(new SQLExprTableSource(tableName));
    }

    public SQLExprTableSource getTableSource() {
        return tableSource;
    }

    public void setTableSource(SQLExprTableSource tableSource) {
        if (tableSource != null) {
            tableSource.setParent(this);
        }
        this.tableSource = tableSource;
    }

    public List<SQLExpr> getColumns() {
        return columns;
    }

    public void addColumn(SQLExpr column) {
        if (column != null) {
            column.setParent(this);
        }
        this.columns.add(column);
    }

    public boolean isLowPriority() {
        return lowPriority;
    }

    public void setLowPriority(boolean lowPriority) {
        this.lowPriority = lowPriority;
    }

    public boolean isDelayed() {
        return delayed;
    }

    public void setDelayed(boolean delayed) {
        this.delayed = delayed;
    }

    public SQLQueryExpr getQuery() {
        return query;
    }

    public void setQuery(SQLQueryExpr query) {
        if (query != null) {
            query.setParent(this);
        }
        this.query = query;
    }

    public List<SQLInsertStatement.ValuesClause> getValuesList() {
        return valuesList;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, tableSource);
            acceptChild(visitor, columns);
            acceptChild(visitor, valuesList);
            acceptChild(visitor, query);
        }
        visitor.endVisit(this);
    }

    @Override
    public SQLQueryExpr getSelectQuery() {
        return this.getQuery() == null ? new SQLQueryExpr() : this.getQuery();
    }

    @Override
    public boolean isIgnore() {
        return false;
    }

    @Override
    public List<SQLExpr> getDuplicateKeyUpdate() {
        return Collections.emptyList();
    }

    @Override
    public SQLStatement clone() {
        SQLReplaceStatement x = new SQLReplaceStatement();
        x.setAfterSemi(this.afterSemi);
        x.setDbType(this.dbType);

        if (headHints != null) {
            for (SQLCommentHint h : headHints) {
                SQLCommentHint clone = h.clone();
                clone.setParent(x);
                x.headHints.add(clone);
            }
        }

        x.lowPriority = this.lowPriority;
        x.delayed = this.delayed;

        if (this.tableSource != null) {
            x.tableSource = this.tableSource.clone();
        }

        for (SQLInsertStatement.ValuesClause clause : valuesList) {
            x.getValuesList().add(clause.clone());
        }

        for (SQLExpr column : columns) {
            x.addColumn(column.clone());
        }

        if (query != null) {
            x.query = this.query.clone();
        }

        return x;
    }
}
