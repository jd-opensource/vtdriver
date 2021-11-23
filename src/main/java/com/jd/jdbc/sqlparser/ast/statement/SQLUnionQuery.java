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

package com.jd.jdbc.sqlparser.ast.statement;

import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLLimit;
import com.jd.jdbc.sqlparser.ast.SQLObjectImpl;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.visitor.SQLASTOutputVisitor;
import com.jd.jdbc.sqlparser.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class SQLUnionQuery extends SQLObjectImpl implements SQLSelectQuery {

    private boolean bracket = false;

    private SQLSelectQuery left;
    private SQLSelectQuery right;
    private SQLUnionOperator operator = SQLUnionOperator.UNION;
    private SQLOrderBy orderBy;

    private SQLLimit limit;
    private String dbType;

    private SQLSelectQuery firstStatement;
    private final List<UnionSelect> unionSelectList = new ArrayList<>();

    private void rebuild(SQLSelectQuery selectQuery, SQLUnionOperator operator) {
        if (selectQuery instanceof SQLUnionQuery) {
            if (selectQuery.isBracket()) {
                if (this.firstStatement == null) {
                    this.firstStatement = selectQuery;
                } else {
                    this.unionSelectList.add(new UnionSelect(selectQuery, operator));
                }
            } else {
                rebuild(((SQLUnionQuery) selectQuery).getLeft(), ((SQLUnionQuery) selectQuery).getOperator());
                rebuild(((SQLUnionQuery) selectQuery).getRight(), ((SQLUnionQuery) selectQuery).getOperator());
            }
        } else if (this.firstStatement == null) {
            this.firstStatement = selectQuery;
        } else {
            this.unionSelectList.add(new UnionSelect(selectQuery, operator));
        }
    }

    public SQLUnionOperator getOperator() {
        return operator;
    }

    public void setOperator(SQLUnionOperator operator) {
        this.operator = operator;
    }

    public SQLUnionQuery() {

    }

    public SQLUnionQuery(SQLSelectQuery left, SQLUnionOperator operator, SQLSelectQuery right) {
        this.setLeft(left);
        this.operator = operator;
        this.setRight(right);
    }

    public SQLSelectQuery getLeft() {
        return left;
    }

    public void setLeft(SQLSelectQuery left) {
        if (left != null) {
            left.setParent(this);
        }
        this.left = left;
        this.rebuild(this.left, this.operator);
    }

    public SQLUnionQuery addUnion(SQLUnionQuery.UnionSelect unionQuery) {
        return new SQLUnionQuery(this, unionQuery.getOperator(), unionQuery.getSelectQuery());
    }

    public SQLSelectQuery getRight() {
        return right;
    }

    public void setRight(SQLSelectQuery right) {
        if (right != null) {
            right.setParent(this);
        }
        this.right = right;
        this.rebuild(this.right, this.operator);
    }

    public SQLOrderBy getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(SQLOrderBy orderBy) {
        if (orderBy != null) {
            orderBy.setParent(this);
        }
        this.orderBy = orderBy;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, left);
            acceptChild(visitor, right);
            acceptChild(visitor, orderBy);
            acceptChild(visitor, limit);
        }
        visitor.endVisit(this);
    }


    public SQLLimit getLimit() {
        return limit;
    }

    public void setLimit(SQLLimit limit) {
        if (limit != null) {
            limit.setParent(this);
        }
        this.limit = limit;
    }

    @Override
    public boolean isBracket() {
        return bracket;
    }

    @Override
    public void setBracket(boolean bracket) {
        this.bracket = bracket;
    }

    @Override
    public SQLUnionQuery clone() {
        SQLUnionQuery x = new SQLUnionQuery();

        x.bracket = bracket;
        if (left != null) {
            x.setLeft(left.clone());
        }
        if (right != null) {
            x.setRight(right.clone());
        }
        x.operator = operator;

        if (orderBy != null) {
            x.setOrderBy(orderBy.clone());
        }

        if (limit != null) {
            x.setLimit(limit.clone());
        }

        x.dbType = dbType;

        return x;
    }

    public SQLSelectQueryBlock getFirstQueryBlock() {
        if (left instanceof SQLSelectQueryBlock) {
            return (SQLSelectQueryBlock) left;
        }

        if (left instanceof SQLUnionQuery) {
            return ((SQLUnionQuery) left).getFirstQueryBlock();
        }

        return null;
    }

    public SQLSelectQuery getFirstStatement() {
        return this.firstStatement;
    }

    public List<UnionSelect> getUnionSelectList() {
        return this.unionSelectList;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    @Override
    public void output(StringBuffer buf) {
        SQLASTOutputVisitor visitor = SQLUtils.createOutputVisitor(buf, dbType);
        this.accept(visitor);
    }

    public static class UnionSelect extends SQLObjectImpl implements SQLSelectQuery {
        private final SQLSelectQuery selectQuery;
        private final SQLUnionOperator operator;

        public UnionSelect(SQLSelectQuery selectQuery, SQLUnionOperator operator) {
            this.selectQuery = selectQuery;
            this.operator = operator;
        }

        public SQLSelectQuery getSelectQuery() {
            return selectQuery;
        }

        public SQLUnionOperator getOperator() {
            return operator;
        }

        @Override
        public boolean isBracket() {
            return false;
        }

        @Override
        public void setBracket(boolean bracket) {
        }

        @Override
        protected void accept0(SQLASTVisitor visitor) {
        }

        @Override
        public SQLSelectQuery clone() {
            return null;
        }
    }
}
