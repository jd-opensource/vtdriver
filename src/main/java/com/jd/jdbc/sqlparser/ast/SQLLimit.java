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

package com.jd.jdbc.sqlparser.ast;

import com.jd.jdbc.sqlparser.ast.expr.SQLIntegerExpr;
import com.jd.jdbc.sqlparser.visitor.SQLASTVisitor;

public final class SQLLimit extends SQLObjectImpl {

    public SQLLimit() {

    }

    public SQLLimit(SQLExpr rowCount) {
        this.setRowCount(rowCount);
    }

    public SQLLimit(SQLExpr offset, SQLExpr rowCount) {
        this.setOffset(offset);
        this.setRowCount(rowCount);
    }

    private SQLExpr rowCount;
    private SQLExpr offset;
    private boolean isLimitOffset;

    public SQLExpr getRowCount() {
        return rowCount;
    }

    public void setRowCount(SQLExpr rowCount) {
        if (rowCount != null) {
            rowCount.setParent(this);
        }
        this.rowCount = rowCount;
    }

    public void setRowCount(int rowCount) {
        this.setRowCount(new SQLIntegerExpr(rowCount));
    }

    public SQLExpr getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.setOffset(new SQLIntegerExpr(offset));
    }

    public void setOffset(SQLExpr offset) {
        if (offset != null) {
            offset.setParent(this);
        }
        this.offset = offset;
    }

    public boolean isLimitOffset() {
        return isLimitOffset;
    }

    public void setLimitOffset(boolean limitOffset) {
        isLimitOffset = limitOffset;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (isLimitOffset) {
                acceptChild(visitor, rowCount);
                acceptChild(visitor, offset);
            } else {
                acceptChild(visitor, offset);
                acceptChild(visitor, rowCount);
            }
        }
        visitor.endVisit(this);
    }

    @Override
    public SQLLimit clone() {
        SQLLimit x = new SQLLimit();

        if (offset != null) {
            x.setOffset(offset.clone());
        }

        if (rowCount != null) {
            x.setRowCount(rowCount.clone());
        }

        x.setLimitOffset(isLimitOffset);

        return x;
    }

}
