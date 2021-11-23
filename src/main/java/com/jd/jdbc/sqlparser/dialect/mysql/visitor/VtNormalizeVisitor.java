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

import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.SQLObjectImpl;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.visitor.SQLASTVisitor;
import java.sql.SQLException;
import lombok.Getter;
import lombok.Setter;


@Getter
@Setter
public class VtNormalizeVisitor extends MySqlASTVisitorAdapter {

    private PreVisitor preVisitor;

    private PostVisitor postVisitor;

    private Cursor cursor;

    public VtNormalizeVisitor() {
    }

    public static SQLObject rewrite(final SQLObject current, final PreVisitor preVisitor, final PostVisitor postVisitor) {
        ParentSqlObject parent = new ParentSqlObject(current);
        try {
            VtNormalizeVisitor visitor = new VtNormalizeVisitor();
            visitor.setPreVisitor(preVisitor);
            visitor.setPostVisitor(postVisitor);
            visitor.setCursor(new Cursor());

            visitor.apply(parent, current, (newNode, parent1) -> parent.child = newNode);
        } catch (SQLException e) {
            return parent.child;
        }
        return parent.child;
    }

    private void apply(final SQLObject parent, final SQLObject current, final RelpacerFunc relpacer) throws SQLException {
        if (current == null) {
            return;
        }

        Cursor saved = this.cursor;
        this.cursor.relpacer = relpacer;
        this.cursor.current = current;
        this.cursor.parent = parent;

        if (this.preVisitor != null && !this.preVisitor.visit(this.cursor)) {
            this.cursor = saved;
            return;
        }

        current.accept(this);

        if (this.postVisitor != null && !this.postVisitor.visit(this.cursor)) {
            throw new SQLException();
        }

        this.cursor = saved;
    }

    @Override
    public boolean visit(final MySqlSelectQueryBlock x) {
        try {
            for (SQLSelectItem selectItem : x.getSelectList()) {
                this.apply(x, selectItem, (newNode, parent) -> parent = newNode);
            }
            this.apply(x, x.getWhere(), (newNode, parent) -> parent = newNode);
        } catch (SQLException e) {
            // Ignore
        }
        return false;
    }

    @Override
    public boolean visit(final SQLSelectItem x) {
        try {
            this.apply(x, x.getExpr(), (newNode, parent) -> ((SQLSelectItem) parent).setExpr((SQLExpr) newNode));
        } catch (SQLException e) {
            // Ignore
        }
        return false;
    }

    @Override
    public boolean visit(final SQLBinaryOpExpr x) {
        try {
            this.apply(x, x.getLeft(), (newNode, parent) -> ((SQLBinaryOpExpr) parent).setLeft((SQLExpr) newNode));
            this.apply(x, x.getRight(), (newNode, parent) -> ((SQLBinaryOpExpr) parent).setRight((SQLExpr) newNode));
        } catch (SQLException e) {
            // Ignore
        }
        return false;
    }

    public interface PreVisitor {
        Boolean visit(Cursor cursor);
    }

    public interface PostVisitor {
        Boolean visit(Cursor cursor);
    }

    @Getter
    public static class Cursor {
        private SQLObject parent;

        private RelpacerFunc relpacer;

        private SQLObject current;

        public void replace(final SQLObject newNode) {
            this.relpacer.replace(newNode, this.parent);
        }
    }

    public interface RelpacerFunc {
        void replace(SQLObject newNode, SQLObject parent);
    }

    private static class ParentSqlObject extends SQLObjectImpl implements SQLObject {
        private SQLObject child;

        ParentSqlObject(final SQLObject child) {
            this.child = child;
        }

        @Override
        protected void accept0(final SQLASTVisitor visitor) {
        }
    }
}
