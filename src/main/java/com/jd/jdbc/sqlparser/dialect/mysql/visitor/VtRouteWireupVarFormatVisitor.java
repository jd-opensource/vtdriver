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

import com.jd.jdbc.planbuilder.Builder;
import com.jd.jdbc.planbuilder.Jointab;
import com.jd.jdbc.planbuilder.RoutePlan;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefExpr;
import java.sql.SQLException;


public class VtRouteWireupVarFormatVisitor extends MySqlASTVisitorAdapter {
    private final RoutePlan rb;

    private final Jointab jt;

    private final Builder bldr;

    public VtRouteWireupVarFormatVisitor(final RoutePlan rb, final Jointab jt, final Builder bldr) {
        this.rb = rb;
        this.jt = jt;
        this.bldr = bldr;
    }

    @Override
    public boolean visit(final SQLIdentifierExpr x) {
        this.format(x);
        return false;
    }

    @Override
    public boolean visit(final SQLPropertyExpr x) {
        this.format(x);
        return false;
    }

    private void format(final SQLName x) {
        try {
            if (!this.rb.isLocal(x)) {
                String joinVar = this.jt.procure(this.bldr, x, this.rb.order());
                SQLVariantRefExpr variantRefExpr = new SQLVariantRefExpr(":" + joinVar);

                SQLObject parent = x.getParent();
                if (!(parent instanceof SQLBinaryOpExpr)) {
                    return;
                }
                SQLExpr left = ((SQLBinaryOpExpr) parent).getLeft();
                if (left.equals(x)) {
                    ((SQLBinaryOpExpr) parent).setLeft(variantRefExpr);
                } else {
                    ((SQLBinaryOpExpr) parent).setRight(variantRefExpr);
                }
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
