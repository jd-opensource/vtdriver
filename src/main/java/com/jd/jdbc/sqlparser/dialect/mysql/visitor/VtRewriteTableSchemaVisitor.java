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

import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.sqlparser.SqlParser;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLMethodInvokeExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefExpr;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqltypes.SqlTypes;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;


public class VtRewriteTableSchemaVisitor extends MySqlASTVisitorAdapter {
    private static final Log LOGGER = LogFactory.getLog(VtRewriteTableSchemaVisitor.class);

    private static final String SQL_NAME_TABLE_SCHEMA = "table_schema";

    private static final String METHOD_NAME_DATABASE = "database";

    private static final String METHOD_NAME_SCHEMA = "schema";

    @Getter
    private SQLException exception;

    @Getter
    private final List<EvalEngine.Expr> tableNameExpressionList;

    public VtRewriteTableSchemaVisitor() {
        this.exception = null;
        this.tableNameExpressionList = new ArrayList<>();
    }

    private Boolean rewriteTableSchema(final SQLName node) {
        if (SQL_NAME_TABLE_SCHEMA.equalsIgnoreCase(node.getSimpleName())) {
            SQLObject parent = node.getParent();
            if (parent instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) parent;
                if (SQLBinaryOperator.Equality.equals(binaryOpExpr.getOperator())
                    && this.shouldRewrite(binaryOpExpr.getRight())) {
                    try {
                        EvalEngine.Expr evalExpr = SqlParser.convert(binaryOpExpr.getRight());
                        this.tableNameExpressionList.add(evalExpr);
                        binaryOpExpr.setRight(new SQLVariantRefExpr(":" + SqlTypes.BV_SCHEMA_NAME));
                    } catch (SQLException e) {
                        this.exception = e;
                        LOGGER.error(e.getMessage());
                        return Boolean.FALSE;
                    }
                }
            }
        }
        return Boolean.TRUE;
    }

    private Boolean shouldRewrite(final SQLExpr expr) {
        if (expr instanceof SQLMethodInvokeExpr) {
            String methodName = ((SQLMethodInvokeExpr) expr).getMethodName();
            // we should not rewrite database() calls against information_schema
            return !(METHOD_NAME_DATABASE.equalsIgnoreCase(methodName)
                || METHOD_NAME_SCHEMA.equalsIgnoreCase(methodName));
        }
        return Boolean.TRUE;
    }

    @Override
    public boolean visit(final SQLIdentifierExpr x) {
        return this.rewriteTableSchema(x);
    }

    @Override
    public boolean visit(final SQLPropertyExpr x) {
        return this.rewriteTableSchema(x);
    }
}
