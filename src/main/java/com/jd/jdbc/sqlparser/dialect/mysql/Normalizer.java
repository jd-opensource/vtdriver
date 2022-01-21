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

package com.jd.jdbc.sqlparser.dialect.mysql;

import com.google.protobuf.ByteString;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.expr.SQLInListExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntegerExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLLiteralExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLMethodInvokeExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNumberExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLTextLiteralExpr;
import com.jd.jdbc.sqlparser.ast.expr.VtArgument;
import com.jd.jdbc.sqlparser.ast.expr.VtListArgument;
import com.jd.jdbc.sqlparser.ast.statement.SQLCommitStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLDDLStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLDescribeStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLExplainStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLReleaseSavePointStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLRollbackStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSavePointStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSetStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLStartTransactionStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLTruncateStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlOptimizeStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSetTransactionStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlShowStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtNormalizeVisitor;
import com.jd.jdbc.sqltypes.SqlTypes;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.srvtopo.BindVariable;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Setter;


@Setter
public class Normalizer {
    private Map<String, BindVariable> bindVars;

    private String prefix;

    private Set<String> reserved;

    private Integer counter;

    private Map<String, String> vals;

    private Normalizer() {
    }

    private static Normalizer newNormalizer(SQLStatement stmt, Map<String, BindVariable> bindvars, String prefix) {
        Normalizer normalizer = new Normalizer();
        normalizer.setBindVars(bindvars);
        normalizer.setPrefix(prefix);
        normalizer.setReserved(null);
        normalizer.setCounter(1);
        normalizer.setVals(new HashMap<>(16, 1));
        return normalizer;
    }

    public static void normalize(SQLStatement stmt, Map<String, BindVariable> bindvars, String prefix) {
        Normalizer nz = newNormalizer(stmt, bindvars, prefix);
        VtNormalizeVisitor.rewrite(stmt, nz::walkStatement, null);
    }

    /**
     * WalkStatement is the top level walk function.
     * If it encounters a Select, it switches to a mode
     * where variables are deduped.
     *
     * @param cursor
     * @return
     */
    public Boolean walkStatement(VtNormalizeVisitor.Cursor cursor) {
        SQLObject current = cursor.getCurrent();
        // no need to normalize the statement types
        if (current instanceof SQLSetStatement
            || current instanceof MySqlShowStatement
            || current instanceof SQLStartTransactionStatement
            || current instanceof SQLCommitStatement
            || current instanceof SQLRollbackStatement
            || current instanceof SQLSavePointStatement
            || current instanceof MySqlSetTransactionStatement
            || current instanceof SQLDDLStatement
            || current instanceof SQLReleaseSavePointStatement
            || current instanceof MySqlOptimizeStatement
            || current instanceof SQLTruncateStatement
            || current instanceof SQLDescribeStatement
            || current instanceof SQLExplainStatement) {
            return Boolean.FALSE;
        } else if (current instanceof SQLSelectStatement) {
            VtNormalizeVisitor.rewrite(current, this::walkSelect, null);
            // Don't continue
            return Boolean.FALSE;
        } else if (current instanceof SQLLiteralExpr) {
            this.convertLiteral((SQLLiteralExpr) current, cursor);
        } else if (current instanceof SQLInListExpr) {
            this.convertComparison((SQLInListExpr) current);
        } else if (current instanceof SQLName
            || current instanceof SQLExprTableSource) {
            // Common node types that never contain Literal or ListArgs but create a lot of object
            // allocations.
            return Boolean.FALSE;
        } else if (current instanceof SQLMethodInvokeExpr) {
            String methodName = ((SQLMethodInvokeExpr) current).getMethodName();
            if ("CONVERT".equalsIgnoreCase(methodName)) {
                return Boolean.FALSE;
            }
        }
        return Boolean.TRUE;
    }

    /**
     * WalkSelect normalizes the AST in Select mode.
     *
     * @param cursor
     * @return
     */
    public Boolean walkSelect(VtNormalizeVisitor.Cursor cursor) {
        SQLObject current = cursor.getCurrent();
        if (current instanceof SQLLiteralExpr) {
            this.convertLiteralDedup((SQLLiteralExpr) current, cursor);
        } else if (current instanceof SQLInListExpr) {
            this.convertComparison((SQLInListExpr) current);
        } else if (current instanceof SQLName) {
            // Common node types that never contain Literal or ListArgs but create a lot of object
            // allocations.
            return Boolean.FALSE;
        } else if (current instanceof SQLOrderBy
            || current instanceof SQLSelectGroupByClause) {
            // do not make a bind var for order by column_position
            return Boolean.FALSE;
        } else if (current instanceof SQLMethodInvokeExpr) {
            // we should not rewrite the type description
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    private void convertLiteral(SQLLiteralExpr node, VtNormalizeVisitor.Cursor cursor) {
        BindVariable bindVariable = this.sqlToBindVar(node);
        if (bindVariable == null) {
            return;
        }

        String bindVarName = this.newName();
        this.bindVars.put(bindVarName, bindVariable);

        cursor.replace(new VtArgument((":" + bindVarName).getBytes()));
    }

    private void convertLiteralDedup(SQLLiteralExpr node, VtNormalizeVisitor.Cursor cursor) {
        // If value is too long, don't dedup.
        // Such values are most likely not for vindexes.
        // We save a lot of CPU because we avoid building
        // the key for them.
        if (node instanceof SQLTextLiteralExpr) {
            if (((SQLTextLiteralExpr) node).getText().getBytes().length > 256) {
                this.convertLiteral(node, cursor);
                return;
            }
        }

        // Make the bindvar
        BindVariable bindVariable = this.sqlToBindVar(node);
        if (bindVariable == null) {
            return;
        }

        // Check if there's a bindvar for that value already.
        String key;
        if (Query.Type.VARBINARY.equals(bindVariable.getType())) {
            // Prefixing strings with "'" ensures that a string
            // and number that have the same representation don't
            // collide.
            key = "'" + new String(bindVariable.getValue());
        } else {
            key = new String(bindVariable.getValue());
        }
        String bindVarName;
        if (this.vals.containsKey(key)) {
            bindVarName = this.vals.get(key);
        } else {
            // If there's no such bindvar, make a new one.
            bindVarName = this.newName();
            this.vals.put(key, bindVarName);
            this.bindVars.put(bindVarName, bindVariable);
        }

        // Modify the AST node to a bindvar.
        cursor.replace(new VtArgument((":" + bindVarName).getBytes()));
    }

    private void convertComparison(SQLInListExpr node) {
        List<SQLExpr> targetList = node.getTargetList();
        // The RHS is a tuple of values.
        // Make a list bindvar.
        List<Query.Value> valueList = new ArrayList<>(targetList.size());
        for (SQLExpr expr : targetList) {
            BindVariable bval = this.sqlToBindVar((SQLLiteralExpr) expr);
            if (bval == null) {
                return;
            }
            Query.Value value = Query.Value.newBuilder().setType(bval.getType()).setValue(ByteString.copyFrom(bval.getValue())).build();
            valueList.add(value);
        }
        BindVariable bindVariable = new BindVariable(valueList, Query.Type.TUPLE);
        String bindVarName = this.newName();
        this.bindVars.put(bindVarName, bindVariable);
        // Modify RHS to be a list bindvar.
        node.setTargetList(new ArrayList<SQLExpr>() {{
            add(new VtListArgument(("::" + bindVarName).getBytes()));
        }});
    }

    private BindVariable sqlToBindVar(SQLLiteralExpr node) {
        VtValue vtValue;
        try {
            if (node instanceof SQLTextLiteralExpr) {
                vtValue = VtValue.newVtValue(Query.Type.VARBINARY, ((SQLTextLiteralExpr) node).getText().getBytes());
            } else if (node instanceof SQLIntegerExpr) {
                vtValue = VtValue.newVtValue(Query.Type.INT64, String.valueOf(((SQLIntegerExpr) node).getNumber().longValue()).getBytes());
            } else if (node instanceof SQLNumberExpr) {
                vtValue = VtValue.newVtValue(Query.Type.FLOAT64, String.valueOf(((SQLNumberExpr) node).getNumber().floatValue()).getBytes());
            } else {
                return null;
            }
        } catch (SQLException e) {
            return null;
        }
        return SqlTypes.valueBindVariable(vtValue);
    }

    private String newName() {
        while (true) {
            String newName = this.prefix + this.counter;
            if (!this.reserved.contains(newName)) {
                this.reserved.add(newName);
                return newName;
            }
            this.counter++;
        }
    }
}
