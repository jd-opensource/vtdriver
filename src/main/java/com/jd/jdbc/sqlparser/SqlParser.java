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

package com.jd.jdbc.sqlparser;

import com.google.common.collect.Sets;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.expr.SQLAllColumnExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLArrayExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBooleanExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLCharExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLDefaultExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLExistsExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLHexExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLInListExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLInSubQueryExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntegerExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLListExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLLiteralExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNCharExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNullExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNumberExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLUnaryExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLUnaryOperator;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefListExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLCommitStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLDeleteStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLRollbackStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelect;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSetStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLStartTransactionStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLUpdateStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.BindVarNeeds;
import com.jd.jdbc.sqlparser.dialect.mysql.SmartNormalizer;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlInsertReplaceStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.parser.MySqlLexer;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.CheckNodeTypesVisitor;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtGetBindVarsVisitor;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtReplaceExprVisitor;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtReplaceVariantRefExprVisitor;
import com.jd.jdbc.sqlparser.parser.Keywords;
import com.jd.jdbc.sqlparser.parser.Token;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqltypes.VtPlanValue;
import com.jd.jdbc.sqltypes.VtSqlStatementType;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.srvtopo.BindVariable;
import io.netty.util.internal.StringUtil;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Getter;

public class SqlParser {
    public static final String WHERE_STR = "where";

    public static final String HAVING_STR = "having";

    public static final String STAR_STR = "*";

    public static final Map<String, Boolean> AGGREGATES = new HashMap<String, Boolean>() {
        {
            put("avg", true);
            put("bit_and", true);
            put("bit_or", true);
            put("bit_xor", true);
            put("count", true);
            put("group_concat", true);
            put("max", true);
            put("min", true);
            put("std", true);
            put("stddev_pop", true);
            put("stddev", true);
            put("sum", true);
            put("var_pop", true);
            put("var_samp", true);
            put("variance", true);
        }
    };

    private static final Log log = LogFactory.getLog(SqlParser.class);

    public static final Keywords MYSQL_KEYWORDS;

    static {
        Map<String, Token> map = new HashMap<>();

        map.putAll(MySqlLexer.DEFAULT_MYSQL_KEYWORDS.getKeywords());

        map.put("BIT", Token.DATATYPE);
        map.put("BOOL", Token.DATATYPE);
        map.put("TINYINT", Token.DATATYPE);
        map.put("SMALLINT", Token.DATATYPE);
        map.put("MEDIUMINT", Token.DATATYPE);
        map.put("INT", Token.DATATYPE);
        map.put("INTEGER", Token.DATATYPE);
        map.put("NUMERIC", Token.DATATYPE);
        map.put("BIGINT", Token.DATATYPE);
        map.put("FLOAT", Token.DATATYPE);
        map.put("DOUBLE", Token.DATATYPE);
        map.put("DECIMAL", Token.DATATYPE);


        map.put("DATE", Token.DATATYPE);
        map.put("TIME", Token.DATATYPE);
        map.put("YEAR", Token.DATATYPE);
        map.put("DATETIME", Token.DATATYPE);
        map.put("TIMESTAMP", Token.DATATYPE);

        map.put("CHAR", Token.DATATYPE);
        map.put("VARCHAR", Token.DATATYPE);
        map.put("TINYBLOB", Token.DATATYPE);
        map.put("TINYTEXT", Token.DATATYPE);
        map.put("BLOB", Token.DATATYPE);
        map.put("TEXT", Token.DATATYPE);
        map.put("MEDIUMBLOB", Token.DATATYPE);
        map.put("MEDIUMTEXT", Token.DATATYPE);
        map.put("LONGBLOB", Token.DATATYPE);
        map.put("LONGTEXT", Token.DATATYPE);

        map.put("BINARY", Token.DATATYPE);
        map.put("VARBINARY", Token.DATATYPE);
        map.put("ENUM", Token.DATATYPE);
        map.put("GEOMETRY", Token.DATATYPE);
        map.put("POINT", Token.DATATYPE);
        map.put("MULTIPOINT", Token.DATATYPE);
        map.put("LINESTRING", Token.DATATYPE);
        map.put("MULTILINESTRING", Token.DATATYPE);
        map.put("POLYGON", Token.DATATYPE);
        map.put("GEOMETRYCOLLECTION", Token.DATATYPE);

        MYSQL_KEYWORDS = new Keywords(map);
    }

    /**
     * @param stmt
     * @return
     */
    public static VtSqlStatementType astToStatementType(final SQLStatement stmt) {
        if (stmt instanceof SQLSelectStatement) {
            return VtSqlStatementType.StmtSelect;
        } else if (stmt instanceof MySqlInsertReplaceStatement) {
            return VtSqlStatementType.StmtInsert;
        } else if (stmt instanceof SQLUpdateStatement) {
            return VtSqlStatementType.StmtUpdate;
        } else if (stmt instanceof SQLSetStatement) {
            return VtSqlStatementType.StmtSet;
        } else if (stmt instanceof SQLDeleteStatement) {
            return VtSqlStatementType.StmtDelete;
        } else if (stmt instanceof SQLStartTransactionStatement) {
            return VtSqlStatementType.StmtBegin;
        } else if (stmt instanceof SQLCommitStatement) {
            return VtSqlStatementType.StmtCommit;
        } else if (stmt instanceof SQLRollbackStatement) {
            return VtSqlStatementType.StmtRollback;
        }
        return VtSqlStatementType.StmtUnknown;
    }

    /**
     * IsValue returns true if the Expr is a string, integral or value arg.
     * NULL is not considered to be a value.
     *
     * @param expr
     * @return
     */
    public static Boolean isValue(final SQLExpr expr) {
        if (expr instanceof SQLVariantRefExpr) {
            return true;
        } else if (expr instanceof SQLLiteralExpr) {
            return expr instanceof SQLCharExpr
                || expr instanceof SQLNCharExpr
                || expr instanceof SQLHexExpr
                || expr instanceof SQLIntegerExpr;
        }
        return false;
    }

    public static boolean isSimpleTuple(final List<SQLExpr> sqlExprs) {
        for (SQLExpr expr : sqlExprs) {
            if (!isValue(expr)) {
                return false;
            }
        }
        return true;
    }

    /**
     * IsSimpleTuple returns true if the Expr is a ValTuple that
     * contains simple values or if it's a list arg.
     *
     * @param expr
     * @return
     */
    public static Boolean isSimpleTuple(final SQLExpr expr) {
        if (expr instanceof SQLArrayExpr) {
            for (SQLExpr value : ((SQLArrayExpr) expr).getValues()) {
                if (!isValue(value)) {
                    return false;
                }
            }
            return true;
        } else {
            return expr instanceof SQLListExpr;
        }
        // It's a subquery
    }

    public static Boolean isComparison(final SQLExpr expr) {
        if (expr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr operator = (SQLBinaryOpExpr) expr;
            return operator.getOperator().isRelational();
        } else {
            return false;
        }
    }

    public static EvalEngine.Expr convert(final SQLExpr expr) throws SQLException {
        if (expr instanceof SQLVariantRefExpr) {
            int index = ((SQLVariantRefExpr) expr).getIndex();
            return new EvalEngine.BindVariable(String.valueOf(index));
        } else if (expr instanceof SQLIntegerExpr) {
            return EvalEngine.newLiteralIntFromBytes(((SQLIntegerExpr) expr).getNumber().toString().getBytes());
        } else if (expr instanceof SQLNumberExpr) {
            return EvalEngine.newLiteralFloat(((SQLNumberExpr) expr).getNumber().toString().getBytes());
        } else if (expr instanceof SQLCharExpr) {
            return EvalEngine.newLiteralString(((SQLCharExpr) expr).getText().getBytes());
        } else if (expr instanceof SQLBooleanExpr) {
            return EvalEngine.newLiteralIntFromBytes("1".getBytes());
        } else if (expr instanceof SQLBinaryOpExpr) {
            EvalEngine.BinaryExpr op;
            switch (((SQLBinaryOpExpr) expr).getOperator()) {
                case Add:
                    op = new EvalEngine.Addition();
                    break;
                case Subtract:
                    op = new EvalEngine.Subtraction();
                    break;
                case Multiply:
                    op = new EvalEngine.Multiplication();
                    break;
                case Divide:
                    op = new EvalEngine.Division();
                    break;
                default:
                    throw new SQLException("Expr Not Supported");
            }
            EvalEngine.Expr left = convert(((SQLBinaryOpExpr) expr).getLeft());
            EvalEngine.Expr right = convert(((SQLBinaryOpExpr) expr).getRight());
            return new EvalEngine.BinaryOp(op, left, right);
        }
        throw new SQLException("Expr Not Supported");

    }

    public static Boolean canNormalize(final SQLStatement stmt) {
        return stmt instanceof SQLSelectStatement || stmt instanceof MySqlInsertReplaceStatement
            || stmt instanceof SQLUpdateStatement || stmt instanceof SQLDeleteStatement;
    }

    /**
     * PrepareAST will normalize the query
     *
     * @param stmt
     * @param bindVariableMap
     * @return
     * @throws SQLException
     */
    public static PrepareAstResult prepareAst(final SQLStatement stmt, final Map<String, BindVariable> bindVariableMap, String charEncoding) throws SQLException {
        SmartNormalizer.SmartNormalizerResult normalizerResult = SmartNormalizer.normalize(stmt, bindVariableMap, charEncoding);
        RewriteAstResult rewriteAstResult = rewriteAst(normalizerResult.getStmt(), normalizerResult, charEncoding);
        return new PrepareAstResult(rewriteAstResult.getAst(), rewriteAstResult.getBindVarNeeds(), normalizerResult.getBindVariableMap());
    }

    /**
     * RewriteAST rewrites the whole AST, replacing function calls and adding column aliases to queries
     *
     * @param stmt
     * @param normalizerResult
     * @return
     * @throws SQLException
     */
    public static RewriteAstResult rewriteAst(final SQLStatement stmt, final SmartNormalizer.SmartNormalizerResult normalizerResult, String charEncoding) throws SQLException {
        VtReplaceVariantRefExprVisitor replaceVariantRefExprVisitor = new VtReplaceVariantRefExprVisitor();
        stmt.accept(replaceVariantRefExprVisitor);
        if (!normalizerResult.getQuery().equals(SQLUtils.toMySqlString(stmt, SQLUtils.NOT_FORMAT_OPTION))) {
            log.error("Rewrite SQL error!!!");
            log.error("Original SQL: " + normalizerResult.getQuery());
            log.error("Rewrited SQL: " + SQLUtils.toMySqlString(stmt, SQLUtils.NOT_FORMAT_OPTION));
            throw new SQLException("Rewrite SQL error!");
        }
        Map<String, BindVariable> bindVariableMap = normalizerResult.getBindVariableMap();
        if (stmt instanceof SQLSelectStatement) {
            SQLSelectQuery selectQuery = ((SQLSelectStatement) stmt).getSelect().getQuery();
            CheckNodeTypesVisitor visitor = new CheckNodeTypesVisitor(Sets.newHashSet(CheckNodeTypesVisitor.CheckNodeType.SUBQUERY, CheckNodeTypesVisitor.CheckNodeType.UNION));
            selectQuery.accept(visitor);
            if (visitor.getCheckResult()) {
                bindVariableMap = combineRepeatBindVars(replaceVariantRefExprVisitor.getVarRefList(), normalizerResult.getBindVariableMap(), charEncoding);
            }
        }
        normalizerResult.setBindVariableMap(bindVariableMap);
        return new RewriteAstResult(stmt, new BindVarNeeds());
    }

    /**
     * @param varRefList
     * @param originBindVariableMap
     * @return
     */
    private static Map<String, BindVariable> combineRepeatBindVars(final List<SQLVariantRefExpr> varRefList, final Map<String, BindVariable> originBindVariableMap, String charEncoding)
        throws SQLException {
        Map<String, Integer> vals = new HashMap<>();
        Map<String, BindVariable> bindVariableMap = new HashMap<>();
        int refIndex = 0;
        for (SQLVariantRefExpr varRef : varRefList) {
            int originKey = varRef.getIndex();
            if (!originBindVariableMap.containsKey(String.valueOf(originKey))) {
                throw new SQLException(String.format("Missing bind variable, missing key %d", originKey));
            }
            BindVariable bindVar = originBindVariableMap.get(String.valueOf(originKey));
            String valueStr;
            if (bindVar.getType() == Query.Type.VARBINARY) {
                String s = StringUtils.toString(bindVar.getValue(), charEncoding);
                valueStr = s + "::" + bindVar.getType();
            } else {
                valueStr = new String(bindVar.getValue(), 0, bindVar.getValue().length) + "::" + bindVar.getType();
            }

            if (vals.containsKey(valueStr)) {
                int key = vals.get(valueStr);
                varRef.setName("?" + key);
                varRef.setIndex(key);
            } else {
                vals.put(valueStr, refIndex);
                varRef.setName("?" + refIndex);
                varRef.setIndex(refIndex);
                bindVariableMap.put(String.valueOf(refIndex), bindVar);
                refIndex++;
            }
        }
        return bindVariableMap;
    }

    /**
     * GetBindvars returns a map of the bind vars referenced in the statement.
     * Ideally, this should be done only once.
     *
     * @param stmt
     * @return
     */
    public static Set<String> getBindVars(final SQLStatement stmt) {
        VtGetBindVarsVisitor visitor = new VtGetBindVarsVisitor();
        stmt.accept(visitor);
        return visitor.getBindVars();
    }

    /**
     * ReplaceExpr finds the from expression from root
     * and replaces it with to. If from matches root,
     * then to is returned.
     *
     * @param root
     * @param from
     * @param to
     * @return
     */
    public static SQLExpr replaceSelect(final SQLExpr root, final SQLSelect from, final SQLExpr to) {
        if (root instanceof SQLInSubQueryExpr) {
            if (((SQLInSubQueryExpr) root).getSubQuery() == from) {
                SQLInListExpr inListExpr = new SQLInListExpr(((SQLInSubQueryExpr) root).getExpr(), ((SQLInSubQueryExpr) root).isNot());
                inListExpr.setParent(root.getParent());
                inListExpr.setTargetList(new ArrayList<SQLExpr>() {
                                             {
                                                 add(to);
                                             }
                                         }
                );
                return inListExpr;
            }
        } else if (root instanceof SQLBinaryOpExpr) {
            VtReplaceExprVisitor visitor = new VtReplaceExprVisitor(from, to);
            root.accept(visitor);
        }
        return root;
    }

    public static SQLExpr replaceInSubQueryExpr(final SQLInSubQueryExpr root, final SQLInSubQueryExpr from, final SQLExpr to) {
        if (root == from) {
            return to;
        }
        return root;
    }

    public static SQLExpr replaceExistsExpr(final SQLExistsExpr root, final SQLExistsExpr from, final SQLExpr to) {
        if (root.getSubQuery() == from.getSubQuery()) {
            return to;
        }
        return root;
    }

    public static VtPlanValue newPlanValue(final List<SQLExpr> nodes) throws SQLException {
        VtPlanValue vtPlanValue = new VtPlanValue();
        for (SQLExpr node : nodes) {
            VtPlanValue innerPlanValue = newPlanValue(node);
            if (innerPlanValue.getListKey() != null && !innerPlanValue.getListKey().isEmpty()) {
                throw new SQLException("unsupported: nested lists");
            }
            vtPlanValue.getVtPlanValueList().add(innerPlanValue);
        }
        return vtPlanValue;
    }

    public static VtPlanValue newPlanValue(final SQLExpr node) throws SQLException {
        if (node instanceof SQLVariantRefListExpr) {
            String nodeName = StringUtils.replaceEach(((SQLVariantRefExpr) node).getName(), new String[] {":"}, new String[] {""});
            VtPlanValue vtPlanValue = new VtPlanValue();
            vtPlanValue.setListKey(nodeName);
            return vtPlanValue;
        } else if (node instanceof SQLVariantRefExpr) {
            String nodeName = ((SQLVariantRefExpr) node).getName();
            if (nodeName.startsWith(":")) {
                return new VtPlanValue(StringUtils.replaceEach(nodeName, new String[] {":"}, new String[] {""}));
            }
            int index = ((SQLVariantRefExpr) node).getIndex();
            return new VtPlanValue(String.valueOf(index));
        } else if (node instanceof SQLCharExpr) {
            String text = ((SQLCharExpr) node).getText();
            return new VtPlanValue(VtValue.newVtValue(Query.Type.VARCHAR, text.getBytes()));
        } else if (node instanceof SQLIntegerExpr) {
            return new VtPlanValue(VtValue.newVtValue(Query.Type.INT64, ((SQLIntegerExpr) node).getValue().toString().getBytes()));
        } else if (node instanceof SQLNumberExpr) {
            return new VtPlanValue(VtValue.newVtValue(Query.Type.FLOAT64, String.valueOf(((SQLNumberExpr) node).getValue()).getBytes()));
        } else if (node instanceof SQLHexExpr) {
            String hex = new String(((SQLHexExpr) node).getValue());
            return new VtPlanValue(VtValue.newVtValue(Query.Type.VARBINARY, hex.getBytes()));
        } else if (node instanceof SQLArrayExpr) {
            SQLArrayExpr sqlArrayExpr = (SQLArrayExpr) node;
            VtPlanValue pv = new VtPlanValue(new ArrayList<>(sqlArrayExpr.getValues().size() - 1));
            for (SQLExpr sqlExpr : sqlArrayExpr.getValues()) {
                VtPlanValue innerPv;
                try {
                    innerPv = newPlanValue(sqlExpr);
                } catch (SQLException e) {
                    return new VtPlanValue();
                }
                if (!StringUtil.isNullOrEmpty(innerPv.getListKey())) {
                    throw new SQLException("unsupported: nested lists");
                } else if (innerPv.getVtPlanValueList() != null && !innerPv.getVtPlanValueList().isEmpty()) {
                    throw new SQLException("unsupported: nested lists");
                }
                pv.getVtPlanValueList().add(innerPv);
            }
            return pv;
        } else if (node instanceof SQLListExpr) {
            return new VtPlanValue(node.toString().substring(2));
        } else if (node instanceof SQLDefaultExpr) {
            return new VtPlanValue();
        } else if (node instanceof SQLNullExpr) {
            return new VtPlanValue();
        } else if (node instanceof SQLUnaryExpr) {
            // for some charset introducers, we can just ignore them
            SQLUnaryExpr unaryExpr = (SQLUnaryExpr) node;
            if (unaryExpr.getOperator() == SQLUnaryOperator.BINARY
                || unaryExpr.getOperator() == SQLUnaryOperator.Utf8mb4Str
                || unaryExpr.getOperator() == SQLUnaryOperator.Utf8Str
                || unaryExpr.getOperator() == SQLUnaryOperator.Latin1Str) {
                return newPlanValue(unaryExpr.getExpr());
            }
        }
        throw new SQLException("expression is too complex " + node.toString());
    }

    /***/
    public enum SelectExpr {
        /***/
        AliasedExpr, StarExpr, Nextval;

        /**
         * @param selectItem
         * @return
         */
        public static SelectExpr type(final SQLSelectItem selectItem) {
            SQLExpr expr = selectItem.getExpr();
            if (expr instanceof SQLAllColumnExpr) {
                return SelectExpr.StarExpr;
            }
            if (expr instanceof SQLPropertyExpr) {
                if (STAR_STR.equals(((SQLPropertyExpr) expr).getName())) {
                    return SelectExpr.StarExpr;
                }
            }

            return SelectExpr.AliasedExpr;
        }
    }

    /***/
    public enum GroupByExpr {
        /***/
        ColName, Literal, OtherGBExpr;

        /**
         * @param sqlExpr
         * @return
         */
        public static GroupByExpr type(final SQLExpr sqlExpr) {
            if (sqlExpr instanceof SQLAllColumnExpr) {
                return OtherGBExpr;
            } else if (sqlExpr instanceof SQLPropertyExpr) {
                if (STAR_STR.equals(((SQLPropertyExpr) sqlExpr).getName())) {
                    return OtherGBExpr;
                }
            } else if (sqlExpr instanceof SQLLiteralExpr) {
                return GroupByExpr.Literal;
            }

            return ColName;
        }
    }

    @Getter
    @AllArgsConstructor
    public static class RewriteAstResult {
        private final SQLStatement ast;

        private final BindVarNeeds bindVarNeeds;
    }

    @Getter
    @AllArgsConstructor
    public static class PrepareAstResult {
        private final SQLStatement ast;

        private final BindVarNeeds bindVarNeeds;

        private final Map<String, BindVariable> bindVariableMap;
    }
}
