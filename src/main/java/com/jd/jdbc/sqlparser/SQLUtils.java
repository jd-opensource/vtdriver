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

import com.jd.jdbc.sqlparser.ast.*;
import com.jd.jdbc.sqlparser.ast.expr.*;
import com.jd.jdbc.sqlparser.ast.statement.*;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.MySqlOutputVisitor;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.jd.jdbc.sqlparser.parser.*;
import com.jd.jdbc.sqlparser.utils.*;
import com.jd.jdbc.sqlparser.visitor.SQLASTOutputVisitor;
import com.jd.jdbc.sqlparser.visitor.SchemaStatVisitor;
import com.jd.jdbc.sqlparser.visitor.VisitorFeature;

import java.util.List;
import java.util.Map;

public class SQLUtils {
    private final static SQLParserFeature[] FORMAT_DEFAULT_FEATURES = {
            SQLParserFeature.KeepComments,
            SQLParserFeature.EnableSQLBinaryOpExprGroup
    };

    public static FormatOption DEFAULT_FORMAT_OPTION = new FormatOption(true, true);
    public static FormatOption DEFAULT_LCASE_FORMAT_OPTION = new FormatOption(false, true);
    public static FormatOption NOT_FORMAT_OPTION = new FormatOption(false, false);

    public static String toSQLString(SQLObject sqlObject, String dbType) {
        return toSQLString(sqlObject, dbType, null);
    }

    public static String toSQLString(SQLObject sqlObject, String dbType, FormatOption option) {
        StringBuilder out = new StringBuilder();
        SQLASTOutputVisitor visitor = createOutputVisitor(out, dbType);

        if (option == null) {
            option = DEFAULT_FORMAT_OPTION;
        }
        visitor.setUppCase(option.isUppCase());
        visitor.setPrettyFormat(option.isPrettyFormat());
        visitor.setParameterized(option.isParameterized());
        visitor.setFeatures(option.features);

        sqlObject.accept(visitor);

        String sql = out.toString();
        return sql;
    }

    public static String toSQLString(SQLObject sqlObject) {
        StringBuilder out = new StringBuilder();
        sqlObject.accept(new SQLASTOutputVisitor(out));

        String sql = out.toString();
        return sql;
    }

    public static String toMySqlString(SQLObject sqlObject) {
        return toMySqlString(sqlObject, (FormatOption) null);
    }

    public static String toMySqlString(SQLObject sqlObject, VisitorFeature... features) {
        return toMySqlString(sqlObject, new FormatOption(features));
    }

    public static String toMySqlString(SQLObject sqlObject, FormatOption option) {
        return toSQLString(sqlObject, JdbcConstants.MYSQL, option);
    }

    public static SQLExpr toMySqlExpr(String sql) {
        return toSQLExpr(sql, JdbcConstants.MYSQL);
    }

    public static String formatMySql(String sql) {
        return format(sql, JdbcConstants.MYSQL);
    }

    public static String formatMySql(String sql, FormatOption option) {
        return format(sql, JdbcConstants.MYSQL, option);
    }

    public static String toPGString(SQLObject sqlObject) {
        return toPGString(sqlObject, null);
    }

    public static String toPGString(SQLObject sqlObject, FormatOption option) {
        return toSQLString(sqlObject, JdbcConstants.POSTGRESQL, option);
    }

    public static String formatPGSql(String sql, FormatOption option) {
        return format(sql, JdbcConstants.POSTGRESQL, option);
    }

    public static SQLExpr toSQLExpr(String sql, String dbType) {
        SQLExprParser parser = SQLParserUtils.createExprParser(sql, dbType);
        SQLExpr expr = parser.expr();

        if (parser.getLexer().token() != Token.EOF) {
            throw new ParserException("illegal sql expr : " + sql);
        }

        return expr;
    }

    public static SQLSelectOrderByItem toOrderByItem(String sql, String dbType) {
        SQLExprParser parser = SQLParserUtils.createExprParser(sql, dbType);
        SQLSelectOrderByItem orderByItem = parser.parseSelectOrderByItem();

        if (parser.getLexer().token() != Token.EOF) {
            throw new ParserException("illegal sql expr : " + sql);
        }

        return orderByItem;
    }

    public static SQLUpdateSetItem toUpdateSetItem(String sql, String dbType) {
        SQLExprParser parser = SQLParserUtils.createExprParser(sql, dbType);
        SQLUpdateSetItem updateSetItem = parser.parseUpdateSetItem();

        if (parser.getLexer().token() != Token.EOF) {
            throw new ParserException("illegal sql expr : " + sql);
        }

        return updateSetItem;
    }

    public static SQLSelectItem toSelectItem(String sql, String dbType) {
        SQLExprParser parser = SQLParserUtils.createExprParser(sql, dbType);
        SQLSelectItem selectItem = parser.parseSelectItem();

        if (parser.getLexer().token() != Token.EOF) {
            throw new ParserException("illegal sql expr : " + sql);
        }

        return selectItem;
    }

    public static List<SQLStatement> toStatementList(String sql, String dbType) {
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType);
        return parser.parseStatementList();
    }

    public static SQLExpr toSQLExpr(String sql) {
        return toSQLExpr(sql, null);
    }

    public static String format(String sql, String dbType) {
        return format(sql, dbType, null, null);
    }

    public static String format(String sql, String dbType, FormatOption option) {
        return format(sql, dbType, null, option);
    }

    public static String format(String sql, String dbType, List<Object> parameters) {
        return format(sql, dbType, parameters, null);
    }

    public static String format(String sql, String dbType, List<Object> parameters, FormatOption option) {
        try {
            SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType, FORMAT_DEFAULT_FEATURES);
            List<SQLStatement> statementList = parser.parseStatementList();
            return toSQLString(statementList, dbType, parameters, option);
        } catch (ClassCastException ex) {
            return sql;
        } catch (ParserException ex) {
            return sql;
        }
    }

    public static String toSQLString(List<SQLStatement> statementList, String dbType) {
        return toSQLString(statementList, dbType, (List<Object>) null);
    }

    public static String toSQLString(List<SQLStatement> statementList, String dbType, FormatOption option) {
        return toSQLString(statementList, dbType, null, option);
    }

    public static String toSQLString(List<SQLStatement> statementList, String dbType, List<Object> parameters) {
        return toSQLString(statementList, dbType, parameters, null, null);
    }

    public static String toSQLString(List<SQLStatement> statementList, String dbType, List<Object> parameters, FormatOption option) {
        return toSQLString(statementList, dbType, parameters, option, null);
    }

    public static String toSQLString(List<SQLStatement> statementList
            , String dbType
            , List<Object> parameters
            , FormatOption option
            , Map<String, String> tableMapping) {
        StringBuilder out = new StringBuilder();
        SQLASTOutputVisitor visitor = createFormatOutputVisitor(out, statementList, dbType);
        if (parameters != null) {
            visitor.setInputParameters(parameters);
        }

        if (option == null) {
            option = DEFAULT_FORMAT_OPTION;
        }
        visitor.setFeatures(option.features);

        if (tableMapping != null) {
            visitor.setTableMapping(tableMapping);
        }

        boolean printStmtSeperator = true;
        for (int i = 0, size = statementList.size(); i < size; i++) {
            SQLStatement stmt = statementList.get(i);

            if (i > 0) {
                SQLStatement preStmt = statementList.get(i - 1);
                if (printStmtSeperator && !preStmt.isAfterSemi()) {
                    visitor.print(";");
                }

                List<String> comments = preStmt.getAfterCommentsDirect();
                if (comments != null) {
                    for (int j = 0; j < comments.size(); ++j) {
                        String comment = comments.get(j);
                        if (j != 0) {
                            visitor.println();
                        }
                        visitor.printComment(comment);
                    }
                }

                if (printStmtSeperator) {
                    visitor.println();
                }

                if (!(stmt instanceof SQLSetStatement)) {
                    visitor.println();
                }
            }
            {
                List<String> comments = stmt.getBeforeCommentsDirect();
                if (comments != null) {
                    for (String comment : comments) {
                        visitor.printComment(comment);
                        visitor.println();
                    }
                }
            }
            stmt.accept(visitor);

            if (i == size - 1) {
                List<String> comments = stmt.getAfterCommentsDirect();
                if (comments != null) {
                    for (int j = 0; j < comments.size(); ++j) {
                        String comment = comments.get(j);
                        if (j != 0) {
                            visitor.println();
                        }
                        visitor.printComment(comment);
                    }
                }
            }
        }

        return out.toString();
    }

    public static SQLASTOutputVisitor createOutputVisitor(Appendable out, String dbType) {
        return createFormatOutputVisitor(out, null, dbType);
    }

    public static SQLASTOutputVisitor createFormatOutputVisitor(Appendable out, //
                                                                List<SQLStatement> statementList, //
                                                                String dbType) {
        if (JdbcConstants.MYSQL.equals(dbType) //
                || JdbcConstants.MARIADB.equals(dbType)) {
            return new MySqlOutputVisitor(out);
        }

        return new SQLASTOutputVisitor(out, dbType);
    }

    @Deprecated
    public static SchemaStatVisitor createSchemaStatVisitor(List<SQLStatement> statementList, String dbType) {
        return createSchemaStatVisitor(dbType);
    }

    public static SchemaStatVisitor createSchemaStatVisitor(String dbType) {
        if (JdbcConstants.MYSQL.equals(dbType) || //
                JdbcConstants.MARIADB.equals(dbType)) {
            return new MySqlSchemaStatVisitor();
        }
        return new SchemaStatVisitor();
    }

    public static List<SQLStatement> parseStatements(String sql, String dbType) {
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType);
        List<SQLStatement> stmtList = parser.parseStatementList();
        if (parser.getLexer().token() != Token.EOF) {
            throw new ParserException("syntax error : " + sql);
        }
        return stmtList;
    }

    public static List<SQLStatement> parseStatements(String sql, String dbType, boolean keepComments) {
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType, keepComments);
        List<SQLStatement> stmtList = parser.parseStatementList();
        if (parser.getLexer().token() != Token.EOF) {
            throw new ParserException("syntax error. " + sql);
        }
        return stmtList;
    }

    public static SQLStatement parseSingleStatement(String sql, String dbType, SQLParserFeature... features) {

        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType, features);
        List<SQLStatement> stmtList = parser.parseStatementList();

        if (stmtList.size() > 1) {
            throw new ParserException(" Mutil-Statment be found.");
        }

        if (parser.getLexer().token() != Token.EOF) {
            throw new ParserException("syntax error. " + sql);
        }
        return stmtList.get(0);
    }

    public static SQLStatement parseSingleMysqlStatement(String sql) {
        return parseSingleStatement(sql, JdbcConstants.MYSQL);
    }

    /**
     * @param columnName
     * @param tableAlias
     * @param pattern    if pattern is null,it will be set {%Y-%m-%d %H:%i:%s} as mysql default value and set {yyyy-mm-dd
     *                   hh24:mi:ss} as oracle default value
     * @param dbType     {@link JdbcConstants} if dbType is null ,it will be set the mysql as a default value
     * @author owenludong.lud
     */
    public static String buildToDate(String columnName, String tableAlias, String pattern, String dbType) {
        StringBuilder sql = new StringBuilder();
        if (StringUtils.isEmpty(columnName)) {
            return "";
        }
        if (StringUtils.isEmpty(dbType)) {
            dbType = JdbcConstants.MYSQL;
        }
        String formatMethod = "";
        if (JdbcConstants.MYSQL.equalsIgnoreCase(dbType)) {
            formatMethod = "STR_TO_DATE";
            if (StringUtils.isEmpty(pattern)) {
                pattern = "%Y-%m-%d %H:%i:%s";
            }
        } else {
            return "";
            // expand date's handle method for other database
        }
        sql.append(formatMethod).append("(");
        if (!StringUtils.isEmpty(tableAlias)) {
            sql.append(tableAlias).append(".");
        }
        sql.append(columnName).append(",");
        sql.append("'");
        sql.append(pattern);
        sql.append("')");
        return sql.toString();
    }

    public static List<SQLExpr> split(SQLBinaryOpExpr x) {
        return SQLBinaryOpExpr.split(x);
    }

    public static String addCondition(String sql, String condition, String dbType) {
        String result = addCondition(sql, condition, SQLBinaryOperator.BooleanAnd, false, dbType);
        return result;
    }

    public static String addCondition(String sql, String condition, SQLBinaryOperator op, boolean left, String dbType) {
        if (sql == null) {
            throw new IllegalArgumentException("sql is null");
        }

        if (condition == null) {
            return sql;
        }

        if (op == null) {
            op = SQLBinaryOperator.BooleanAnd;
        }

        if (op != SQLBinaryOperator.BooleanAnd //
                && op != SQLBinaryOperator.BooleanOr) {
            throw new IllegalArgumentException("add condition not support : " + op);
        }

        List<SQLStatement> stmtList = parseStatements(sql, dbType);

        if (stmtList.size() == 0) {
            throw new IllegalArgumentException("not support empty-statement :" + sql);
        }

        if (stmtList.size() > 1) {
            throw new IllegalArgumentException("not support multi-statement :" + sql);
        }

        SQLStatement stmt = stmtList.get(0);

        SQLExpr conditionExpr = toSQLExpr(condition, dbType);

        addCondition(stmt, op, conditionExpr, left);

        return toSQLString(stmt, dbType);
    }

    public static void addCondition(SQLStatement stmt, SQLBinaryOperator op, SQLExpr condition, boolean left) {
        if (stmt instanceof SQLSelectStatement) {
            SQLSelectQuery query = ((SQLSelectStatement) stmt).getSelect().getQuery();
            if (query instanceof SQLSelectQueryBlock) {
                SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock) query;
                SQLExpr newCondition = buildCondition(op, condition, left, queryBlock.getWhere());
                queryBlock.setWhere(newCondition);
            } else {
                throw new IllegalArgumentException("add condition not support " + stmt.getClass().getName());
            }

            return;
        }

        if (stmt instanceof SQLDeleteStatement) {
            SQLDeleteStatement delete = (SQLDeleteStatement) stmt;

            SQLExpr newCondition = buildCondition(op, condition, left, delete.getWhere());
            delete.setWhere(newCondition);

            return;
        }

        if (stmt instanceof SQLUpdateStatement) {
            SQLUpdateStatement update = (SQLUpdateStatement) stmt;

            SQLExpr newCondition = buildCondition(op, condition, left, update.getWhere());
            update.setWhere(newCondition);

            return;
        }

        throw new IllegalArgumentException("add condition not support " + stmt.getClass().getName());
    }

    public static SQLExpr buildCondition(SQLBinaryOperator op, SQLExpr condition, boolean left, SQLExpr where) {
        if (where == null) {
            return condition;
        }

        SQLBinaryOpExpr newCondition;
        if (left) {
            newCondition = new SQLBinaryOpExpr(condition, op, where);
        } else {
            newCondition = new SQLBinaryOpExpr(where, op, condition);
        }
        return newCondition;
    }

    public static String addSelectItem(String selectSql, String expr, String alias, String dbType) {
        return addSelectItem(selectSql, expr, alias, false, dbType);
    }

    public static String addSelectItem(String selectSql, String expr, String alias, boolean first, String dbType) {
        List<SQLStatement> stmtList = parseStatements(selectSql, dbType);

        if (stmtList.size() == 0) {
            throw new IllegalArgumentException("not support empty-statement :" + selectSql);
        }

        if (stmtList.size() > 1) {
            throw new IllegalArgumentException("not support multi-statement :" + selectSql);
        }

        SQLStatement stmt = stmtList.get(0);

        SQLExpr columnExpr = toSQLExpr(expr, dbType);

        addSelectItem(stmt, columnExpr, alias, first);

        return toSQLString(stmt, dbType);
    }

    public static void addSelectItem(SQLStatement stmt, SQLExpr expr, String alias, boolean first) {
        if (expr == null) {
            return;
        }

        if (stmt instanceof SQLSelectStatement) {
            SQLSelectQuery query = ((SQLSelectStatement) stmt).getSelect().getQuery();
            if (query instanceof SQLSelectQueryBlock) {
                SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock) query;
                addSelectItem(queryBlock, expr, alias, first);
            } else {
                throw new IllegalArgumentException("add condition not support " + stmt.getClass().getName());
            }

            return;
        }

        throw new IllegalArgumentException("add selectItem not support " + stmt.getClass().getName());
    }

    public static void addSelectItem(SQLSelectQueryBlock queryBlock, SQLExpr expr, String alias, boolean first) {
        SQLSelectItem selectItem = new SQLSelectItem(expr, alias);
        queryBlock.getSelectList().add(selectItem);
        selectItem.setParent(selectItem);
    }

    public static class FormatOption {
        private int features = VisitorFeature.of(VisitorFeature.OutputUCase
                , VisitorFeature.OutputPrettyFormat);

        public FormatOption() {

        }

        public FormatOption(VisitorFeature... features) {
            this.features = VisitorFeature.of(features);
        }

        public FormatOption(boolean ucase) {
            this(ucase, true);
        }

        public FormatOption(boolean ucase, boolean prettyFormat) {
            this(ucase, prettyFormat, false);
        }

        public FormatOption(boolean ucase, boolean prettyFormat, boolean parameterized) {
            this.features = VisitorFeature.config(this.features, VisitorFeature.OutputUCase, ucase);
            this.features = VisitorFeature.config(this.features, VisitorFeature.OutputPrettyFormat, prettyFormat);
            this.features = VisitorFeature.config(this.features, VisitorFeature.OutputParameterized, parameterized);
        }

        public boolean isDesensitize() {
            return isEnabled(VisitorFeature.OutputDesensitize);
        }

        public void setDesensitize(boolean val) {
            config(VisitorFeature.OutputDesensitize, val);
        }

        public boolean isUppCase() {
            return isEnabled(VisitorFeature.OutputUCase);
        }

        public void setUppCase(boolean val) {
            config(VisitorFeature.OutputUCase, val);
        }

        public boolean isPrettyFormat() {
            return isEnabled(VisitorFeature.OutputPrettyFormat);
        }

        public void setPrettyFormat(boolean prettyFormat) {
            config(VisitorFeature.OutputPrettyFormat, prettyFormat);
        }

        public boolean isParameterized() {
            return isEnabled(VisitorFeature.OutputParameterized);
        }

        public void setParameterized(boolean parameterized) {
            config(VisitorFeature.OutputParameterized, parameterized);
        }

        public void config(VisitorFeature feature, boolean state) {
            features = VisitorFeature.config(features, feature, state);
        }

        public final boolean isEnabled(VisitorFeature feature) {
            return VisitorFeature.isEnabled(this.features, feature);
        }
    }

    public static String refactor(String sql, String dbType, Map<String, String> tableMapping) {
        List<SQLStatement> stmtList = parseStatements(sql, dbType);
        return SQLUtils.toSQLString(stmtList, dbType, null, null, tableMapping);
    }

    public static long hash(String sql, String dbType) {
        Lexer lexer = SQLParserUtils.createLexer(sql, dbType);

        StringBuilder buf = new StringBuilder(sql.length());

        for (; ; ) {
            lexer.nextToken();

            Token token = lexer.token();
            if (token == Token.EOF) {
                break;
            }

            if (token == Token.ERROR) {
                return Utils.fnv_64(sql);
            }

            if (buf.length() != 0) {

            }
        }

        return buf.hashCode();
    }

    public static SQLExpr not(SQLExpr expr) {
        if (expr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) expr;
            SQLBinaryOperator op = binaryOpExpr.getOperator();

            SQLBinaryOperator notOp = null;

            switch (op) {
                case Equality:
                    notOp = SQLBinaryOperator.LessThanOrGreater;
                    break;
                case LessThanOrEqualOrGreaterThan:
                    notOp = SQLBinaryOperator.Equality;
                    break;
                case LessThan:
                    notOp = SQLBinaryOperator.GreaterThanOrEqual;
                    break;
                case LessThanOrEqual:
                    notOp = SQLBinaryOperator.GreaterThan;
                    break;
                case GreaterThan:
                    notOp = SQLBinaryOperator.LessThanOrEqual;
                    break;
                case GreaterThanOrEqual:
                    notOp = SQLBinaryOperator.LessThan;
                    break;
                case Is:
                    notOp = SQLBinaryOperator.IsNot;
                    break;
                case IsNot:
                    notOp = SQLBinaryOperator.Is;
                    break;
                default:
                    break;
            }


            if (notOp != null) {
                return new SQLBinaryOpExpr(binaryOpExpr.getLeft(), notOp, binaryOpExpr.getRight());
            }
        }

        if (expr instanceof SQLInListExpr) {
            SQLInListExpr inListExpr = (SQLInListExpr) expr;

            SQLInListExpr newInListExpr = new SQLInListExpr(inListExpr);
            newInListExpr.getTargetList().addAll(inListExpr.getTargetList());
            newInListExpr.setNot(!inListExpr.isNot());
            return newInListExpr;
        }

        return new SQLUnaryExpr(SQLUnaryOperator.Not, expr);
    }

    public static String normalize(String name) {
        return normalize(name, null);
    }

    public static String normalize(String name, String dbType) {
        if (name == null) {
            return null;
        }

        if (name.length() > 2) {
            char c0 = name.charAt(0);
            char x0 = name.charAt(name.length() - 1);
            if ((c0 == '"' && x0 == '"') || (c0 == '`' && x0 == '`')) {
                String normalizeName = name.substring(1, name.length() - 1);
                if (c0 == '`') {
                    normalizeName = normalizeName.replaceAll("`\\.`", ".");
                }

                if (JdbcConstants.MYSQL.equals(dbType)) {
                    if (MySqlUtils.isKeyword(normalizeName)) {
                        return name;
                    }
                }

                return normalizeName;
            }
        }

        return name;
    }

    public static boolean nameEquals(SQLName a, SQLName b) {
        if (a == b) {
            return true;
        }

        if (a == null || b == null) {
            return false;
        }

        return a.nameHashCode64() == b.nameHashCode64();
    }

    public static boolean nameEquals(String a, String b) {
        if (a == b) {
            return true;
        }

        if (a == null || b == null) {
            return false;
        }

        if (a.equalsIgnoreCase(b)) {
            return true;
        }

        String normalize_a = normalize(a);
        String normalize_b = normalize(b);

        return normalize_a.equalsIgnoreCase(normalize_b);
    }

    public static boolean isValue(SQLExpr expr) {
        if (expr instanceof SQLLiteralExpr) {
            return true;
        }

        if (expr instanceof SQLVariantRefExpr) {
            return true;
        }

        if (expr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) expr;
            SQLBinaryOperator op = binaryOpExpr.getOperator();
            if (op == SQLBinaryOperator.Add
                    || op == SQLBinaryOperator.Subtract
                    || op == SQLBinaryOperator.Multiply) {
                return isValue(binaryOpExpr.getLeft())
                        && isValue(binaryOpExpr.getRight());
            }
        }

        return false;
    }

    public static boolean replaceInParent(SQLExpr expr, SQLExpr target) {
        if (expr == null) {
            return false;
        }

        SQLObject parent = expr.getParent();

        if (parent instanceof SQLReplaceable) {
            return ((SQLReplaceable) parent).replace(expr, target);
        }

        return false;
    }

    public static String desensitizeTable(String tableName) {
        if (tableName == null) {
            return null;
        }

        tableName = normalize(tableName);
        long hash = FnvHash.hashCode64(tableName);
        return Utils.hex_t(hash);
    }

    /**
     * 重新排序建表语句，解决建表语句的依赖关系
     *
     * @param sql
     * @param dbType
     * @return
     */
    public static String sort(String sql, String dbType) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcConstants.ORACLE);
        SQLCreateTableStatement.sort(stmtList);
        return SQLUtils.toSQLString(stmtList, dbType);
    }
}

