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
package com.jd.jdbc.sqlparser.parser;

import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQueryBlock;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.dialect.mysql.parser.MySqlExprParser;
import com.jd.jdbc.sqlparser.dialect.mysql.parser.MySqlLexer;
import com.jd.jdbc.sqlparser.dialect.mysql.parser.MySqlStatementParser;
import com.jd.jdbc.sqlparser.utils.JdbcConstants;
import com.jd.jdbc.sqlparser.utils.JdbcUtils;
import com.jd.jdbc.sqltypes.VtSqlStatementType;
import java.sql.SQLSyntaxErrorException;

public class SQLParserUtils {

    public static SQLStatementParser createSQLStatementParser(String sql, String dbType) {
        SQLParserFeature[] features;
        if (JdbcConstants.MYSQL.equals(dbType)) {
            features = new SQLParserFeature[]{SQLParserFeature.KeepComments};
        } else {
            features = new SQLParserFeature[]{};
        }
        return createSQLStatementParser(sql, dbType, features);
    }

    public static SQLStatementParser createSQLStatementParser(String sql, String dbType, boolean keepComments) {
        SQLParserFeature[] features;
        if (keepComments) {
            features = new SQLParserFeature[]{SQLParserFeature.KeepComments};
        } else {
            features = new SQLParserFeature[]{};
        }

        return createSQLStatementParser(sql, dbType, features);
    }

    public static SQLStatementParser createSQLStatementParser(String sql, String dbType, SQLParserFeature... features) {

        if (JdbcUtils.MYSQL.equals(dbType)) {
            return new MySqlStatementParser(sql, features);
        }

        return new SQLStatementParser(sql, dbType);
    }

    public static SQLExprParser createExprParser(String sql, String dbType) {
        if (JdbcUtils.MYSQL.equals(dbType) || //
                JdbcUtils.MARIADB.equals(dbType)) {
            return new MySqlExprParser(sql);
        }

        return new SQLExprParser(sql);
    }

    public static Lexer createLexer(String sql, String dbType) {
        if (JdbcUtils.MYSQL.equals(dbType) || //
                JdbcUtils.MARIADB.equals(dbType)) {
            return new MySqlLexer(sql);
        }
        return new Lexer(sql);
    }

    public static SQLSelectQueryBlock createSelectQueryBlock(String dbType) {
        if (JdbcConstants.MYSQL.equals(dbType)) {
            return new MySqlSelectQueryBlock();
        }
        return new SQLSelectQueryBlock();
    }

    // Preview analyzes the beginning of the query using a simpler and faster
    // textual comparison to identify the statement type.
    public static VtSqlStatementType preview(final String sql) {
        String trimmed = stripLeadingComment(sql);

        if (trimmed.indexOf("/*!") == 0) {
            return VtSqlStatementType.StmtComment;
        }

        String firstWord = getFirstWord(trimmed).trim();
        firstWord = firstWord.toLowerCase();
        // Comparison is done in order of priority.
        switch (firstWord) {
            case "select":
                return VtSqlStatementType.StmtSelect;
            case "stream":
                return VtSqlStatementType.StmtStream;
            case "insert":
                return VtSqlStatementType.StmtInsert;
            case "replace":
                return VtSqlStatementType.StmtReplace;
            case "update":
                return VtSqlStatementType.StmtUpdate;
            case "delete":
                return VtSqlStatementType.StmtDelete;
            case "savepoint":
                return VtSqlStatementType.StmtSavepoint;
            case "plan":
                return VtSqlStatementType.StmtPlan;
        }

        // don't consider tail comments now
        // For the following statements it is not sufficient to rely
        // on loweredFirstWord. This is because they are not statements
        // in the grammar and we are relying on Preview to parse them.
        // For instance, we don't want: "BEGIN JUNK" to be parsed
        // as StmtBegin.
        switch (trimmed.toLowerCase()) {
            case "begin":
            case "start transaction":
                return VtSqlStatementType.StmtBegin;
            case "commit":
                return VtSqlStatementType.StmtCommit;
            case "rollback":
                return VtSqlStatementType.StmtRollback;
        }
        switch (firstWord) {
            case "create":
            case "alter":
            case "rename":
            case "drop":
            case "truncate":
            case "flush":
                return VtSqlStatementType.StmtDDL;
            case "set":
                return VtSqlStatementType.StmtSet;
            case "show":
                return VtSqlStatementType.StmtShow;
            case "use":
                return VtSqlStatementType.StmtUse;
            case "describe":
            case "desc":
            case "explain":
                return VtSqlStatementType.StmtExplain;
            case "analyze":
            case "repair":
            case "optimize":
                return VtSqlStatementType.StmtOther;
            case "grant":
            case "revoke":
                return VtSqlStatementType.StmtPriv;
            case "release":
                return VtSqlStatementType.StmtRelease;
            case "rollback":
                return VtSqlStatementType.StmtSRollback;
        }
        return VtSqlStatementType.StmtUnknown;
    }

    private static String getFirstWord(final String sentence) {
        String trimSentence = sentence.trim();
        int index = trimSentence.trim().indexOf(' ');
        if (index == -1) {
            return trimSentence;
        }
        return trimSentence.substring(0, index);
    }

    public static String getShowType(final String sentence) throws SQLSyntaxErrorException {
        String commentSentence = stripLeadingComment(sentence);
        String firstWord = getFirstWord(commentSentence);
        if (!firstWord.equalsIgnoreCase("show")) {
            throw new SQLSyntaxErrorException("this is not show statement, sql:" + sentence);
        }

        int secondWordStart = commentSentence.indexOf(firstWord) + 5;
        String restString = commentSentence.substring(secondWordStart).trim();
        String type = getFirstWord(restString).trim();
        if (type.isEmpty()) {
            throw new SQLSyntaxErrorException("expect show type after keyword show, sql:" + sentence);
        }
        String[] types = type.split(";");
        if (types.length > 1) {
            throw new SQLSyntaxErrorException("expect show type after keyword show, sql:" + sentence);
        }
        return types[0];
    }

    public static String stripLeadingComment(final String sql) {
        String stripSql = sql.trim();

        while (hasCommentPrefix(stripSql)) {
            switch (stripSql.charAt(1)) {
                case '*': {
                    // Multi line comment
                    int index = stripSql.indexOf("*/");
                    if (index <= 1) {
                        return stripSql;
                    }
                    // don't strip /*! ... */ or /*!50700 ... */
                    if (stripSql.length() > 2 && stripSql.charAt(3) == '!') {
                        return stripSql;
                    }
                    stripSql = stripSql.substring(index + 2);
                    break;
                }
                case '/': {
                    // Single line comment
                    int index = stripSql.indexOf("\n");
                    if (index <= 1) {
                        return stripSql;
                    }
                    stripSql = stripSql.substring(index + 1);
                }
            }
        }
        return stripSql;
    }

    public static boolean hasCommentPrefix(final String sql) {
        return sql.length() > 1 && ((sql.charAt(0) == '/' && sql.charAt(1) == '*') || (sql.charAt(0) == '/' && sql.charAt(1) == '/'));
    }

}
