/*
Copyright 2021 JD Project Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.jd.jdbc.sqlparser.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SplitMultiQueryUtils {
    private static final char DEFAULT_CHAR = '0';

    private static final Set<String> startCommentSet;

    static {
        startCommentSet = new HashSet();
        startCommentSet.add("/*");
        startCommentSet.add("#");
        startCommentSet.add("--");
        startCommentSet.add("//");
    }

    public static List<String> splitMulti(String sql) {
        ArrayList<String> sqls = new ArrayList<>();
        int beginPos = 0;
        while (true) {
            String nextSql = getNextSql(sql, beginPos);
            if (StringUtils.isEmpty(nextSql)) {
                return sqls;
            }
            sqls.add(nextSql);
            beginPos += nextSql.length();
        }
    }

    private static String getNextSql(String sql, int beginPos) {
        int sqlLength = sql.length();
        boolean escape = false;
        char symbol = DEFAULT_CHAR;
        if (beginPos >= sqlLength) {
            return null;
        }
        int startStmtPos = getStatementBeginIndex(sql, beginPos);
        if (startStmtPos >= sqlLength) {
            return null;
        }
        for (int i = startStmtPos; i < sqlLength; i++) {
            char var = sql.charAt(i);
            switch (var) {
                case ';':
                    if (symbol == DEFAULT_CHAR) {
                        return sql.substring(beginPos, i + 1);
                    }
                    escape = false;
                    break;
                case '\'':
                case '"':
                    if (!escape) {
                        if (symbol == var) {
                            symbol = DEFAULT_CHAR;
                        } else if (symbol == DEFAULT_CHAR) {
                            symbol = var;
                        }
                    }
                    escape = false;
                    break;
                case '\\':
                    escape = !escape;
                    break;
                default:
                    escape = false;
                    break;
            }
        }
        return sql.substring(beginPos, sqlLength);
    }

    /**
     * @param sql
     * @param beginPos
     * @return the position of the first character after the comment(skip whitespace)
     */
    public static int getStatementBeginIndex(String sql, int beginPos) {
        int sqlLength = sql.length();
        for (; beginPos < sqlLength; beginPos++) {
            if (!Character.isWhitespace(sql.charAt(beginPos))) {
                break;
            }
        }
        int statementBeginIndex = beginPos;
        do {
            beginPos = statementBeginIndex;
            switch (getCommentStart(sql, beginPos)) {
                case "/*":
                    statementBeginIndex = sql.indexOf("*/", beginPos + 2);
                    if (statementBeginIndex == -1) {
                        statementBeginIndex = beginPos;
                    } else {
                        statementBeginIndex += 2;
                    }
                    break;
                case "#":
                case "//":
                case "--":
                    statementBeginIndex = sql.indexOf('\n', beginPos + 1);
                    if (statementBeginIndex == -1) {
                        statementBeginIndex = sql.indexOf('\r', beginPos + 1);
                        if (statementBeginIndex == -1) {
                            statementBeginIndex = beginPos;
                        }
                    }
                    break;
                default:
                    break;
            }
            for (; statementBeginIndex < sqlLength; statementBeginIndex++) {
                if (!Character.isWhitespace(sql.charAt(statementBeginIndex))) {
                    break;
                }
            }
        } while (statementBeginIndex != beginPos);
        return statementBeginIndex;
    }

    private static String getCommentStart(String sql, int beginPos) {
        for (String startComment : startCommentSet) {
            if (StringUtils.startsWith(sql, startComment, beginPos)) {
                return startComment;
            }
        }
        return "";
    }
}
