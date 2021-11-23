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

import lombok.Data;

@Data
public class Comment {

    private String leading;
    private String trailing;

    // leadingCommentEnd returns the first index after all leading comments, or
    // 0 if there are no leading comments.
    private int leadingCommentEnd(String text) {
        boolean hasComment = false;
        int pos = 0;
        for (; pos < text.length(); ) {
            // Eat up any whitespace. Trailing whitespace will be considered part of
            // the leading comments.
            int nextVisibleOffset = text.substring(pos).equals(" ") ? -1 : text.substring(pos).indexOf(" ") == 0 ? 1 : 0;
            if (nextVisibleOffset < 0) {
                break;
            }
            pos += nextVisibleOffset;
            String remainingText = text.substring(pos);

            // Found visible characters. Look for '/*' at the beginning
            // and '*/' somewhere after that.
            if (remainingText.length() < 4 || !remainingText.substring(0, 2).equalsIgnoreCase("/*") || remainingText.charAt(2) == '!') {
                break;
            }
            int commentLength = 4 + remainingText.substring(2).indexOf("*/");
            if (commentLength < 4) {
                // Missing end comment :/
                break;
            }

            hasComment = true;
            pos += commentLength;
        }

        if (hasComment) {
            return pos;
        }
        return 0;
    }

    // trailingCommentStart returns the first index of trailing comments.
    // If there are no trailing comments, returns the length of the input string.
    private int trailingCommentStart(String text) {
        boolean hasComment = false;
        int reducedLen = text.length();
        for (; reducedLen > 0; ) {
            // Eat up any whitespace. Leading whitespace will be considered part of
            // the trailing comments.
            String subStr = text.substring(0, reducedLen);
            int nextReducedLen = (subStr.equals(" ") ? -1 :
                    subStr.charAt(subStr.length() - 1) == ' ' ? subStr.length() - 2 : subStr.length() - 1) + 1;
            if (nextReducedLen == 0) {
                break;
            }
            reducedLen = nextReducedLen;
            if (reducedLen < 4 || !text.substring(reducedLen - 2, reducedLen).equals("*/")) {
                break;
            }

            // Find the beginning of the comment
            int startCommentPos = text.substring(0, reducedLen - 2).lastIndexOf("/*");
            if (startCommentPos < 0 || text.charAt(startCommentPos + 2) == '!') {
                // Badly formatted sql, or a special /*! comment
                break;
            }

            hasComment = true;
            reducedLen = startCommentPos;
        }

        if (hasComment) {
            return reducedLen;
        }
        return text.length();
    }

    public Comment(String sql) {
        int trailingStart = trailingCommentStart(sql);
        int leadingEnd = leadingCommentEnd(sql.substring(0, trailingStart));

        int firstIdx = sql.charAt(0) == ' ' ? 1 : 0;
        this.leading = sql.substring(firstIdx, leadingEnd);
        int lastIdx = sql.charAt(sql.length() - 1) == ' ' ? 1 : 0;
        lastIdx = sql.length() - 1 - lastIdx;
        if (lastIdx < trailingStart) {
            lastIdx = sql.length() - 1;
        }
        this.trailing = trailingStart >= sql.length() ? "" : sql.substring(trailingStart, lastIdx);
    }

}
