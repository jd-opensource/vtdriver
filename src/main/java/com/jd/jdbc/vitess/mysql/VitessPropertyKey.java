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

package com.jd.jdbc.vitess.mysql;

public enum VitessPropertyKey {
    USER("user"),
    PASSWORD("password"),
    CHARACTER_ENCODING("characterEncoding"),
    SERVER_TIMEZONE("serverTimezone"),
    MAX_ALLOWED_PACKET("maxAllowedPacket"),
    SEND_FRACTIONAL_SECONDS("sendFractionalSeconds"),
    TREAT_UTIL_DATE_AS_TIMESTAMP("treatUtilDateAsTimestamp"),
    USE_STREAM_LENGTHS_IN_PREP_STMTS("useStreamLengthsInPrepStmts"),
    AUTO_CLOSE_P_STMT_STREAMS("autoClosePStmtStreams"),
    SOCKET_TIMEOUT("socketTimeout"),
    ALLOW_MULTI_QUERIES("allowMultiQueries"),
    MAX_ROWS("maxRows"),
    REWRITE_BATCHED_STATEMENTS("rewriteBatchedStatements"),
    ZERO_DATE_TIME_BEHAVIOR("zeroDateTimeBehavior");

    private final String keyName;

    VitessPropertyKey(String keyName) {
        this.keyName = keyName;
    }

    public String getKeyName() {
        return keyName;
    }
}