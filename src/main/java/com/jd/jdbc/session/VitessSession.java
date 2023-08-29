/*
Copyright 2021 JD Project Authors. Licensed under Apache-2.0.

Copyright 2019 The Vitess Authors.

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

package com.jd.jdbc.session;

import io.vitess.proto.Query;
import java.math.BigInteger;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

public class VitessSession {

    @Getter
    private BigInteger lastInsertId;

    @Setter
    private List<ShardSession> shardSessions;

    @Setter
    private List<ShardSession> preSessions;

    @Setter
    private List<ShardSession> postSessions;

    @Setter
    private boolean inTransaction;

    @Setter
    private boolean autocommit;

    private boolean inReservedConn;

    private TransactionMode transactionMode;

    @Setter
    private List<Query.QueryWarning> warnings;

    public VitessSession() {
        this.lastInsertId = BigInteger.ZERO;
        this.shardSessions = java.util.Collections.emptyList();
        this.preSessions = java.util.Collections.emptyList();
        this.postSessions = java.util.Collections.emptyList();
        this.inTransaction = false;
        this.autocommit = false;
        this.inReservedConn = false;
        this.transactionMode = TransactionMode.UNSPECIFIED;
        this.warnings = java.util.Collections.emptyList();
    }

    public void setLastInsertId(long setId) {
        this.lastInsertId = BigInteger.valueOf(setId);
    }

    public void setLastInsertId(BigInteger setId) {
        this.lastInsertId = setId;
    }

    public List<ShardSession> getPostSessionsList() {
        return postSessions;
    }

    public List<ShardSession> getPreSessionsList() {
        return preSessions;
    }

    public List<ShardSession> getShardSessionsList() {
        return shardSessions;
    }

    public List<Query.QueryWarning> getWarningsList() {
        return warnings;
    }

    public void clearWarnings() {
        warnings.clear();
    }

    public boolean getInTransaction() {
        return inTransaction;
    }

    public boolean getAutocommit() {
        return autocommit;
    }

    public TransactionMode getTransactionMode() {
        return transactionMode == null ? TransactionMode.UNRECOGNIZED : transactionMode;
    }

    public boolean getInReservedConn() {
        return inReservedConn;
    }

    public void clearShardSessions() {
        shardSessions.clear();
    }

    public void clearPreSessions() {
        preSessions.clear();
    }

    public void clearPostSessions() {
        postSessions.clear();
    }

    public void clearInTransaction() {
        inTransaction = false;
    }
}
