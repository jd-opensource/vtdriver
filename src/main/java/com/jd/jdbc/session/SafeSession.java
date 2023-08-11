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

import com.jd.jdbc.common.Constant;
import com.jd.jdbc.sqlparser.utils.Utils;
import com.jd.jdbc.vitess.VitessConnection;
import com.jd.jdbc.vitess.mysql.VitessPropertyKey;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import io.vitess.proto.Vtgate;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

public class SafeSession {
    private final ReentrantLock lock = new ReentrantLock(true);

    private Boolean mustRollback = false;

    private AutocommitState autocommitState = AutocommitState.NOT_AUTO_COMMITTABLE;

    private Vtgate.CommitOrder commitOrder = Vtgate.CommitOrder.NORMAL;

    @Setter
    @Getter
    private VitessConnection vitessConnection;

    /**
     * @param vitessConnection
     * @return
     */
    public static SafeSession newSafeSession(VitessConnection vitessConnection) {
        SafeSession safeSession = new SafeSession();
        safeSession.setVitessConnection(vitessConnection);
        return safeSession;
    }

    public static SafeSession newSafeSession(VitessConnection vitessConnection, Vtgate.Session sessn) {
        if (sessn == null) {
            sessn = Vtgate.Session.newBuilder().build();
        }
        SafeSession safeSession = new SafeSession();
        safeSession.setVitessConnection(new VitessConnection(vitessConnection.getResolver(), sessn));
        safeSession.vitessConnection.setSessionNew(vitessConnection.getSessionNew());
        return safeSession;
    }

    public static SafeSession newAutoCommitSession(VitessConnection vitessConnection) {
        Vtgate.Session sessn = vitessConnection.getSession();
        Vtgate.Session.Builder newSessionBuilder = sessn.toBuilder().clone();
        newSessionBuilder.setInTransaction(false);
        newSessionBuilder.clearShardSessions();
        newSessionBuilder.clearPreSessions();
        newSessionBuilder.clearPostSessions();
        newSessionBuilder.setAutocommit(true);
        newSessionBuilder.clearWarnings();
        return newSafeSession(vitessConnection, newSessionBuilder.build());
    }

    /**
     *
     */
    public void resetTx() {
        this.lock.lock();
        try {
            VitessSession currentSessionNew = this.vitessConnection.getSessionNew();
            Vtgate.Session.Builder resetTxSessionBuilder = this.vitessConnection.getSession().toBuilder();
            this.mustRollback = false;
            this.autocommitState = AutocommitState.NOT_AUTO_COMMITTABLE;
            resetTxSessionBuilder.clearInTransaction();
            this.commitOrder = Vtgate.CommitOrder.NORMAL;
            resetTxSessionBuilder.clearSavepoints();
            if (!this.vitessConnection.getSession().getInReservedConn()) {
                resetTxSessionBuilder.clearShardSessions();
                resetTxSessionBuilder.clearPreSessions();
                resetTxSessionBuilder.clearPostSessions();
            }
            this.vitessConnection.setSession(resetTxSessionBuilder.build());
            this.vitessConnection.setSessionNew(currentSessionNew);
        } finally {
            this.lock.unlock();
        }
    }

    /**
     *
     */
    public void reset() {
        this.lock.lock();
        try {
            VitessSession currentSessionNew = this.vitessConnection.getSessionNew();
            Vtgate.Session.Builder resetSessionBuilder = this.vitessConnection.getSession().toBuilder();
            this.mustRollback = false;
            this.autocommitState = AutocommitState.NOT_AUTO_COMMITTABLE;
            resetSessionBuilder.clearInTransaction();
            this.commitOrder = Vtgate.CommitOrder.NORMAL;
            resetSessionBuilder.clearSavepoints();
            resetSessionBuilder.clearShardSessions();
            resetSessionBuilder.clearPreSessions();
            resetSessionBuilder.clearPostSessions();
            this.vitessConnection.setSession(resetSessionBuilder.build());
            this.vitessConnection.setSessionNew(currentSessionNew);
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * @param flag
     */
    public void setAutocommittable(Boolean flag) {
        this.lock.lock();
        try {
            if (this.autocommitState == AutocommitState.AUTO_COMMITTED) {
                return;
            }
            if (flag) {
                this.autocommitState = AutocommitState.AUTO_COMMITTABLE;
            } else {
                this.autocommitState = AutocommitState.NOT_AUTO_COMMITTABLE;
            }
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * @return
     */
    public Boolean autocommitApproval() {
        this.lock.lock();
        try {
            if (this.autocommitState.equals(AutocommitState.AUTO_COMMITTED)) {
                // Unreachable.
                return false;
            }

            if (this.autocommitState.equals(AutocommitState.AUTO_COMMITTABLE)) {
                this.autocommitState = AutocommitState.AUTO_COMMITTED;
                return true;
            }
            return false;
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * @return
     */
    public Boolean inTransaction() {
        this.lock.lock();
        try {
            return this.vitessConnection.getSession().getInTransaction();
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * @return
     */
    public Boolean inReservedConn() {
        this.lock.lock();
        try {
            return this.vitessConnection.getSession().getInReservedConn();
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * @return
     */
    public List<String> setPreQueries() {
        this.lock.lock();
        try {
            List<String> result = new ArrayList<>(this.vitessConnection.getSession().getSystemVariablesCount());
            int idx = 0;
            for (Map.Entry<String, String> entry : vitessConnection.getSession().getSystemVariablesMap().entrySet()) {
                result.set(idx, String.format("set @@%s = %s", entry.getKey(), entry.getValue()));
                idx++;
            }
            return result;
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * @param keyspace
     * @param shard
     * @param tabletType
     * @return
     */
    public FindResponse find(String keyspace, String shard, Topodata.TabletType tabletType) {
        this.lock.lock();
        try {
            List<Vtgate.Session.ShardSession> shardSessionsList = this.vitessConnection.getSession().getShardSessionsList();
            switch (this.commitOrder) {
                case PRE:
                    shardSessionsList = this.vitessConnection.getSession().getPreSessionsList();
                    break;
                case POST:
                    shardSessionsList = this.vitessConnection.getSession().getPostSessionsList();
                    break;
                default:
                    break;
            }

            for (Vtgate.Session.ShardSession shardSession : shardSessionsList) {
                if (keyspace.equals(shardSession.getTarget().getKeyspace())
                    && tabletType.equals(shardSession.getTarget().getTabletType())
                    && shard.equals(shardSession.getTarget().getShard())) {
                    // return shardSession.TransactionId, shardSession.ReservedId, shardSession.TabletAlias
                    return new FindResponse(shardSession.getTransactionId(),
                        shardSession.getReservedId(),
                        shardSession.getTabletAlias());
                }
            }
        } finally {
            this.lock.unlock();
        }
        return new FindResponse(0L, 0L, null);
    }

    /**
     * @param shardSession
     * @param txMode
     * @throws Exception
     */
    public void appendOrUpdate(Vtgate.Session.ShardSession shardSession, Vtgate.TransactionMode txMode) throws SQLException {
        this.lock.lock();
        try {

            if (AutocommitState.AUTO_COMMITTED.equals(this.autocommitState)) {
                // Should be unreachable
                throw new SQLException("BUG: SafeSession.AppendOrUpdate: unexpected autocommit state");
            }
            if (!(this.vitessConnection.getSession().getInTransaction() || this.vitessConnection.getSession().getInReservedConn())) {
                // Should be unreachable
                throw new SQLException("BUG: SafeSession.AppendOrUpdate: not in transaction and not in reserved connection");
            }
            this.autocommitState = AutocommitState.NOT_AUTO_COMMITTABLE;

            // Always append, in order for rollback to succeed.
            List<Vtgate.Session.ShardSession> shardSessionList;
            switch (this.commitOrder) {
                case NORMAL:
                    shardSessionList = buildSession(shardSession, this.vitessConnection.getSession().getShardSessionsList());
                    this.vitessConnection.setSession(this.vitessConnection.getSession().toBuilder().clearShardSessions().addAllShardSessions(shardSessionList).build());
                    this.vitessConnection.setSessionNew(this.vitessConnection.getSessionNew());

                    // isSingle is enforced only for normmal commit order operations.
                    if (this.isSingleDb(txMode) && this.vitessConnection.getSession().getShardSessionsCount() > 1) {
                        this.mustRollback = true;
                        throw new SQLException(String.format("multi-db transaction attempted: %s", this.vitessConnection.getSession().getShardSessionsList()));
                    }
                    break;
                case PRE:
                    shardSessionList = buildSession(shardSession, this.vitessConnection.getSession().getPreSessionsList());
                    this.vitessConnection.setSession(this.vitessConnection.getSession().toBuilder().clearPreSessions().addAllPreSessions(shardSessionList).build());
                    this.vitessConnection.setSessionNew(this.vitessConnection.getSessionNew());
                    break;
                case POST:
                    shardSessionList = buildSession(shardSession, this.vitessConnection.getSession().getPostSessionsList());
                    this.vitessConnection.setSession(this.vitessConnection.getSession().toBuilder().clearPostSessions().addAllPostSessions(shardSessionList).build());
                    this.vitessConnection.setSessionNew(this.vitessConnection.getSessionNew());
                    break;
                default:
                    throw new SQLException("BUG: SafeSession.AppendOrUpdate: unexpected commitOrder");
            }

        } finally {
            this.lock.unlock();
        }
    }

    /**
     * @param shardSession
     * @param shardSessionList
     * @return
     * @throws Exception
     */
    private List<Vtgate.Session.ShardSession> buildSession(Vtgate.Session.ShardSession shardSession, List<Vtgate.Session.ShardSession> shardSessionList) throws SQLException {
        return this.addOrUpdate(shardSession, shardSessionList);
    }

    public String getCharEncoding() {
        if (this.vitessConnection == null) {
            return null;
        }

        if (this.vitessConnection.getProperties() == null) {
            return null;
        }

        return this.vitessConnection.getProperties().getProperty(VitessPropertyKey.CHARACTER_ENCODING.getKeyName());
    }

    public int getMaxParallelNum() {
        if (this.vitessConnection == null) {
            return 1;
        }

        if (this.vitessConnection.getProperties() == null) {
            return 1;
        }

        int maxParallel = Utils.getInteger(this.vitessConnection.getProperties(), Constant.DRIVER_PROPERTY_QUERY_PARALLEL_NUM, 1);
        return maxParallel > 0 ? maxParallel : 1;
    }

    /**
     * @return
     */
    public Boolean mustRollback() {
        this.lock.lock();
        try {
            return this.mustRollback;
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * @param queryWarning
     */
    public void recordWarning(Query.QueryWarning queryWarning) {
        this.lock.lock();
        try {
            List<Query.QueryWarning> queryWarnings = new ArrayList<>();
            if (this.vitessConnection.getSession().getWarningsList().size() == 0 ||
                this.vitessConnection.getSession().getWarningsList().isEmpty()) {
                queryWarnings.add(queryWarning);
            } else {
                queryWarnings.addAll(this.vitessConnection.getSession().getWarningsList());
            }
            Vtgate.Session session = Vtgate.Session.newBuilder(vitessConnection.getSession()).clearWarnings().addAllWarnings(queryWarnings).build();
            this.vitessConnection.setSession(session);
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * @param shardSession
     * @param sessions
     * @return
     */
    private List<Vtgate.Session.ShardSession> addOrUpdate(Vtgate.Session.ShardSession shardSession, List<Vtgate.Session.ShardSession> sessions) throws SQLException {
        sessions = new ArrayList<>(sessions);
        boolean appendSession = true;
        for (int i = 0; i < sessions.size(); i++) {
            Vtgate.Session.ShardSession sess = sessions.get(i);
            boolean targetedAtSameTablet = sess.getTarget().getKeyspace().equals(shardSession.getTarget().getKeyspace())
                && sess.getTarget().getTabletType().equals(shardSession.getTarget().getTabletType())
                && sess.getTarget().getShard().equals(shardSession.getTarget().getShard());
            if (targetedAtSameTablet) {
                if ((!sess.getTabletAlias().getCell().equals(shardSession.getTabletAlias().getCell()))
                    || sess.getTabletAlias().getUid() != shardSession.getTabletAlias().getUid()) {
                    throw new SQLException("got a different alias for the same target");
                }
                // replace the old info with the new one
                sessions.set(i, shardSession);
                appendSession = false;
                break;
            }
        }
        if (appendSession) {
            sessions.add(shardSession);
        }

        return sessions;
    }

    private Boolean isSingleDb(Vtgate.TransactionMode txMode) {
        return this.vitessConnection.getSession().getTransactionMode().equals(Vtgate.TransactionMode.SINGLE)
            || (this.vitessConnection.getSession().getTransactionMode().equals(Vtgate.TransactionMode.UNSPECIFIED) && txMode.equals(Vtgate.TransactionMode.SINGLE));
    }

    /**
     *
     */
    private enum AutocommitState {
        /**
         *
         */
        NOT_AUTO_COMMITTABLE(0),
        AUTO_COMMITTABLE(1),
        AUTO_COMMITTED(2);

        int value;

        AutocommitState(int value) {
            this.value = value;
        }
    }

    /**
     *
     */
    @Data
    @AllArgsConstructor
    public static class FindResponse {
        Long transactionId;

        Long reservedId;

        Topodata.TabletAlias tabletAlias;
    }
}
