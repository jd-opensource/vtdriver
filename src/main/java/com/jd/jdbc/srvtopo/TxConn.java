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

package com.jd.jdbc.srvtopo;

import com.jd.jdbc.concurrency.AllErrorRecorder;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.queryservice.IQueryService;
import com.jd.jdbc.session.SafeSession;
import com.jd.jdbc.session.ShardSession;
import com.jd.jdbc.session.TransactionMode;
import com.jd.jdbc.session.VitessSession;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.util.threadpool.impl.VtQueryExecutorService;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class TxConn {
    private static final Log logger = LogFactory.getLog(TxConn.class);

    private final Gateway gateway;

    @Getter
    private final TransactionMode txMode;

    /**
     * @param ctx
     * @param safeSession
     * @return
     */
    public void begin(IContext ctx, SafeSession safeSession) throws SQLException {
        if (safeSession.getVitessConnection().getSession().getInTransaction()) {
            this.commit(ctx, safeSession);
        }
        safeSession.getVitessConnection().getSession().setInTransaction(true);
    }

    /**
     * @param ctx
     * @param safeSession
     * @return
     */
    public void commit(IContext ctx, SafeSession safeSession) throws SQLException {
        try {
            if (!safeSession.getVitessConnection().getSession().getInTransaction()) {
                return;
            }

            boolean twopc = false;

            switch (safeSession.getVitessConnection().getSession().getTransactionMode()) {
                case TWOPC:
                    twopc = true;
                    break;
                case UNSPECIFIED:
                    twopc = this.txMode == TransactionMode.TWOPC;
                    break;
                default:
                    break;
            }
            if (twopc) {
                this.commit2Pc(ctx, safeSession);
            }
            this.commitNormal(ctx, safeSession);
        } finally {
            safeSession.resetTx();
        }
    }

    /**
     * @param tabletAlias
     * @return
     */
    private IQueryService queryService(Topodata.TabletAlias tabletAlias) {
        return this.gateway.queryServiceByAlias(tabletAlias);
    }

    /**
     * @param ctx
     * @param shardSession
     * @return
     */
    private SQLException commitShard(IContext ctx, ShardSession shardSession) {
        if (shardSession.getTransactionId() == 0) {
            return null;
        }
        try {
            IQueryService queryService = this.queryService(shardSession.getTabletAlias());
            Query.CommitResponse commitResponse = queryService.commit(ctx, shardSession.getTarget(), shardSession.getTransactionId());
            shardSession.clearTransactionId();
            shardSession.setReservedId(commitResponse.getReservedId());
        } catch (SQLException e) {
            return e;
        }
        return null;
    }

    /**
     * @param ctx
     * @param safeSession
     * @return
     */
    private void commitNormal(IContext ctx, SafeSession safeSession) throws SQLException {
        SQLException exception = this.runSessions(ctx, safeSession.getVitessConnection().getSession().getPreSessionsList(), this::commitShard);
        if (exception != null) {
            this.release(ctx, safeSession);
            throw exception;
        }

        // Retain backward compatibility on commit order for the normal session.
        for (ShardSession shardSession : safeSession.getVitessConnection().getSession().getShardSessionsList()) {
            SQLException e = this.commitShard(ctx, shardSession);
            if (e != null) {
                this.release(ctx, safeSession);
                throw e;

            }
        }

        SQLException e = this.runSessions(ctx, safeSession.getVitessConnection().getSession().getPostSessionsList(), this::commitShard);
        if (e != null) {
            // If last commit fails, there will be nothing to rollback.
            safeSession.recordWarning(Query.QueryWarning.newBuilder().setMessage(String.format("post-operation transaction had an error: %s", e.getMessage())).build());
            // With reserved connection we should release them.
            if (safeSession.inReservedConn()) {
                this.release(ctx, safeSession);
            }
        }
    }

    /**
     * @param ctx
     * @param safeSession
     * @return
     */
    private void commit2Pc(IContext ctx, SafeSession safeSession) throws SQLException {
    }

    /**
     * @param ctx
     * @param safeSession
     * @return
     */
    public void rollback(IContext ctx, SafeSession safeSession) throws SQLException {
        try {
            if (!safeSession.inTransaction()) {
                return;
            }

            VitessSession session = safeSession.getVitessConnection().getSession();
            List<ShardSession> allsessions = Stream
                .of(session.getPreSessionsList(), session.getShardSessionsList(), session.getPostSessionsList())
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

            SQLException exception = this.runSessions(ctx, allsessions, (vContext, shardSession) -> {
                if (shardSession.getTransactionId() == 0) {
                    return null;
                }
                try {
                    IQueryService queryService = this.queryService(shardSession.getTabletAlias());
                    long reservedId = queryService.rollback(ctx, shardSession.getTarget(), shardSession.getTransactionId()).getReservedId();
                    shardSession.clearTransactionId();
                    shardSession.setReservedId(reservedId);
                } catch (SQLException e) {
                    return e;
                }
                return null;
            });
            if (exception != null) {
                safeSession.recordWarning(
                    Query.QueryWarning.newBuilder().setMessage(String.format("rollback encountered an error and connection to all shard for this session is released: %s", exception.getMessage()))
                        .build());
                if (safeSession.inReservedConn()) {
                    this.release(ctx, safeSession);
                }
                throw exception;
            }
        } finally {
            safeSession.resetTx();
        }
    }

    /**
     * @param ctx
     * @param safeSession
     * @return
     */
    public Exception release(IContext ctx, SafeSession safeSession) {

        try {
            if (!safeSession.inTransaction() && !safeSession.inReservedConn()) {
                return null;
            }

            VitessSession session = safeSession.getVitessConnection().getSession();
            List<ShardSession> allsessions = Stream
                .of(session.getPreSessionsList(), session.getShardSessionsList(), session.getPostSessionsList())
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

            safeSession.reset();

            return this.runSessions(ctx, allsessions, (vContext, shardSession) -> {
                if (shardSession.getReservedId() == 0 && shardSession.getTransactionId() == 0) {
                    return null;
                }
                try {
                    IQueryService queryService = this.queryService(shardSession.getTabletAlias());
                    queryService.release(ctx, shardSession.getTarget(), shardSession.getTransactionId(), shardSession.getReservedId());
                } catch (SQLException e) {
                    return e;
                }
                shardSession.clearTransactionId();
                shardSession.clearReservedId();
                return null;
            });
        } finally {
            safeSession.reset();
        }
    }

    /**
     * @param ctx
     * @param shardSessions
     * @param action
     * @return
     */
    private SQLException runSessions(IContext ctx, List<ShardSession> shardSessions, RunSessionsFunc action) {
        if (shardSessions == null || shardSessions.size() == 0) {
            return null;
        }
        // Fastpath.
        if (shardSessions.size() == 1) {
            return action.action(ctx, shardSessions.get(0));
        }

        AllErrorRecorder allErrorRecorder = new AllErrorRecorder();
        CountDownLatch countDownLatch = new CountDownLatch(shardSessions.size());

        for (ShardSession shardSession : shardSessions) {
            VtQueryExecutorService.execute(() -> {
                try {
                    Exception exception = action.action(ctx, shardSession);
                    if (exception != null) {
                        allErrorRecorder.recordError(exception);
                    }
                } finally {
                    countDownLatch.countDown();
                }
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }

        return allErrorRecorder.error();
    }

    /**
     *
     */
    @FunctionalInterface
    private interface RunSessionsFunc {
        SQLException action(IContext ctx, ShardSession shardSession);
    }
}
