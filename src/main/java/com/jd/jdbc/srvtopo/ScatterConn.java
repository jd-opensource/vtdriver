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

import com.jd.jdbc.IExecute;
import com.jd.jdbc.IExecute.ExecuteMultiShardResponse;
import com.jd.jdbc.concurrency.AllErrorRecorder;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.context.VtContext;
import com.jd.jdbc.discovery.HealthCheck;
import com.jd.jdbc.queryservice.IQueryService;
import com.jd.jdbc.queryservice.StreamIterator;
import com.jd.jdbc.session.SafeSession;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqltypes.BatchVtResultSet;
import com.jd.jdbc.sqltypes.BeginBatchVtResultSet;
import com.jd.jdbc.sqltypes.BeginVtResultSet;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.util.threadpool.impl.VtQueryExecutorService;
import com.jd.jdbc.vitess.VitessConnection;
import com.jd.jdbc.vitess.mysql.VitessPropertyKey;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import io.vitess.proto.Vtgate;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class ScatterConn {

    private static final Log logger = LogFactory.getLog(ScatterConn.class);

    private TxConn txConn;

    private Gateway gateway;

    private HealthCheck legacyHealthCheck;

    /**
     * @param txConn
     * @param gateway
     * @param legacyHealthCheck
     */
    private ScatterConn(TxConn txConn, Gateway gateway, HealthCheck legacyHealthCheck) {
        this.txConn = txConn;
        this.gateway = gateway;
        this.legacyHealthCheck = legacyHealthCheck;
    }

    /**
     * @param statsName
     * @param txConn
     * @param gw
     * @return
     */
    public static ScatterConn newScatterConn(String statsName, TxConn txConn, TabletGateway gw) {
        // gateway has a reference to healthCheck so we don't need this any more
        return new ScatterConn(txConn, gw, null);
    }

    /**
     * ExecuteMultiShard is like Execute,
     * but each shard gets its own Sql Queries and BindVariables.
     * <p>
     * It always returns a non-nil query result and an array of
     * shard errors which may be nil so that callers can optionally
     * process a partially-successful operation.
     *
     * @param context
     * @param rss
     * @param queries
     * @param safeSession
     * @param autocommit
     * @param ignoreMaxMemoryRows
     * @return
     * @throws Exception
     */
    public ExecuteMultiShardResponse executeMultiShard(IContext context, List<ResolvedShard> rss, List<BoundQuery> queries, SafeSession safeSession, Boolean autocommit,
                                                       Boolean ignoreMaxMemoryRows) throws SQLException {
        if (rss.size() != queries.size()) {
            throw new SQLException("BUG: got mismatched number of queries and shards");
        }
        // lock protects resultSet
        ReentrantLock lock = new ReentrantLock(true);
        VtResultSet resultSet = new VtResultSet();

        IContext ctx = VtContext.withCancel(context);
        AllErrorRecorder allErrors = this.multiGoTransaction(ctx, "Execute", rss, safeSession, autocommit, (rs, i, shardActionInfo) -> {
            VtResultSet innerResultSet;
            Query.ExecuteOptions opts = null;
            Topodata.TabletAlias alias = null;

            Long transactionId = shardActionInfo.transactionId;
            Long reservedId = shardActionInfo.reservedId;

            if (safeSession != null && safeSession.getVitessConnection().getSession() != null) {
                opts = safeSession.getVitessConnection().getSession().getOptions();
            }

            if (autocommit) {
                // As this is auto-commit, the transactionID is supposed to be zero.
                if (shardActionInfo.transactionId != 0L) {
                    return new ShardActionTransactionFuncResponse(null, new SQLException(String.format("in autocommit mode, transactionID should be zero but was: %d", shardActionInfo.transactionId)));
                }
            }

            IQueryService queryService;
            switch (shardActionInfo.actionNeeded) {
                case NOTHING:
                    try {
                        queryService = getQueryService(rs, shardActionInfo);
                        innerResultSet =
                            queryService.execute(ctx, rs.getTarget(), queries.get(i).getSql(), queries.get(i).getBindVariablesMap(), shardActionInfo.transactionId, shardActionInfo.reservedId, opts);
                    } catch (Exception e) {
                        return new ShardActionTransactionFuncResponse(null, e);
                    }
                    break;
                case BEGIN:
                    try {
                        queryService = getQueryService(rs, shardActionInfo);
                    } catch (Exception e) {
                        return new ShardActionTransactionFuncResponse(null, e);
                    }
                    try {
                        BeginVtResultSet beginVtResultSet =
                            queryService.beginExecute(ctx, rs.getTarget(), Objects.requireNonNull(safeSession).getVitessConnection().getSession().getSavepointsList(), queries.get(i).getSql(),
                                queries.get(i).getBindVariablesMap(), shardActionInfo.reservedId, opts);
                        innerResultSet = beginVtResultSet.getVtResultSet();
                        transactionId = beginVtResultSet.getTransactionId();
                        alias = beginVtResultSet.getAlias();
                    } catch (Exception e) {
                        return new ShardActionTransactionFuncResponse(shardActionInfo.updateTransactionId(transactionId, null), e);
                    }
                    break;
                default:
                    return new ShardActionTransactionFuncResponse(null,
                        new SQLException(String.format("BUG: unexpected actionNeeded on ScatterConn#ExecuteMultiShard %s", shardActionInfo.actionNeeded)));
            }
            lock.lock();
            try {
                Exception exception = null;
                // Don't append more rows if row count is exceeded.
                if (ignoreMaxMemoryRows || resultSet.getRows().size() <= VitessConnection.MAX_MEMORY_ROWS) {
                    try {
                        resultSet.appendResult(innerResultSet);
                    } catch (SQLException e) {
                        exception = e;
                    }
                }
                return new ShardActionTransactionFuncResponse(shardActionInfo.updateTransactionAndReservedId(transactionId, reservedId, alias), exception);
            } finally {
                lock.unlock();
            }
        });

        if (!ignoreMaxMemoryRows && resultSet.getRows().size() > VitessConnection.MAX_MEMORY_ROWS) {
            throw new SQLException(String.format("in-memory row count exceeded allowed limit of %d", VitessConnection.MAX_MEMORY_ROWS));
        }
        allErrors.throwException();
        return new ExecuteMultiShardResponse(resultSet);
    }

    public IExecute.ExecuteBatchMultiShardResponse executeBatchMultiShard(IContext context, List<ResolvedShard> rss, List<List<BoundQuery>> queries, SafeSession safeSession, Boolean autocommit,
                                                                          Boolean ignoreMaxMemoryRows, Boolean asTransaction) throws SQLException {
        if (rss.size() != queries.size()) {
            throw new SQLException("BUG: got mismatched number of queries and shards");
        }
        // lock protects resultSet
        ReentrantLock lock = new ReentrantLock(true);
        List<List<VtResultSet>> vtResultSetList = new ArrayList<>();
        List<ResolvedShard> rssr = new ArrayList<>();
        if (safeSession != null) {
            VitessConnection vitessConnection = safeSession.getVitessConnection();
            if (vitessConnection != null) {
                context.setContextValue(VitessPropertyKey.MAX_ALLOWED_PACKET, vitessConnection.getServerSessionPropertiesMap().get(VitessPropertyKey.MAX_ALLOWED_PACKET.getKeyName()));
            }
        }
        IContext ctx = VtContext.withCancel(context);
        AllErrorRecorder allErrors = this.multiGoTransaction(ctx, "batchExecute", rss, safeSession, autocommit, (rs, i, shardActionInfo) -> {
            List<VtResultSet> innerResultSets = null;
            Query.ExecuteOptions opts = null;
            Topodata.TabletAlias alias = null;
            Long transactionId = shardActionInfo.transactionId;
            Long reservedId = shardActionInfo.reservedId;
            if (safeSession != null && safeSession.getVitessConnection().getSession() != null) {
                opts = safeSession.getVitessConnection().getSession().getOptions();
            }
            if (autocommit) {
                // As this is auto-commit, the transactionID is supposed to be zero.
                if (shardActionInfo.transactionId != 0L) {
                    return new ShardActionTransactionFuncResponse(null, new SQLException(String.format("in autocommit mode, transactionID should be zero but was: %d", shardActionInfo.transactionId)));
                }
            }

            IQueryService queryService;
            switch (shardActionInfo.actionNeeded) {
                case NOTHING:
                    try {
                        queryService = getQueryService(rs, shardActionInfo);
                        BatchVtResultSet batchVtResultSet = queryService.executeBatch(ctx, rs.getTarget(), queries.get(i), asTransaction, shardActionInfo.transactionId, opts);
                        innerResultSets = batchVtResultSet.getVtResultSets();
                    } catch (Exception e) {
                        return new ShardActionTransactionFuncResponse(null, e);
                    }
                    break;
                case BEGIN:
                    try {
                        queryService = getQueryService(rs, shardActionInfo);
                    } catch (Exception e) {
                        return new ShardActionTransactionFuncResponse(null, e);
                    }
                    try {
                        BeginBatchVtResultSet beginBatchVtResultSet = queryService.beginExecuteBatch(ctx, rs.getTarget(), queries.get(i), true, opts);
                        innerResultSets = beginBatchVtResultSet.getVtResultSets();
                        transactionId = beginBatchVtResultSet.getTransactionId();
                        alias = beginBatchVtResultSet.getAlias();
                    } catch (Exception e) {
                        return new ShardActionTransactionFuncResponse(shardActionInfo.updateTransactionId(transactionId, null), e);
                    }
                    break;
                default:
                    return new ShardActionTransactionFuncResponse(null,
                        new SQLException(String.format("BUG: unexpected actionNeeded on ScatterConn#ExecuteMultiShard %s", shardActionInfo.actionNeeded)));
            }

            lock.lock();
            try {
                Exception exception = null;
                // Don't append more rows if row count is exceeded.
                int size = getTotalSize(vtResultSetList);
                if (ignoreMaxMemoryRows || size <= VitessConnection.MAX_MEMORY_ROWS) {
                    vtResultSetList.add(innerResultSets);
                    rssr.add(rs);
                }
                return new ShardActionTransactionFuncResponse(shardActionInfo.updateTransactionAndReservedId(transactionId, reservedId, alias), exception);
            } finally {
                lock.unlock();
            }
        });

        int size = getTotalSize(vtResultSetList);
        if (!ignoreMaxMemoryRows && size > VitessConnection.MAX_MEMORY_ROWS) {
            throw new SQLException(String.format("in-memory row count exceeded allowed limit of %d", VitessConnection.MAX_MEMORY_ROWS));
        }
        allErrors.throwException();
        return new IExecute.ExecuteBatchMultiShardResponse(vtResultSetList, rssr);
    }

    /**
     * @param ctx
     * @param name
     * @param rss
     * @param safeSession
     * @param autocommit
     * @param action
     * @return
     */
    public AllErrorRecorder multiGoTransaction(IContext ctx, String name, List<ResolvedShard> rss, SafeSession safeSession, Boolean autocommit, ShardActionTransactionFunc action) {
        AllErrorRecorder allErrors = new AllErrorRecorder();
        if (rss.isEmpty()) {
            return allErrors;
        }

        if (rss.size() == 1) {
            allErrors.recordError(this.oneShard(rss.get(0), 0, safeSession, autocommit, action));
        } else {
            CountDownLatch countDownLatch = new CountDownLatch(rss.size());

            for (int i = 0; i < rss.size(); i++) {
                ResolvedShard rs = rss.get(i);
                final int ii = i;
                VtQueryExecutorService.execute(() -> {
                    try {
                        allErrors.recordError(this.oneShard(rs, ii, safeSession, autocommit, action));
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error(e.getLocalizedMessage());
            }
        }

        if (safeSession.mustRollback()) {
            try {
                this.txConn.rollback(ctx, safeSession);
            } catch (SQLException e) {
                allErrors.recordError(e);
            }
        }
        return allErrors;
    }


    public List<StreamIterator> streamExecuteMultiShard(IContext context, List<ResolvedShard> rss, List<BoundQuery> queries, SafeSession safeSession) throws SQLException {
        if (rss == null || rss.size() == 0) {
            throw new SQLException("ResolvedShard should not empty");
        }

        ReentrantLock lock = new ReentrantLock();
        List<StreamIterator> iteratorList = new ArrayList<>(rss.size());

        IContext ctx = VtContext.withCancel(context);

        AllErrorRecorder allErrors = this.multiGo("StreamExecute", rss, (rs, i) -> {
            StreamIterator iterator = null;
            Exception exception = null;
            ShardActionInfo shardActionInfo = actionInfo(rs.getTarget(), safeSession, false);
            try {
                IQueryService queryService = getQueryService(rs, shardActionInfo);
                iterator = queryService.streamExecute(ctx, rs.getTarget(), queries.get(i).getSql(), queries.get(i).getBindVariablesMap(), 0L, null);
            } catch (SQLException e) {
                exception = e;
            }

            lock.lock();
            try {
                iteratorList.add(iterator);
            } finally {
                lock.unlock();
            }
            return exception;
        });
        if (allErrors.hasErrors()) {
            for (StreamIterator streamIterator : iteratorList) {
                if (streamIterator != null) {
                    streamIterator.close();
                }
            }
        }
        allErrors.throwException();
        return iteratorList;
    }

    private AllErrorRecorder multiGo(String name, List<ResolvedShard> rss, ShardActionFunc action) {
        int numShards = rss.size();
        AllErrorRecorder allErrors = new AllErrorRecorder();
        if (rss.isEmpty()) {
            return allErrors;
        }
        if (numShards == 1) {
            allErrors.recordError(action.action(rss.get(0), 0));
        } else {
            CountDownLatch countDownLatch = new CountDownLatch(rss.size());
            for (int i = 0; i < rss.size(); i++) {
                ResolvedShard rs = rss.get(i);
                final int ii = i;
                VtQueryExecutorService.execute(() -> {
                    try {
                        allErrors.recordError(action.action(rs, ii));
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error(e.getLocalizedMessage());
            }
        }
        return allErrors;
    }

    /**
     * @param rs
     * @param info
     * @return
     */
    private IQueryService getQueryService(ResolvedShard rs, ShardActionInfo info) {
        if (info.alias == null) {
            return rs.getGateway().getQueryService();
        }
        return rs.getGateway().queryServiceByAlias(info.alias);
    }

    /**
     * @param rs
     * @param i
     * @param safeSession
     * @param autocommit
     * @param action
     */
    private Exception oneShard(ResolvedShard rs, Integer i, SafeSession safeSession, Boolean autocommit, ShardActionTransactionFunc action) {
        Exception exception = null;
        /* TODO
        startTime, statsKey := stc.startAction(name, rs.Target)
        defer stc.endAction(startTime, allErrors, statsKey, &err, session)
        */
        ShardActionInfo shardActionInfo = actionInfo(rs.getTarget(), safeSession, autocommit);
        ShardActionTransactionFuncResponse response = action.action(rs, i, shardActionInfo);
        ShardActionInfo updated = response.getShardActionInfo();
        exception = response.getException();

        if (updated == null) {
            return exception;
        }
        if (updated.actionNeeded != ActionNeeded.NOTHING && (updated.transactionId != 0 || updated.reservedId != 0)) {
            try {
                safeSession.appendOrUpdate(Vtgate.Session.ShardSession.newBuilder()
                        .setTarget(rs.getTarget())
                        .setTransactionId(updated.transactionId)
                        .setReservedId(updated.reservedId)
                        .setTabletAlias(updated.alias)
                        .build(),
                    this.txConn.getTxMode());
            } catch (Exception e) {
                logger.error("ScatterConn.oneShard.safeSession.appendOrUpdate.exception", e);
                exception = e;
            }
        }
        return exception;
    }

    /**
     * @param target
     * @param safeSession
     * @param autocommit
     * @return
     */
    private ShardActionInfo actionInfo(Query.Target target, SafeSession safeSession, Boolean autocommit) {
        if (!(safeSession.inTransaction() || safeSession.inReservedConn())) {
            return new ShardActionInfo();
        }
        // No need to protect ourselves from the race condition between
        // Find and AppendOrUpdate. The higher level functions ensure that no
        // duplicate (target) tuples can execute
        // this at the same time.
        SafeSession.FindResponse findResponse = safeSession.find(target.getKeyspace(), target.getShard(), target.getTabletType());
        Long transactionId = findResponse.getTransactionId();
        Long reservedId = findResponse.getReservedId();
        Topodata.TabletAlias tabletAlias = findResponse.getTabletAlias();

        Boolean shouldReserve = safeSession.inReservedConn() && reservedId == 0;
        Boolean shouldBegin = safeSession.inTransaction() && transactionId == 0 && !autocommit;

        ActionNeeded act = ScatterConn.ActionNeeded.NOTHING;
        if (shouldBegin && shouldReserve) {
            act = ActionNeeded.RESERVE_BEGIN;
        } else if (shouldReserve) {
            act = ActionNeeded.RESERVE;
        } else if (shouldBegin) {
            act = ActionNeeded.BEGIN;
        }

        return new ShardActionInfo(act, reservedId, transactionId, tabletAlias);
    }

    private int getTotalSize(List<List<VtResultSet>> vtResultSetList) {
        int size = 0;
        if (vtResultSetList == null || vtResultSetList.isEmpty()) {
            return size;
        }
        for (List<VtResultSet> vtResultSets : vtResultSetList) {
            if (vtResultSets == null || vtResultSets.isEmpty()) {
                continue;
            }
            for (VtResultSet vtResultSet : vtResultSets) {
                if (vtResultSet == null) {
                    continue;
                }
                size += vtResultSet.getRows() == null ? 0 : vtResultSet.getRows().size();
            }
        }
        return size;
    }

    /**
     *
     */
    enum ActionNeeded {
        /**
         *
         */
        NOTHING(0),
        RESERVE_BEGIN(1),
        RESERVE(2),
        BEGIN(3);

        int value;

        ActionNeeded(int value) {
            this.value = value;
        }
    }

    /**
     *
     */
    @FunctionalInterface
    interface ShardActionTransactionFunc {

        /**
         * @param rs
         * @param i
         * @param shardActionInfo
         * @return
         */
        ScatterConn.ShardActionTransactionFuncResponse action(ResolvedShard rs, Integer i, ScatterConn.ShardActionInfo shardActionInfo);
    }

    @FunctionalInterface
    interface ShardActionFunc {
        /**
         * @param rs
         * @param i
         * @return
         */
        Exception action(ResolvedShard rs, Integer i);
    }

    /**
     *
     */
    @Data
    @AllArgsConstructor
    static class ShardActionTransactionFuncResponse {
        ShardActionInfo shardActionInfo;

        Exception exception;
    }

    /**
     *
     */
    @NoArgsConstructor
    @AllArgsConstructor
    static class ShardActionInfo {
        ActionNeeded actionNeeded = ActionNeeded.NOTHING;

        Long reservedId = 0L;

        Long transactionId = 0L;

        Topodata.TabletAlias alias;

        /**
         * @param txId
         * @param alias
         * @return
         */
        ShardActionInfo updateTransactionId(Long txId, Topodata.TabletAlias alias) {
            return updateTransactionAndReservedId(txId, this.reservedId, alias);
        }

        /**
         * @param rId
         * @param alias
         * @return
         */
        ShardActionInfo updateReservedId(Long rId, Topodata.TabletAlias alias) {
            return updateTransactionAndReservedId(this.transactionId, rId, alias);
        }

        /**
         * @param txId
         * @param rId
         * @param alias
         * @return
         */
        ShardActionInfo updateTransactionAndReservedId(Long txId, Long rId, Topodata.TabletAlias alias) {
            return new ShardActionInfo(this.actionNeeded, rId, txId, alias);
        }
    }
}
