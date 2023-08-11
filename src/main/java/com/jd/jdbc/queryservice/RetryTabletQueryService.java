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

package com.jd.jdbc.queryservice;

import com.jd.jdbc.context.IContext;
import com.jd.jdbc.discovery.HealthCheck;
import com.jd.jdbc.discovery.TabletHealthCheck;
import com.jd.jdbc.queryservice.util.RoleUtils;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqltypes.BatchVtResultSet;
import com.jd.jdbc.sqltypes.BeginBatchVtResultSet;
import com.jd.jdbc.sqltypes.BeginVtResultSet;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.BoundQuery;
import com.jd.jdbc.srvtopo.Gateway;
import com.jd.jdbc.srvtopo.TabletGateway;
import com.jd.jdbc.topo.topoproto.TopoProto;
import io.grpc.stub.StreamObserver;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RetryTabletQueryService implements IQueryService, IHealthCheckQueryService {
    private static final Log log = LogFactory.getLog(RetryTabletQueryService.class);

    private static final int RETRY_COUNT = 3;

    private final Gateway gateway;

    public RetryTabletQueryService(Gateway gateway) {
        this.gateway = gateway;
    }

    @Override
    public Query.BeginResponse begin(IContext context, Query.Target target, Query.ExecuteOptions options) throws Exception {
        //not used.
        return null;
    }

    @Override
    public Query.CommitResponse commit(IContext context, Query.Target target, Long transactionID) throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug("commit --> " + transactionID);
        }
        return (Query.CommitResponse) retry(true, target, (IQueryService qs) -> {
            try {
                Query.CommitResponse ret = qs.commit(context, target, transactionID);
                return new IInner.InnerResult(false, ret);
            } catch (SQLException e) {
                if (canRetry(context, e)) {
                    return IInner.DEFAULT_RETRY;
                } else {
                    throw e;
                }
            }
        }, RoleUtils.getRoleType(context)).responses;
    }

    @Override
    public Query.RollbackResponse rollback(IContext context, Query.Target target, Long transactionID) throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug("rollback --> " + transactionID);
        }
        return (Query.RollbackResponse) retry(true, target, (IQueryService qs) -> {
            try {
                Query.RollbackResponse ret = qs.rollback(context, target, transactionID);
                return new IInner.InnerResult(false, ret);
            } catch (SQLException e) {
                if (canRetry(context, e)) {
                    return IInner.DEFAULT_RETRY;
                } else {
                    throw e;
                }
            }
        }, RoleUtils.getRoleType(context)).responses;
    }

    @Override
    public BeginVtResultSet beginExecute(IContext context, Query.Target target, List<String> preQuries, String sql, Map<String, BindVariable> bindVariables, Long reservedID,
                                         Query.ExecuteOptions options) throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug("beginExecute --> " + sql);
        }

        boolean inDedicatedConn = (reservedID != 0);
        return (BeginVtResultSet) retry(inDedicatedConn, target, (IQueryService qs) -> {
            try {
                BeginVtResultSet beginVtResultSet = qs.beginExecute(context, target, preQuries, sql, bindVariables, reservedID, options);
                return new IInner.InnerResult(false, beginVtResultSet);
            } catch (SQLException e) {
                if (canRetry(context, e)) {
                    if (inDedicatedConn) {
                        throw e;
                    } else {
                        return IInner.DEFAULT_RETRY;
                    }
                } else {
                    throw e;
                }
            }
        }, RoleUtils.getRoleType(context)).resultSetMessage;
    }

    @Override
    public VtResultSet execute(IContext context, Query.Target target, String sql, Map<String, BindVariable> bindVariables, Long transactionID, Long reservedID, Query.ExecuteOptions options)
        throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug("execute [" + transactionID + "] --> " + sql);
        }
        boolean inDedicatedConn = (reservedID != 0 || transactionID != 0);
        return (VtResultSet) retry(inDedicatedConn, target, (IQueryService qs) -> {
            try {
                VtResultSet ret = qs.execute(context, target, sql, bindVariables, transactionID, reservedID, options);
                return new IInner.InnerResult(false, ret);
            } catch (SQLException e) {
                if (canRetry(context, e)) {
                    if (inDedicatedConn) {
                        throw e;
                    } else {
                        return IInner.DEFAULT_RETRY;
                    }
                } else {
                    throw e;
                }
            }
        }, RoleUtils.getRoleType(context)).resultSetMessage;
    }

    @Override
    public StreamIterator streamExecute(IContext context, Query.Target target, String sql, Map<String, BindVariable> bindVariables, Long transactionID, Query.ExecuteOptions options)
        throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug("streamExecute --> " + sql);
        }
        List<TabletHealthCheck> tablets = ((TabletGateway) gateway).getHc().getHealthyTabletStatsMaybeStandby(target);
        if (tablets == null || tablets.size() == 0) {
            throw new SQLException("no valid tablet");
        }

        return tablets.get(0).getQueryService().streamExecute(context, target, sql, bindVariables, transactionID, options);
    }

    @Override
    public BatchVtResultSet executeBatch(IContext context, Query.Target target, List<BoundQuery> queries, Boolean asTransaction, Long transactionId, Query.ExecuteOptions options)
        throws SQLException {

        boolean inDedicatedConn = transactionId != 0;
        return (BatchVtResultSet) retry(inDedicatedConn, target, (IQueryService qs) -> {
            try {
                BatchVtResultSet batchVtResultSet = qs.executeBatch(context, target, queries, asTransaction, transactionId, options);
                return new IInner.InnerResult(false, batchVtResultSet);
            } catch (SQLException e) {
                if (canRetry(context, e)) {
                    if (inDedicatedConn) {
                        throw e;
                    } else {
                        return IInner.DEFAULT_RETRY;
                    }
                } else {
                    throw e;
                }
            }
        }, RoleUtils.getRoleType(context)).resultSetMessage;
    }

    @Override
    public BeginBatchVtResultSet beginExecuteBatch(IContext context, Query.Target target, List<BoundQuery> queries, Boolean asTransaction, Query.ExecuteOptions options) throws SQLException {

        return (BeginBatchVtResultSet) retry(asTransaction, target, (IQueryService qs) -> {
            try {
                BeginBatchVtResultSet beginBatchVtResultSet = qs.beginExecuteBatch(context, target, queries, asTransaction, options);
                return new IInner.InnerResult(false, beginBatchVtResultSet);
            } catch (SQLException e) {
                if (canRetry(context, e)) {
                    return IInner.DEFAULT_RETRY;
                } else {
                    throw e;
                }
            }
        }, RoleUtils.getRoleType(context)).resultSetMessage;
    }

    @Override
    public void streamHealth(TabletHealthCheck thc, StreamObserver<Query.StreamHealthResponse> responseObserver) throws Exception {
        //not used here.
    }

    @Override
    public Query.ReserveBeginExecuteResponse reserveBeginExecute(IContext context, Query.Target target, List<String> preQuries, String sql, Map<String, BindVariable> bindVariables,
                                                                 Query.ExecuteOptions options) throws Exception {
        //not used.
        return null;
    }

    @Override
    public Query.ReserveExecuteResponse reserveExecute(IContext context, Query.Target target, List<String> preQueries, String sql, Map<String, BindVariable> bindVariables, Long transactionID,
                                                       Query.ExecuteOptions options) throws Exception {
        //not used.
        return null;
    }

    @Override
    public Query.ReleaseResponse release(IContext context, Query.Target target, Long transactionID, Long reservedID) throws SQLException {
        boolean inDedicatedConn = reservedID != 0 || transactionID != 0;
        return (Query.ReleaseResponse) retry(inDedicatedConn, target, (IQueryService qs) -> {
            Query.ReleaseResponse ret = qs.release(context, target, transactionID, reservedID);
            return new IInner.InnerResult(false, ret);
        }, RoleUtils.getRoleType(context)).responses;
    }

    @Override
    public void close() {
        //do nothing
    }

    private boolean canRetry(IContext context, Exception e) {
        return !context.isDone() && (e instanceof SQLRecoverableException);
    }

    private IInner.InnerResult retry(boolean inTransaction, Query.Target target, IInner inner, RoleType roleType) throws SQLException {

        if (inTransaction && target.getTabletType() != Topodata.TabletType.MASTER) {
            throw new SQLException("gateway's query service can only be used for non-transactional queries on replica and rdonly");
        }

        Set<String> invalidTablets = new HashSet<>();

        for (int i = 0; i < RETRY_COUNT + 1; i++) {
            List<TabletHealthCheck> typeTablets = ((TabletGateway) gateway).getHc().getTabletHealthChecks(target, roleType);

            if (typeTablets == null || typeTablets.size() == 0) {
                throw new SQLException("target: " + HealthCheck.keyFromTarget(target) + ". no valid tablet");
            }

            List<TabletHealthCheck> tablets;
            if (invalidTablets.isEmpty()) {
                tablets = typeTablets;
            } else {
                tablets = new ArrayList<>(typeTablets.size());
                for (TabletHealthCheck tb : typeTablets) {
                    if (!invalidTablets.contains(TopoProto.tabletAliasString(tb.getTablet().getAlias()))) {
                        tablets.add(tb);
                    }
                }
                if (tablets.size() == 0) {
                    throw new SQLException("target: " + HealthCheck.keyFromTarget(target) + ". no valid tablet");
                }
            }

            // not used for replica till now, so use a simple shuffle().
            TabletHealthCheck tablet = getRandomTablet(tablets);
            if (tablet == null) {
                throw new SQLException("no available connection");
            }

            if (tablet.getQueryService() == null) {
                invalidTablets.add(TopoProto.tabletAliasString(tablet.getTablet().getAlias()));
                continue;
            }

            if (log.isDebugEnabled()) {
                log.debug("choose tablet " + TopoProto.tabletToHumanString(tablet.getTablet()) + " for target: " + HealthCheck.keyFromTarget(target));
            }
            IInner.InnerResult ret = inner.run(tablet.getQueryService());
            if (ret.retry) {
                log.info("retry for target: " + HealthCheck.keyFromTarget(target) + ", as " + TopoProto.tabletToHumanString(tablet.getTablet()) + " is unavailable");
                invalidTablets.add(TopoProto.tabletAliasString(tablet.getTablet().getAlias()));
                continue;
            }
            return ret;
        }

        throw new SQLException("target: " + HealthCheck.keyFromTarget(target) + ". all tablets are tried, no available connection");
    }

    private TabletHealthCheck getRandomTablet(List<TabletHealthCheck> tablets) {
        if (tablets.size() > 1) {
            Collections.shuffle(tablets);
        }
        return tablets.get(0);
    }
}