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
import com.jd.jdbc.discovery.TabletHealthCheck;
import com.jd.jdbc.sqltypes.BatchVtResultSet;
import com.jd.jdbc.sqltypes.BeginVtResultSet;
import com.jd.jdbc.sqltypes.VtResultSet;
import io.grpc.ManagedChannel;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TabletQueryService extends AbstractTabletQueryService implements IQueryService, IHealthCheckQueryService {
    /**
     * @param channel
     */
    public TabletQueryService(ManagedChannel channel) {
        super(channel);
    }

    @Override
    public Query.BeginResponse begin(IContext context, Query.Target target, Query.ExecuteOptions options) {
        return blockingStub.begin(Query.BeginRequest.newBuilder().setTarget(target).setOptions(options).build());
    }

    @Override
    public Query.CommitResponse commit(IContext context, Query.Target target, Long transactionID) {
        return blockingStub.commit(Query.CommitRequest.newBuilder().setTarget(target).setTransactionId(transactionID).build());
    }

    @Override
    public Query.RollbackResponse rollback(IContext context, Query.Target target, Long transactionID) {
        return blockingStub.rollback(Query.RollbackRequest.newBuilder().setTarget(target).setTransactionId(transactionID).build());
    }

    @Override
    public BeginVtResultSet beginExecute(IContext context, Query.Target target, List<String> preQuries, String sql, Map<String, Query.BindVariable> bindVariables, Long reservedID,
                                         Query.ExecuteOptions options) throws SQLException {
        return null;
    }

    @Override
    public VtResultSet execute(IContext context, Query.Target target, String sql, Map<String, Query.BindVariable> bindVariables, Long transactionID, Long reservedID, Query.ExecuteOptions options)
        throws SQLException {
        return null;
    }

    @Override
    public StreamIterator streamExecute(IContext context, Query.Target target, String sql, Map<String, Query.BindVariable> bindVariables, Long transactionID, Query.ExecuteOptions options) {
        return null;
    }

    @Override
    public BatchVtResultSet executeBatch(IContext context, Query.Target target, List<Query.BoundQuery> queries, Boolean asTransaction, Long transactionId, Query.ExecuteOptions options)
        throws SQLException {
        //not used.
        return null;
    }

    @Override
    public void streamHealth(TabletHealthCheck thc, io.grpc.stub.StreamObserver<io.vitess.proto.Query.StreamHealthResponse> responseObserver) {
        Query.StreamHealthRequest.Builder builder = Query.StreamHealthRequest.newBuilder();
        Query.StreamHealthRequest request = builder.build();
        asyncStub.streamHealth(request, responseObserver);
    }

    @Override
    public Query.ReserveBeginExecuteResponse reserveBeginExecute(IContext context, Query.Target target, List<String> preQuries, String sql, Map<String, Query.BindVariable> bindVariables,
                                                                 Query.ExecuteOptions options) throws Exception {
        Query.ReserveBeginExecuteRequest.Builder builder = Query.ReserveBeginExecuteRequest.newBuilder();
        builder.setTarget(target);
        for (int i = 0; i < preQuries.size(); i++) {
            builder.setPreQueries(i, preQuries.get(i));
        }
        builder.setQuery(Query.BoundQuery.newBuilder().setSql(sql).putAllBindVariables(bindVariables).build());
        builder.setOptions(options);
        Query.ReserveBeginExecuteRequest request = builder.build();
        Query.ReserveBeginExecuteResponse rsp = blockingStub.reserveBeginExecute(request);
        if (rsp.hasError()) {
            throw new SQLException(rsp.getError().getMessage());
        }
        return rsp;
    }

    @Override
    public Query.ReserveExecuteResponse reserveExecute(IContext context, Query.Target target, List<String> preQueries, String sql, Map<String, Query.BindVariable> bindVariables, Long transactionID,
                                                       Query.ExecuteOptions options) throws Exception {
        Query.ReserveExecuteRequest.Builder builder = Query.ReserveExecuteRequest.newBuilder();
        builder.setTarget(target);
        for (int i = 0; i < preQueries.size(); i++) {
            builder.setPreQueries(i, preQueries.get(i));
        }
        builder.setQuery(Query.BoundQuery.newBuilder().setSql(sql).putAllBindVariables(bindVariables).build());
        builder.setOptions(options);
        Query.ReserveExecuteRequest request = builder.build();
        Query.ReserveExecuteResponse rsp = blockingStub.reserveExecute(request);
        if (rsp.hasError()) {
            throw new SQLException(rsp.getError().getMessage());
        }
        return rsp;
    }

    @Override
    public Query.ReleaseResponse release(IContext context, Query.Target target, Long transactionID, Long reservedID) {
        Query.ReleaseRequest.Builder builder = Query.ReleaseRequest.newBuilder();
        builder.setTarget(target);
        builder.setTransactionId(transactionID);
        builder.setReservedId(reservedID);
        Query.ReleaseRequest request = builder.build();
        return blockingStub.release(request);
    }

    @Override
    public void close() {
        try {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return;
        }
    }
}
