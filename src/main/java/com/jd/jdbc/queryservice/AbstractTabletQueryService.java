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

import binlogdata.Binlogdata;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.discovery.TabletHealthCheck;
import com.jd.jdbc.sqltypes.BeginBatchVtResultSet;
import com.jd.jdbc.sqltypes.VtResultSetMessage;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.BoundQuery;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.vitess.proto.Query;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import queryservice.QueryGrpc;

public abstract class AbstractTabletQueryService extends AbstractQueryService implements IHealthCheckQueryService {
    protected final ManagedChannel channel;

    protected final QueryGrpc.QueryBlockingStub blockingStub;

    protected final QueryGrpc.QueryStub asyncStub;

    protected final QueryGrpc.QueryFutureStub futureStub;

    public AbstractTabletQueryService(ManagedChannel channel) {
        this.channel = channel;
        this.blockingStub = QueryGrpc.newBlockingStub(channel);
        this.asyncStub = QueryGrpc.newStub(channel);
        this.futureStub = QueryGrpc.newFutureStub(channel);
    }

    /**
     * returns the transaction id to use for further operations
     *
     * @param target
     * @param options
     * @return
     */
    @Override
    public abstract Query.BeginResponse begin(IContext context, Query.Target target, Query.ExecuteOptions options);

    /**
     * commits the current transaction
     *
     * @param target
     * @param transactionId
     * @return
     */
    @Override
    public abstract Query.CommitResponse commit(IContext context, Query.Target target, Long transactionId);

    /**
     * aborts the current transaction
     *
     * @param target
     * @param transactionId
     * @return
     */
    @Override
    public abstract Query.RollbackResponse rollback(IContext context, Query.Target target, Long transactionId);

    /**
     * prepares the specified transaction.
     *
     * @param target
     * @param transactionId
     * @param dtid
     * @return
     */
    @Override
    public Query.PrepareResponse prepare(Query.Target target, Long transactionId, String dtid) {
        return null;
    }

    /**
     * commits the prepared transaction.
     *
     * @param target
     * @param dtid
     * @return
     */
    @Override
    public Query.CommitPreparedResponse commitPrepared(Query.Target target, String dtid) {
        return null;
    }

    /**
     * rolls back the prepared transaction.
     *
     * @param target
     * @param dtid
     * @param originalId
     * @return
     */
    @Override
    public Query.RollbackPreparedResponse rollbackPrepared(Query.Target target, String dtid, Long originalId) {
        return null;
    }

    /**
     * creates the metadata for a 2PC transaction.
     *
     * @param target
     * @param dtid
     * @param participants
     * @return
     */
    @Override
    public Query.CreateTransactionResponse createTransaction(Query.Target target, String dtid, List<Query.Target> participants) {
        return null;
    }

    /**
     * atomically commits the transaction along with the
     * decision to commit the associated 2pc transaction.
     *
     * @param target
     * @param transactionId
     * @param dtid
     * @return
     */
    @Override
    public Query.StartCommitResponse startCommit(Query.Target target, Long transactionId, String dtid) {
        return null;
    }

    /**
     * transitions the 2pc transaction to the Rollback state.
     * If a transaction id is provided, that transaction is also rolled back.
     *
     * @param target
     * @param dtid
     * @param transactionId
     * @return
     */
    @Override
    public Query.SetRollbackResponse setRollback(Query.Target target, String dtid, Long transactionId) {
        return null;
    }

    /**
     * deletes the 2pc transaction metadata essentially resolving it.
     *
     * @param target
     * @param dtid
     * @return
     */
    @Override
    public Query.ConcludeTransactionResponse concludeTransaction(Query.Target target, String dtid) {
        return null;
    }

    /**
     * returns the metadata for the specified dtid.
     *
     * @param target
     * @param dtid
     * @return
     */
    @Override
    public Query.ReadTransactionResponse readTransaction(Query.Target target, String dtid) {
        return null;
    }

    /**
     * Query execution
     *
     * @param target
     * @param sql
     * @param bindVariables
     * @param transactionId
     * @param reservedId
     * @param options
     * @return
     */
    @Override
    public abstract VtResultSetMessage execute(IContext context, Query.Target target, String sql, Map<String, BindVariable> bindVariables, Long transactionId, Long reservedId,
                                               Query.ExecuteOptions options) throws Exception;

    /**
     * Currently always called with transactionID = 0
     *
     * @param target
     * @param sql
     * @param bindVariables
     * @param transactionId
     * @param options
     * @return
     */
    @Override
    public abstract StreamIterator streamExecute(IContext context, Query.Target target, String sql, Map<String, BindVariable> bindVariables, Long transactionId, Query.ExecuteOptions options);

    /**
     * Currently always called with transactionID = 0
     *
     * @param target
     * @param queries
     * @param asTransaction
     * @param transactionId
     * @param options
     * @return
     */
    @Override
    public Query.ExecuteBatchResponse executeBatch(Query.Target target, List<BoundQuery> queries, Boolean asTransaction, Long transactionId, Query.ExecuteOptions options) {
        return null;
    }

    /**
     * Combo methods, they also return the transactionID from the
     * Begin part. If err != nil, the transactionID may still be
     * non-zero, and needs to be propagated back (like for a DB
     * Integrity Error)
     *
     * @param target
     * @param preQueries
     * @param sql
     * @param bindVariables
     * @param reservedId
     * @param options
     * @return
     */
    @Override
    public abstract VtResultSetMessage beginExecute(IContext context, Query.Target target, List<String> preQueries, String sql, Map<String, BindVariable> bindVariables, Long reservedId,
                                                    Query.ExecuteOptions options) throws Exception;

    /**
     * Combo methods, they also return the transactionID from the
     * Begin part. If err != nil, the transactionID may still be
     * non-zero, and needs to be propagated back (like for a DB
     * Integrity Error)
     *
     * @param target
     * @param queries
     * @param asTransaction
     * @param options
     * @return
     */
    @Override
    public BeginBatchVtResultSet beginExecuteBatch(IContext context, Query.Target target, List<BoundQuery> queries, Boolean asTransaction, Query.ExecuteOptions options) {
        return null;
    }

    /**
     * Messaging methods.
     *
     * @param target
     * @param name
     * @return
     */
    @Override
    public Query.MessageStreamResponse messageStream(Query.Target target, String name) {
        return null;
    }

    /**
     * Messaging methods.
     *
     * @param target
     * @param name
     * @param ids
     * @return
     */
    @Override
    public Query.MessageAckResponse messageAck(Query.Target target, String name, List<Query.Value> ids) {
        return null;
    }

    /**
     * streams VReplication events based on the specified filter.
     *
     * @param target
     * @param startPos
     * @param tableLastPks
     * @param filter
     * @return
     */
    @Override
    public Iterator<Binlogdata.VStreamResponse> vStream(Query.Target target, String startPos, List<Binlogdata.TableLastPK> tableLastPks, Binlogdata.Filter filter) {
        return null;
    }

    /**
     * streams rows of a table from the specified starting point.
     *
     * @param target
     * @param query
     * @param lastPk
     * @return
     */
    @Override
    public Iterator<Binlogdata.VStreamRowsResponse> vStreamRows(Query.Target target, String query, Query.QueryResult lastPk) {
        return null;
    }

    /**
     * streams results along with the gtid of the snapshot.
     *
     * @param target
     * @param query
     * @return
     */
    @Override
    public Iterator<Binlogdata.VStreamResultsResponse> vStreamResults(Query.Target target, String query) {
        return null;
    }

    /**
     * streams health status.
     *
     * @return
     */
    @Override
    public Iterator<Query.StreamHealthResponse> streamHealth() {
        return null;
    }


    /**
     * @param target
     * @param preQuries
     * @param sql
     * @param bindVariables
     * @param options
     * @return
     */
    @Override
    public Query.ReserveBeginExecuteResponse reserveBeginExecute(IContext context, Query.Target target, List<String> preQuries, String sql, Map<String, BindVariable> bindVariables,
                                                                 Query.ExecuteOptions options) throws Exception {
        return null;
    }

    /**
     * @param target
     * @param preQueries
     * @param sql
     * @param bindVariables
     * @param transactionID
     * @param options
     * @return
     */
    @Override
    public Query.ReserveExecuteResponse reserveExecute(IContext context, Query.Target target, List<String> preQueries, String sql, Map<String, BindVariable> bindVariables, Long transactionID,
                                                       Query.ExecuteOptions options) throws Exception {
        return null;
    }

    /**
     * @param target
     * @param transactionID
     * @param reservedID
     * @return
     */
    @Override
    public Query.ReleaseResponse release(IContext context, Query.Target target, Long transactionID, Long reservedID) {
        return null;
    }

    /**
     * must be called for releasing resources.
     */
    @Override
    public void close() {

    }

    /**
     * returns the transaction id to use for further operations
     *
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void begin(Query.BeginRequest request, StreamObserver<Query.BeginResponse> responseStreamObserver) {

    }

    /**
     * commits the current transaction
     *
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void commit(Query.CommitRequest request, StreamObserver<Query.CommitResponse> responseStreamObserver) {

    }

    /**
     * aborts the current transaction
     *
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void rollback(Query.RollbackRequest request, StreamObserver<Query.RollbackResponse> responseStreamObserver) {

    }

    /**
     * prepares the specified transaction.
     *
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void prepare(Query.PrepareRequest request, StreamObserver<Query.RollbackResponse> responseStreamObserver) {

    }

    /**
     * commits the prepared transaction.
     *
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void commitPrepared(Query.CommitPreparedRequest request, StreamObserver<Query.CommitPreparedResponse> responseStreamObserver) {

    }

    /**
     * rolls back the prepared transaction.
     *
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void rollbackPrepared(Query.RollbackPreparedRequest request, StreamObserver<Query.RollbackPreparedResponse> responseStreamObserver) {

    }

    /**
     * creates the metadata for a 2PC transaction.
     *
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void createTransaction(Query.CreateTransactionRequest request, StreamObserver<Query.CreateTransactionResponse> responseStreamObserver) {

    }

    /**
     * atomically commits the transaction along with the
     * decision to commit the associated 2pc transaction.
     *
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void startCommit(Query.StartCommitRequest request, StreamObserver<Query.StartCommitResponse> responseStreamObserver) {

    }

    /**
     * transitions the 2pc transaction to the Rollback state.
     * If a transaction id is provided, that transaction is also rolled back.
     *
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void setRollback(Query.SetRollbackRequest request, StreamObserver<Query.SetRollbackResponse> responseStreamObserver) {

    }

    /**
     * deletes the 2pc transaction metadata essentially resolving it.
     *
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void concludeTransaction(Query.ConcludeTransactionRequest request, StreamObserver<Query.ConcludeTransactionResponse> responseStreamObserver) {

    }

    /**
     * returns the metadata for the specified dtid.
     *
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void readTransaction(Query.ReadTransactionRequest request, StreamObserver<Query.ReadTransactionResponse> responseStreamObserver) {

    }

    /**
     * Query execution
     *
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void execute(Query.ExecuteRequest request, StreamObserver<Query.ExecuteResponse> responseStreamObserver) {

    }

    /**
     * Currently always called with transactionID = 0
     *
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void streamExecute(Query.StreamExecuteRequest request, StreamObserver<Query.StreamExecuteResponse> responseStreamObserver) {

    }

    /**
     * Currently always called with transactionID = 0
     *
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void executeBatch(Query.ExecuteBatchRequest request, StreamObserver<Query.ExecuteBatchResponse> responseStreamObserver) {

    }

    /**
     * Combo methods, they also return the transactionID from the
     * Begin part. If err != nil, the transactionID may still be
     * non-zero, and needs to be propagated back (like for a DB
     * Integrity Error)
     *
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void beginExecute(Query.BeginExecuteRequest request, StreamObserver<Query.BeginExecuteResponse> responseStreamObserver) {

    }

    /**
     * Combo methods, they also return the transactionID from the
     * Begin part. If err != nil, the transactionID may still be
     * non-zero, and needs to be propagated back (like for a DB
     * Integrity Error)
     *
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void beginExecuteBatch(Query.BeginExecuteBatchRequest request, StreamObserver<Query.BeginExecuteBatchResponse> responseStreamObserver) {

    }

    /**
     * Messaging methods.
     *
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void messageStream(Query.MessageStreamRequest request, StreamObserver<Query.MessageStreamResponse> responseStreamObserver) {

    }

    /**
     * Messaging methods.
     *
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void messageAck(Query.MessageAckRequest request, StreamObserver<Query.MessageAckResponse> responseStreamObserver) {

    }

    /**
     * streams VReplication events based on the specified filter.
     *
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void vStream(Binlogdata.VStreamRequest request, StreamObserver<Binlogdata.VStreamResponse> responseStreamObserver) {

    }

    /**
     * streams rows of a table from the specified starting point.
     *
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void vStreamRows(Binlogdata.VStreamRowsRequest request, StreamObserver<Binlogdata.VStreamRowsResponse> responseStreamObserver) {

    }

    /**
     * streams results along with the gtid of the snapshot.
     *
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void vStreamResults(Binlogdata.VStreamResultsRequest request, StreamObserver<Binlogdata.VStreamResultsResponse> responseStreamObserver) {

    }

    /**
     * streams health status.
     *
     * @param thc
     * @param responseObserver
     */
    @Override
    public void streamHealth(TabletHealthCheck thc, StreamObserver<Query.StreamHealthResponse> responseObserver) {

    }


    /**
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void reserveBeginExecute(Query.ReserveBeginExecuteRequest request, StreamObserver<Query.ReserveBeginExecuteResponse> responseStreamObserver) {

    }

    /**
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void reserveExecute(Query.ReserveExecuteRequest request, StreamObserver<Query.ReserveExecuteResponse> responseStreamObserver) {

    }

    /**
     * @param request
     * @param responseStreamObserver
     */
    @Override
    public void release(Query.ReleaseRequest request, StreamObserver<Query.ReleaseResponse> responseStreamObserver) {

    }
}
