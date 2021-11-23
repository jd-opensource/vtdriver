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
import com.jd.jdbc.discovery.TabletHealthCheck;
import io.grpc.stub.StreamObserver;
import io.vitess.proto.Query;

public interface AsyncQueryService {
    // Transaction management

    /**
     * returns the transaction id to use for further operations
     *
     * @param request
     * @param responseStreamObserver
     */
    void begin(Query.BeginRequest request, StreamObserver<Query.BeginResponse> responseStreamObserver);

    /**
     * commits the current transaction
     *
     * @param request
     * @param responseStreamObserver
     */
    void commit(Query.CommitRequest request, StreamObserver<Query.CommitResponse> responseStreamObserver);

    /**
     * aborts the current transaction
     *
     * @param request
     * @param responseStreamObserver
     */
    void rollback(Query.RollbackRequest request, StreamObserver<Query.RollbackResponse> responseStreamObserver);

    /**
     * prepares the specified transaction.
     *
     * @param request
     * @param responseStreamObserver
     */
    void prepare(Query.PrepareRequest request, StreamObserver<Query.RollbackResponse> responseStreamObserver);

    /**
     * commits the prepared transaction.
     *
     * @param request
     * @param responseStreamObserver
     */
    void commitPrepared(Query.CommitPreparedRequest request, StreamObserver<Query.CommitPreparedResponse> responseStreamObserver);

    /**
     * rolls back the prepared transaction.
     *
     * @param request
     * @param responseStreamObserver
     */
    void rollbackPrepared(Query.RollbackPreparedRequest request, StreamObserver<Query.RollbackPreparedResponse> responseStreamObserver);

    /**
     * creates the metadata for a 2PC transaction.
     *
     * @param request
     * @param responseStreamObserver
     */
    void createTransaction(Query.CreateTransactionRequest request, StreamObserver<Query.CreateTransactionResponse> responseStreamObserver);

    /**
     * atomically commits the transaction along with the
     * decision to commit the associated 2pc transaction.
     *
     * @param request
     * @param responseStreamObserver
     */
    void startCommit(Query.StartCommitRequest request, StreamObserver<Query.StartCommitResponse> responseStreamObserver);

    /**
     * transitions the 2pc transaction to the Rollback state.
     * If a transaction id is provided, that transaction is also rolled back.
     *
     * @param request
     * @param responseStreamObserver
     */
    void setRollback(Query.SetRollbackRequest request, StreamObserver<Query.SetRollbackResponse> responseStreamObserver);

    /**
     * deletes the 2pc transaction metadata essentially resolving it.
     *
     * @param request
     * @param responseStreamObserver
     */
    void concludeTransaction(Query.ConcludeTransactionRequest request, StreamObserver<Query.ConcludeTransactionResponse> responseStreamObserver);

    /**
     * returns the metadata for the specified dtid.
     *
     * @param request
     * @param responseStreamObserver
     */
    void readTransaction(Query.ReadTransactionRequest request, StreamObserver<Query.ReadTransactionResponse> responseStreamObserver);

    /**
     * Query execution
     *
     * @param request
     * @param responseStreamObserver
     */
    void execute(Query.ExecuteRequest request, StreamObserver<Query.ExecuteResponse> responseStreamObserver);

    /**
     * Currently always called with transactionID = 0
     *
     * @param request
     * @param responseStreamObserver
     */
    void streamExecute(Query.StreamExecuteRequest request, StreamObserver<Query.StreamExecuteResponse> responseStreamObserver);

    /**
     * Currently always called with transactionID = 0
     *
     * @param request
     * @param responseStreamObserver
     */
    void executeBatch(Query.ExecuteBatchRequest request, StreamObserver<Query.ExecuteBatchResponse> responseStreamObserver);

    /**
     * Combo methods, they also return the transactionID from the
     * Begin part. If err != nil, the transactionID may still be
     * non-zero, and needs to be propagated back (like for a DB
     * Integrity Error)
     *
     * @param request
     * @param responseStreamObserver
     */
    void beginExecute(Query.BeginExecuteRequest request, StreamObserver<Query.BeginExecuteResponse> responseStreamObserver);

    /**
     * Combo methods, they also return the transactionID from the
     * Begin part. If err != nil, the transactionID may still be
     * non-zero, and needs to be propagated back (like for a DB
     * Integrity Error)
     *
     * @param request
     * @param responseStreamObserver
     */
    void beginExecuteBatch(Query.BeginExecuteBatchRequest request, StreamObserver<Query.BeginExecuteBatchResponse> responseStreamObserver);

    /**
     * Messaging methods.
     *
     * @param request
     * @param responseStreamObserver
     */
    void messageStream(Query.MessageStreamRequest request, StreamObserver<Query.MessageStreamResponse> responseStreamObserver);

    /**
     * Messaging methods.
     *
     * @param request
     * @param responseStreamObserver
     */
    void messageAck(Query.MessageAckRequest request, StreamObserver<Query.MessageAckResponse> responseStreamObserver);

    /**
     * streams VReplication events based on the specified filter.
     *
     * @param request
     * @param responseStreamObserver
     */
    void vStream(Binlogdata.VStreamRequest request, StreamObserver<Binlogdata.VStreamResponse> responseStreamObserver);

    /**
     * streams rows of a table from the specified starting point.
     *
     * @param request
     * @param responseStreamObserver
     */
    void vStreamRows(Binlogdata.VStreamRowsRequest request, StreamObserver<Binlogdata.VStreamRowsResponse> responseStreamObserver);

    /**
     * streams results along with the gtid of the snapshot.
     *
     * @param request
     * @param responseStreamObserver
     */
    void vStreamResults(Binlogdata.VStreamResultsRequest request, StreamObserver<Binlogdata.VStreamResultsResponse> responseStreamObserver);

    /**
     * streams health status.
     *
     * @param thc
     * @param responseObserver
     */
    void streamHealth(TabletHealthCheck thc, StreamObserver<Query.StreamHealthResponse> responseObserver);

    // HandlePanic will be called if any of the functions panic.
    // HandlePanic(err *error)

    /**
     * @param request
     * @param responseStreamObserver
     */
    void reserveBeginExecute(Query.ReserveBeginExecuteRequest request, StreamObserver<Query.ReserveBeginExecuteResponse> responseStreamObserver);

    /**
     * @param request
     * @param responseStreamObserver
     */
    void reserveExecute(Query.ReserveExecuteRequest request, StreamObserver<Query.ReserveExecuteResponse> responseStreamObserver);

    /**
     * @param request
     * @param responseStreamObserver
     */
    void release(Query.ReleaseRequest request, StreamObserver<Query.ReleaseResponse> responseStreamObserver);

    /**
     * must be called for releasing resources.
     */
    void close();
}
