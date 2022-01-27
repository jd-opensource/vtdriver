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
import com.jd.jdbc.sqltypes.BeginBatchVtResultSet;
import com.jd.jdbc.sqltypes.VtResultSetMessage;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.BoundQuery;
import io.vitess.proto.Query;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface BlockingQueryService {
    // Transaction management

    /**
     * returns the transaction id to use for further operations
     *
     * @param target
     * @param options
     * @return
     */
    Query.BeginResponse begin(IContext context, Query.Target target, Query.ExecuteOptions options);

    /**
     * commits the current transaction
     *
     * @param target
     * @param transactionId
     * @return
     */
    Query.CommitResponse commit(IContext context, Query.Target target, Long transactionId);

    /**
     * aborts the current transaction
     *
     * @param target
     * @param transactionId
     * @return
     */
    Query.RollbackResponse rollback(IContext context, Query.Target target, Long transactionId);

    /**
     * prepares the specified transaction.
     *
     * @param target
     * @param transactionId
     * @param dtid
     * @return
     */
    Query.PrepareResponse prepare(Query.Target target, Long transactionId, String dtid);

    /**
     * commits the prepared transaction.
     *
     * @param target
     * @param dtid
     * @return
     */
    Query.CommitPreparedResponse commitPrepared(Query.Target target, String dtid);

    /**
     * rolls back the prepared transaction.
     *
     * @param target
     * @param dtid
     * @param originalId
     * @return
     */
    Query.RollbackPreparedResponse rollbackPrepared(Query.Target target, String dtid, Long originalId);

    /**
     * creates the metadata for a 2PC transaction.
     *
     * @param target
     * @param dtid
     * @param participants
     * @return
     */
    Query.CreateTransactionResponse createTransaction(Query.Target target, String dtid, List<Query.Target> participants);

    /**
     * atomically commits the transaction along with the
     * decision to commit the associated 2pc transaction.
     *
     * @param target
     * @param transactionId
     * @param dtid
     * @return
     */
    Query.StartCommitResponse startCommit(Query.Target target, Long transactionId, String dtid);

    /**
     * transitions the 2pc transaction to the Rollback state.
     * If a transaction id is provided, that transaction is also rolled back.
     *
     * @param target
     * @param dtid
     * @param transactionId
     * @return
     */
    Query.SetRollbackResponse setRollback(Query.Target target, String dtid, Long transactionId);

    /**
     * deletes the 2pc transaction metadata essentially resolving it.
     *
     * @param target
     * @param dtid
     * @return
     */
    Query.ConcludeTransactionResponse concludeTransaction(Query.Target target, String dtid);

    /**
     * returns the metadata for the specified dtid.
     *
     * @param target
     * @param dtid
     * @return
     */
    Query.ReadTransactionResponse readTransaction(Query.Target target, String dtid);

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
    VtResultSetMessage execute(IContext context, Query.Target target, String sql, Map<String, BindVariable> bindVariables, Long transactionId, Long reservedId, Query.ExecuteOptions options)
        throws Exception;

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
    StreamIterator streamExecute(IContext context, Query.Target target, String sql, Map<String, BindVariable> bindVariables, Long transactionId, Query.ExecuteOptions options);

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
    Query.ExecuteBatchResponse executeBatch(Query.Target target, List<BoundQuery> queries, Boolean asTransaction, Long transactionId, Query.ExecuteOptions options);

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
    VtResultSetMessage beginExecute(IContext context, Query.Target target, List<String> preQueries, String sql, Map<String, BindVariable> bindVariables, Long reservedId,
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
    BeginBatchVtResultSet beginExecuteBatch(IContext context, Query.Target target, List<BoundQuery> queries, Boolean asTransaction, Query.ExecuteOptions options);

    /**
     * Messaging methods.
     *
     * @param target
     * @param name
     * @return
     */
    Query.MessageStreamResponse messageStream(Query.Target target, String name);

    /**
     * Messaging methods.
     *
     * @param target
     * @param name
     * @param ids
     * @return
     */
    Query.MessageAckResponse messageAck(Query.Target target, String name, List<Query.Value> ids);

    /**
     * streams VReplication events based on the specified filter.
     *
     * @param target
     * @param startPos
     * @param tableLastPks
     * @param filter
     * @return
     */
    Iterator<Binlogdata.VStreamResponse> vStream(Query.Target target, String startPos, List<Binlogdata.TableLastPK> tableLastPks, Binlogdata.Filter filter);

    /**
     * streams rows of a table from the specified starting point.
     *
     * @param target
     * @param query
     * @param lastPk
     * @return
     */
    Iterator<Binlogdata.VStreamRowsResponse> vStreamRows(Query.Target target, String query, Query.QueryResult lastPk);

    /**
     * streams results along with the gtid of the snapshot.
     *
     * @param target
     * @param query
     * @return
     */
    Iterator<Binlogdata.VStreamResultsResponse> vStreamResults(Query.Target target, String query);

    /**
     * streams health status.
     *
     * @return
     */
    Iterator<Query.StreamHealthResponse> streamHealth();

    // HandlePanic will be called if any of the functions panic.
    // HandlePanic(err *error)

    /**
     * @param target
     * @param preQuries
     * @param sql
     * @param bindVariables
     * @param options
     * @return
     */
    Query.ReserveBeginExecuteResponse reserveBeginExecute(IContext context, Query.Target target, List<String> preQuries, String sql, Map<String, BindVariable> bindVariables,
                                                          Query.ExecuteOptions options) throws Exception;

    /**
     * @param target
     * @param preQueries
     * @param sql
     * @param bindVariables
     * @param transactionID
     * @param options
     * @return
     */
    Query.ReserveExecuteResponse reserveExecute(IContext context, Query.Target target, List<String> preQueries, String sql, Map<String, BindVariable> bindVariables, Long transactionID,
                                                Query.ExecuteOptions options) throws Exception;

    /**
     * @param target
     * @param transactionID
     * @param reservedID
     * @return
     */
    Query.ReleaseResponse release(IContext context, Query.Target target, Long transactionID, Long reservedID);

    /**
     * must be called for releasing resources.
     */
    void close();
}
