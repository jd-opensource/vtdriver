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
import com.jd.jdbc.sqltypes.BatchVtResultSet;
import com.jd.jdbc.sqltypes.BeginBatchVtResultSet;
import com.jd.jdbc.sqltypes.BeginVtResultSet;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.BoundQuery;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface IQueryService extends IParentQueryService {
    Query.BeginResponse begin(IContext context, Query.Target target, Query.ExecuteOptions options) throws Exception;

    Query.CommitResponse commit(IContext context, Query.Target target, Long transactionID) throws SQLException;

    Query.RollbackResponse rollback(IContext context, Query.Target target, Long transactionID) throws SQLException;

    BeginVtResultSet beginExecute(IContext context, Query.Target target, List<String> preQuries, String sql, Map<String, BindVariable> bindVariables, Long reservedID,
                                  Query.ExecuteOptions options) throws SQLException;

    VtResultSet execute(IContext context, Query.Target target, String sql, Map<String, BindVariable> bindVariables, Long transactionID, Long reservedID, Query.ExecuteOptions options)
        throws SQLException;

    StreamIterator streamExecute(IContext context, Query.Target target, String sql, Map<String, BindVariable> bindVariables, Long transactionID, Query.ExecuteOptions options)
        throws SQLException;

    BatchVtResultSet executeBatch(IContext context, Query.Target target, List<BoundQuery> queries, Boolean asTransaction, Long transactionId, Query.ExecuteOptions options) throws SQLException;

    BeginBatchVtResultSet beginExecuteBatch(IContext context, Query.Target target, List<BoundQuery> queries, Boolean asTransaction, Query.ExecuteOptions options) throws SQLException;

    Query.ReserveBeginExecuteResponse reserveBeginExecute(IContext context, Query.Target target, List<String> preQuries, String sql, Map<String, BindVariable> bindVariables,
                                                          Query.ExecuteOptions options) throws Exception;

    Query.ReserveExecuteResponse reserveExecute(IContext context, Query.Target target, List<String> preQueries, String sql, Map<String, BindVariable> bindVariables, Long transactionID,
                                                Query.ExecuteOptions options) throws Exception;

    Query.ReleaseResponse release(IContext context, Query.Target target, Long transactionID, Long reservedID) throws SQLException;
}
