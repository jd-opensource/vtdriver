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

package com.jd.jdbc;

import com.jd.jdbc.context.IContext;
import com.jd.jdbc.queryservice.StreamIterator;
import com.jd.jdbc.session.SafeSession;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.parser.VitessRuntimeException;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtRowList;
import com.jd.jdbc.srvtopo.ResolvedShard;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;

public interface IExecute {

    /**
     * @param ctx
     * @param method
     * @param safeSession
     * @param stmt
     * @param bindVariableMap
     * @return
     * @throws Exception
     */
    VtRowList execute(IContext ctx, String method, SafeSession safeSession, String keyspace, SQLStatement stmt, Map<String, Query.BindVariable> bindVariableMap) throws Exception;

    /**
     * @param ctx
     * @param method
     * @param safeSession
     * @param stmt
     * @param bindVariableMap
     * @return
     * @throws Exception
     */
    VtRowList streamExecute(IContext ctx, String method, SafeSession safeSession, String keyspace, SQLStatement stmt, Map<String, Query.BindVariable> bindVariableMap) throws Exception;

    List<VtRowList> batchExecute(IContext ctx, String method, SafeSession safeSession, String keyspace, List<SQLStatement> batchStmts, List<Map<String, Query.BindVariable>> bindVariableMapList)
        throws Exception;

    /**
     * @param ctx
     * @param rss
     * @param queries
     * @param safeSession
     * @param autocommit
     * @param ignoreMaxMemoryRows
     * @return
     */
    ExecuteMultiShardResponse executeMultiShard(IContext ctx, List<ResolvedShard> rss, List<Query.BoundQuery> queries, SafeSession safeSession, Boolean autocommit, Boolean ignoreMaxMemoryRows)
        throws SQLException;

    ExecuteBatchMultiShardResponse executeBatchMultiShard(IContext ctx, List<ResolvedShard> rss, List<List<Query.BoundQuery>> queries, SafeSession safeSession, Boolean autocommit,
                                                          Boolean ignoreMaxMemoryRows, Boolean asTransaction) throws SQLException;

    /**
     * @param ctx
     * @param rss
     * @param queries
     * @param safeSession
     * @return
     */
    List<StreamIterator> streamExecuteMultiShard(IContext ctx, List<ResolvedShard> rss, List<Query.BoundQuery> queries, SafeSession safeSession) throws SQLException;

    interface VtStream {
        VtRowList fetch(boolean wantFields) throws SQLException;

        void close() throws SQLException;
    }

    /* TODO
    ExecuteLock(ctx context.Context, rs *srvtopo.ResolvedShard, query *querypb.BoundQuery, session *SafeSession) (*sqltypes.Result, error)

    // TODO: remove when resolver is gone
    ParseDestinationTarget(targetString string) (string, topodatapb.TabletType, key.Destination, error)
    */

    @Getter
    class ExecuteMultiShardResponse {
        private final VtRowList vtRowList;

        public ExecuteMultiShardResponse(VtRowList vtRowList) {
            this.vtRowList = vtRowList;
        }

        public ExecuteMultiShardResponse setUpdate() {
            this.vtRowList.setDML();
            return this;
        }
    }

    @Getter
    @AllArgsConstructor
    class ExecuteBatchResultSet {
        private final List<VtResultSet> vtResultSetList;

        private int index;

        public ExecuteBatchResultSet(List<VtResultSet> vtResultSetList) {
            this.vtResultSetList = vtResultSetList;
            this.index = 0;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public VtResultSet getAndMoveIndex() {
            VtResultSet vtResultSet = vtResultSetList.get(index);
            index++;
            if (vtResultSetList.size() < index) {
                throw new VitessRuntimeException("size Error!,current index=" + index);
            }
            return vtResultSet;
        }
    }

    @Getter
    @AllArgsConstructor
    class ExecuteBatchMultiShardResponse {
        private final List<List<VtResultSet>> vtResultSetList;

        private final List<ResolvedShard> rss;
    }

    @Getter
    @AllArgsConstructor
    class ResolvedShardQuery {
        List<ResolvedShard> rss;

        List<Query.BoundQuery> queries;
    }
}
