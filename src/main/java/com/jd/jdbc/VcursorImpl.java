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

import com.jd.jdbc.IExecute.ExecuteMultiShardResponse;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.queryservice.StreamIterator;
import com.jd.jdbc.session.SafeSession;
import com.jd.jdbc.sqlparser.Comment;
import com.jd.jdbc.sqltypes.VtRowList;
import com.jd.jdbc.srvtopo.ResolvedShard;
import com.jd.jdbc.srvtopo.Resolver;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Data;

import static com.jd.jdbc.common.Constant.DRIVER_PROPERTY_ROLE_KEY;

@Data
public class VcursorImpl implements Vcursor {
    private final Integer maxMemoryRows = 300000;

    private IContext ctx;

    private SafeSession safeSession;

    private Comment comment;

    private IExecute executor;

    private VSchemaManager vm;

    private Resolver resolver;

    private Boolean ignoreMaxMemoryRows;

    private Boolean rollbackOnPartialExec;

    public VcursorImpl(IContext ctx, SafeSession safeSession, Comment comment, IExecute executor, VSchemaManager vm, Resolver resolver) {
        this.ctx = ctx;
        this.safeSession = safeSession;
        this.comment = comment;
        this.executor = executor;
        this.vm = vm;
        this.resolver = resolver;
        this.ignoreMaxMemoryRows = Boolean.FALSE;
        this.rollbackOnPartialExec = Boolean.FALSE;
    }

    @Override
    public Integer maxMemoryRows() {
        return this.maxMemoryRows;
    }

    @Override
    public Boolean exceedsMaxMemoryRows(Integer numRows) {
        return !this.ignoreMaxMemoryRows && numRows > this.maxMemoryRows;
    }

    @Override
    public Boolean autocommitApproval() {
        return this.safeSession.autocommitApproval();
    }

    /**
     * Shard-level functions.
     *
     * @param rss
     * @param queries
     * @param rollbackOnError
     * @param canAutocommit
     * @return
     */
    @Override
    public ExecuteMultiShardResponse executeMultiShard(List<ResolvedShard> rss, List<Query.BoundQuery> queries, Boolean rollbackOnError, Boolean canAutocommit) throws SQLException {
        ExecuteMultiShardResponse response = this.executor.executeMultiShard(this.ctx, rss, queries, safeSession, canAutocommit, true);
        if (rollbackOnError) {
            this.rollbackOnPartialExec = true;
        }
        return response;
    }

    @Override
    public IExecute.ExecuteBatchMultiShardResponse executeBatchMultiShard(List<ResolvedShard> rss, List<List<Query.BoundQuery>> queries, Boolean rollbackOnError, Boolean canAutocommit)
        throws SQLException {
        return this.executor.executeBatchMultiShard(this.ctx, rss, queries, safeSession, canAutocommit, true, false);
    }

    @Override
    public List<StreamIterator> streamExecuteMultiShard(List<ResolvedShard> rss, List<Query.BoundQuery> queries) throws SQLException {
        return this.executor.streamExecuteMultiShard(this.ctx, rss, queries, safeSession);
    }

    /**
     * Resolver methods, from key.Destination to srvtopo.ResolvedShard.
     * Will replace all of the Topo functions.
     *
     * @param keyspace
     * @param ids
     * @param destinations
     * @return
     * @throws Exception
     */
    @Override
    public Resolver.ResolveDestinationResult resolveDestinations(String keyspace, List<Query.Value> ids, List<Destination> destinations) throws SQLException {
        return this.resolver.resolveDestinations(this.ctx, keyspace, (Topodata.TabletType) this.ctx.getContextValue(DRIVER_PROPERTY_ROLE_KEY), ids, destinations);
    }

    /**
     * No usages found in all places
     * -_-
     *
     * @param keyspace
     * @param tabletType
     * @return
     * @throws Exception
     */
    @Override
    public Resolver.AllShardResult getAllShards(String keyspace, Topodata.TabletType tabletType) throws SQLException {
        return this.resolver.getAllShards(this.ctx, keyspace, tabletType);
    }

    @Override
    public VtRowList executeStandalone(String sql, Map<String, Query.BindVariable> bindVars, ResolvedShard resolvedShard, boolean canAutocommit) throws SQLException {
        List<ResolvedShard> rss = new ArrayList<>();
        rss.add(resolvedShard);

        List<Query.BoundQuery> queries = new ArrayList<>();
        final String querySql = comment.getLeading() + sql + comment.getTrailing();
        Query.BoundQuery boundQuery = Query.BoundQuery.newBuilder().setSql(querySql).putAllBindVariables(bindVars).build();
        queries.add(boundQuery);
        ExecuteMultiShardResponse response = this.executor.executeMultiShard(this.ctx, rss, queries, SafeSession.newAutoCommitSession(this.safeSession.getVitessConnection()), canAutocommit, true);
        if (null != response) {
            return response.getVtRowList();
        }
        return null;
    }

    @Override
    public String getCharEncoding() {
        if (this.safeSession == null) {
            return null;
        }
        return this.safeSession.getCharEncoding();
    }
}
