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

package com.jd.jdbc.engine;

import com.jd.jdbc.IExecute;
import com.jd.jdbc.common.Constant;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.queryservice.StreamIterator;
import com.jd.jdbc.queryservice.VtIterator;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqltypes.SqlTypes;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtRowList;
import com.jd.jdbc.srvtopo.ResolvedShard;
import com.jd.jdbc.srvtopo.Resolver;
import com.jd.jdbc.vindexes.VKeyspace;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class SendEngine implements PrimitiveEngine {
    /**
     * ShardName as key for setting shard name in bind variables map
     */
    public static final String SHARD_NAME = "__vt_shard";

    /**
     * Keyspace specifies the keyspace to send the query to.
     */
    private final VKeyspace keyspace;

    /**
     * TargetDestination specifies an explicit target destination to send the query to.
     * This bypases the core of the v3 engine.
     */
    private final Destination targetDestination;

    /**
     * Query specifies the query to be executed.
     */
    private final String query;

    private final SQLStatement stmt;

    /**
     * IsDML specifies how to deal with autocommit behaviour
     */
    private final boolean isDML;

    /**
     * SingleShardOnly specifies that the query must be send to only single shard
     */
    private final boolean singleShardOnly;

    /**
     * ShardNameNeeded specified that the shard name is added to the bind variables
     */
    private final boolean shardNameNeeded;

    /**
     * MultishardAutocommit specifies that a multishard transaction query can autocommit
     */
    private final boolean multishardAutocommit;

    @Override
    public String getKeyspaceName() {
        return null;
    }

    @Override
    public String getTableName() {
        return null;
    }

    /**
     * @param ctx
     * @param vcursor
     * @param bindVariableMap
     * @param wantFields
     * @return
     * @throws SQLException
     */
    @Override
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, Query.BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        if (this.isDML && ctx.getContextValue(Constant.DRIVER_PROPERTY_ROLE_KEY) != Topodata.TabletType.MASTER) {
            throw new SQLException("dml is not allowed for read only connection");
        }
        IExecute.ResolvedShardQuery resolvedShardQuery = this.getResolvedShardQuery(vcursor, bindVariableMap);

        boolean canAutocommit = false;
        if (this.isDML) {
            canAutocommit = (resolvedShardQuery.getRss().size() == 1 || this.multishardAutocommit) && vcursor.autocommitApproval();
        }
        // for non-dml queries, there's no need to do a rollback
        boolean rollbackOnError = this.isDML;
        return this.isDML ? vcursor.executeMultiShard(resolvedShardQuery.getRss(), resolvedShardQuery.getQueries(), rollbackOnError, canAutocommit).setUpdate()
            : vcursor.executeMultiShard(resolvedShardQuery.getRss(), resolvedShardQuery.getQueries(), rollbackOnError, canAutocommit);
    }

    /**
     * @param ctx
     * @param vcursor
     * @param bindValue
     * @param wantFields
     * @return
     * @throws SQLException
     */
    @Override
    public IExecute.VtStream streamExecute(IContext ctx, Vcursor vcursor, Map<String, Query.BindVariable> bindValue, boolean wantFields) throws SQLException {
        if (this.isDML) {
            throw new SQLFeatureNotSupportedException("dml sql doesn't support stream query");
        }

        IExecute.ResolvedShardQuery resolvedShardQuery = this.getResolvedShardQuery(vcursor, bindValue);
        List<StreamIterator> iteratorList = vcursor.streamExecuteMultiShard(resolvedShardQuery.getRss(), resolvedShardQuery.getQueries());
        return new SendStream(iteratorList, this, vcursor, bindValue);
    }

    private IExecute.ResolvedShardQuery getResolvedShardQuery(Vcursor vcursor, Map<String, Query.BindVariable> bindValue) throws SQLException {
        List<Destination> destinations = new ArrayList<>();
        destinations.add(targetDestination);
        Resolver.ResolveDestinationResult resolveDestinationResult = vcursor.resolveDestinations(this.keyspace.getName(), null, destinations);
        String charEncoding = vcursor.getCharEncoding();
        if (resolveDestinationResult == null) {
            throw new SQLException("Keyspace does not have exactly one shard: " + this.query + ", got: " + this.targetDestination);
        }
        List<ResolvedShard> resolvedShardList = resolveDestinationResult.getResolvedShards();
        if (!this.keyspace.getSharded() && resolvedShardList.size() != 1) {
            throw new SQLException("Keyspace does not have exactly one shard: " + resolveDestinationResult);
        }
        if (this.singleShardOnly && resolvedShardList.size() != 1) {
            throw new SQLException("Unexpected error, DestinationKeyspaceID mapping to multiple shards: " + this.query + ", got: " + this.targetDestination);
        }

        List<Map<String, Query.BindVariable>> multiBindVars = new ArrayList<>(resolvedShardList.size());
        for (ResolvedShard resolvedShard : resolvedShardList) {
            Map<String, Query.BindVariable> bv = bindValue == null ? new HashMap<>() : new HashMap<>(bindValue);
            if (this.shardNameNeeded) {
                bv.put(SHARD_NAME, SqlTypes.stringBindVariable(resolvedShard.getTarget().getShard()));
            }
            multiBindVars.add(bv);
        }
        List<Query.BoundQuery> queries = Engine.getQueries(stmt, multiBindVars, charEncoding);
        return new IExecute.ResolvedShardQuery(resolvedShardList, queries);
    }

    @Override
    public Boolean needsTransaction() {
        return this.isDML;
    }

    class SendStream implements IExecute.VtStream {

        private final List<StreamIterator> iterators;

        private final SendEngine sendEngine;

        private final Map<String, Query.BindVariable> bindVariableMap;

        Vcursor vcursor;

        private Query.Field[] fields = null;

        public SendStream(List<StreamIterator> iterators, SendEngine sendEngine, Vcursor vcursor, Map<String, Query.BindVariable> bindVariableMap) {
            this.iterators = iterators;
            this.sendEngine = sendEngine;
            this.vcursor = vcursor;
            this.bindVariableMap = bindVariableMap;
        }

        @Override
        public VtRowList fetch(boolean wantFields) throws SQLException {
            VtResultSet vtResultSet = new VtResultSet();
            if (iterators == null) {
                return vtResultSet;
            }
            for (VtIterator<VtResultSet> it : iterators) {
                if (it.hasNext()) {
                    VtResultSet next = it.next();
                    vtResultSet.appendResult(next);
                    if (fields == null) {
                        fields = vtResultSet.getFields();
                    }
                }
            }
            vtResultSet.setRowsAffected(vtResultSet.getRows() == null ? 0 : vtResultSet.getRows().size());

            if (wantFields && vtResultSet.getFields() == null) {
                vtResultSet.appendResult(this.sendEngine.getFields(vcursor, bindVariableMap));
                fields = vtResultSet.getFields();
            }
            return vtResultSet;
        }

        @Override
        public void close() throws SQLException {
            if (iterators == null || iterators.isEmpty()) {
                return;
            }
            for (StreamIterator streamIterator : iterators) {
                streamIterator.close();
            }
            iterators.clear();
        }
    }
}
