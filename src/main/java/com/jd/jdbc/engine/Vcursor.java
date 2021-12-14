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
import com.jd.jdbc.IExecute.ExecuteMultiShardResponse;
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.queryservice.StreamIterator;
import com.jd.jdbc.sqltypes.VtRowList;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.BoundQuery;
import com.jd.jdbc.srvtopo.ResolvedShard;
import com.jd.jdbc.srvtopo.Resolver;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface Vcursor {
    // MaxMemoryRows returns the maxMemoryRows flag value.
    Integer maxMemoryRows();

    // ExceedsMaxMemoryRows returns a boolean indicating whether
    // the maxMemoryRows value has been exceeded. Returns false
    // if the max memory rows override directive is set to true
    Boolean exceedsMaxMemoryRows(Integer numRows);

    Boolean autocommitApproval();

    /**
     * Shard-level functions.
     *
     * @param rss
     * @param queries
     * @param rollbackOnError
     * @param canAutocommit
     * @return
     */
    ExecuteMultiShardResponse executeMultiShard(List<ResolvedShard> rss, List<BoundQuery> queries, Boolean rollbackOnError, Boolean canAutocommit) throws SQLException;

    IExecute.ExecuteBatchMultiShardResponse executeBatchMultiShard(List<ResolvedShard> rss, List<List<BoundQuery>> queries, Boolean rollbackOnError, Boolean canAutocommit) throws SQLException;

    List<StreamIterator> streamExecuteMultiShard(List<ResolvedShard> rss, List<BoundQuery> queries) throws SQLException;

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
    Resolver.ResolveDestinationResult resolveDestinations(String keyspace, List<Query.Value> ids, List<Destination> destinations) throws SQLException;

    /**
     * No usages found in all places
     * -_-
     *
     * @param keyspace
     * @param tabletType
     * @return
     * @throws Exception
     */
    Resolver.AllShardResult getAllShards(String keyspace, Topodata.TabletType tabletType) throws SQLException;

    VtRowList executeStandalone(String sql, Map<String, BindVariable> bindVars, ResolvedShard resolvedShard, boolean canAutocommit) throws SQLException;

    Boolean getRollbackOnPartialExec();

    String getCharEncoding();
}
