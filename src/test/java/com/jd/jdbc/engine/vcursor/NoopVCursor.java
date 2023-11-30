/*
Copyright 2023 JD Project Authors. Licensed under Apache-2.0.

Copyright 2022 The Vitess Authors.

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

package com.jd.jdbc.engine.vcursor;

import com.jd.jdbc.IExecute;
import com.jd.jdbc.engine.Vcursor;
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

/**
 * noopVCursor is used to build other vcursors.
 */
public class NoopVCursor implements Vcursor {

    @Override
    public Integer maxMemoryRows() {
        return FakeVcursorUtil.testMaxMemoryRows;
    }

    @Override
    public Boolean exceedsMaxMemoryRows(Integer numRows) {
        return !FakeVcursorUtil.testIgnoreMaxMemoryRows && numRows > FakeVcursorUtil.testMaxMemoryRows;
    }

    @Override
    public Boolean autocommitApproval() {
        return null;
    }

    @Override
    public IExecute.ExecuteMultiShardResponse executeMultiShard(List<ResolvedShard> rss, List<BoundQuery> queries, Boolean rollbackOnError, Boolean canAutocommit) throws SQLException {
        return null;
    }

    @Override
    public IExecute.ExecuteBatchMultiShardResponse executeBatchMultiShard(List<ResolvedShard> rss, List<List<BoundQuery>> queries, Boolean rollbackOnError, Boolean canAutocommit) throws SQLException {
        return null;
    }

    @Override
    public List<StreamIterator> streamExecuteMultiShard(List<ResolvedShard> rss, List<BoundQuery> queries) throws SQLException {
        return null;
    }

    @Override
    public Resolver.ResolveDestinationResult resolveDestinations(String keyspace, List<Query.Value> ids, List<Destination> destinations) throws SQLException {
        return null;
    }

    @Override
    public Resolver.AllShardResult getAllShards(String keyspace, Topodata.TabletType tabletType) throws SQLException {
        return null;
    }

    @Override
    public VtRowList executeStandalone(String sql, Map<String, BindVariable> bindVars, ResolvedShard resolvedShard, boolean canAutocommit) throws SQLException {
        return null;
    }

    @Override
    public Boolean getRollbackOnPartialExec() {
        return null;
    }

    @Override
    public String getCharEncoding() {
        return null;
    }

    @Override
    public int getMaxParallelNum() {
        return 0;
    }
}
