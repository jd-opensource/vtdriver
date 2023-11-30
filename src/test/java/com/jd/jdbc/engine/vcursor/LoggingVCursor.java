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

import com.google.common.collect.Lists;
import com.jd.jdbc.IExecute;
import com.jd.jdbc.common.util.CollectionUtils;
import com.jd.jdbc.engine.Vcursor;
import static com.jd.jdbc.engine.vcursor.FakeVcursorUtil.printBindVars;
import static com.jd.jdbc.engine.vcursor.FakeVcursorUtil.printValues;
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.key.DestinationAllShard;
import com.jd.jdbc.key.DestinationAnyShard;
import com.jd.jdbc.key.DestinationKeyspaceID;
import com.jd.jdbc.key.DestinationNone;
import com.jd.jdbc.key.DestinationShard;
import com.jd.jdbc.queryservice.StreamIterator;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtRowList;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.BoundQuery;
import com.jd.jdbc.srvtopo.ResolvedShard;
import com.jd.jdbc.srvtopo.Resolver;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Data;
import org.junit.Assert;
import vschema.Vschema;

/**
 * loggingVCursor logs requests and allows you to verify
 * that the correct requests were made.
 */
@Data
public class LoggingVCursor implements Vcursor {

    private NoopVCursor noopVCursor;

    private List<String> shards;

    private List<String> shardForKsid;

    private int curShardForKsid;

    private SQLException shardErr;

    private List<VtResultSet> results;

    private int curResult;

    private SQLException resultErr;

    private List<Query.QueryWarning> warnings = new ArrayList<>();

    // Optional errors that can be returned from nextResult() alongside the results for
    // multi-shard queries
    private List<SQLException> multiShardErrs;

    private List<String> log = new ArrayList<>();

    private final ReentrantLock lock = new ReentrantLock();

    private Topodata.TabletType resolvedTargetTabletType = Topodata.TabletType.UNKNOWN;

    private Vschema.Table tableRoutes;

    private String dbDDLPlugin;

    private boolean ksAvailable;

    private boolean inReservedConn;

    private Map<String, String> systemVariables;

    private boolean disableSetVar;

    // map different shards to keyspaces in the test.
    private Map<String, List<String>> ksShardMap;

    public LoggingVCursor(List<String> shards, List<VtResultSet> results) {
        this.shards = shards;
        this.results = results;
    }

    public LoggingVCursor(SQLException shardErr) {
        this.shardErr = shardErr;
    }

    @Override
    public Integer maxMemoryRows() {
        return null;
    }

    @Override
    public Boolean exceedsMaxMemoryRows(Integer numRows) {
        return null;
    }

    @Override
    public Boolean autocommitApproval() {
        return null;
    }

    @Override
    public IExecute.ExecuteMultiShardResponse executeMultiShard(List<ResolvedShard> rss, List<BoundQuery> queries, Boolean rollbackOnError, Boolean canAutocommit) throws SQLException {
        if (CollectionUtils.isNotEmpty(multiShardErrs)) {
            throw multiShardErrs.get(0);
        }
        log.add(String.format("ExecuteMultiShard %s %s %s", printResolvedShardQueries(rss, queries), rollbackOnError, canAutocommit));

        VtResultSet res = nextResult();
        return new IExecute.ExecuteMultiShardResponse(res);
    }

    private String printResolvedShardQueries(List<ResolvedShard> rss, List<BoundQuery> queries) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rss.size(); i++) {
            ResolvedShard rs = rss.get(i);
            String format = String.format("%s.%s: %s{%s} ", rs.getTarget().getKeyspace(), rs.getTarget().getShard(), queries.get(i).getSql(), printBindVars(queries.get(i).getBindVariablesMap()));
            sb.append(format);
        }
        sb.deleteCharAt(sb.length() - 1);

        return sb.toString();
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
        log.add(String.format("ResolveDestinations %s %s %s", keyspace, printValues(ids), destinationsString(destinations)));
        if (shardErr != null) {
            throw shardErr;
        }
        List<ResolvedShard> rss = new ArrayList<>();
        List<List<Query.Value>> values = new LinkedList<>();
        Map<String, Integer> visited = new HashMap<>();
        for (int i = 0; i < destinations.size(); i++) {
            List<String> shards = new ArrayList<>();
            Destination destination = destinations.get(i);
            if (destination instanceof DestinationAllShard) {
                if (ksShardMap != null) {
                    if (ksShardMap.containsKey(keyspace)) {
                        shards = ksShardMap.get(keyspace);
                    }
                } else {
                    shards = this.shards;
                }
            } else if (destination instanceof DestinationKeyspaceID) {
                if (shardForKsid == null || curShardForKsid > shardForKsid.size()) {
                    shards = Lists.newArrayList("-20");
                } else {
                    shards = Lists.newArrayList(shardForKsid.get(curShardForKsid));
                    curShardForKsid++;
                }
            } else if (destination instanceof DestinationAnyShard) {
                // Take the first shard.
                shards = Lists.newArrayList(this.shards.get(0));
            } else if (destination instanceof DestinationNone) {
                // Nothing to do here.
            } else if (destination instanceof DestinationShard) {
                shards = Lists.newArrayList(destination.toString());
            } else {
                throw new SQLException("unsupported destination: " + destination.toString());
            }
            for (String shard : shards) {
                Integer vi = visited.get(shard);
                if (vi == null) {
                    vi = rss.size();
                    visited.put(shard, vi);
                    Query.Target.Builder targetBuilder = Query.Target.newBuilder().setKeyspace(keyspace)
                        .setShard(shard)
                        .setTabletType(resolvedTargetTabletType);
                    ResolvedShard resolvedShard = new ResolvedShard();
                    resolvedShard.setTarget(targetBuilder.build());
                    rss.add(resolvedShard);
                    if (ids != null) {
                        values.add(new ArrayList<>());
                    }
                }
                if (ids != null) {
                    values.get(vi).add(ids.get(i));
                }
            }
        }
        return new Resolver.ResolveDestinationResult(rss, values);
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

    private VtResultSet nextResult() throws SQLException {
        if (results == null || curResult >= results.size()) {
            if (resultErr != null) {
                throw resultErr;
            } else {
                return new VtResultSet();
            }
        }
        VtResultSet r = results.get(curResult);
        curResult++;
        if (r == null) {
            throw resultErr;
        }
        return r;
    }

    // DestinationsString returns a printed version of the destination array.
    public String destinationsString(List<Destination> destinations) {
        StringBuilder sb = new StringBuilder("Destinations:");
        for (Destination d : destinations) {
            sb.append(d.toString());
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public void expectLog(List<String> want) {
        if (want.size() != log.size()) {
            Assert.fail("wants size " + want.size() + " actual size" + log.size());
        }
        for (int i = 0; i < want.size(); i++) {
            Assert.assertEquals(want.get(i), log.get(i));
        }
    }

    public void rewind() {
        this.curShardForKsid = 0;
        this.curResult = 0;
        this.log.clear();
        this.warnings.clear();
    }


}
