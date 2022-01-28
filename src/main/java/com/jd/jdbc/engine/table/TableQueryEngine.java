/*
Copyright 2021 JD Project Authors. Licensed under Apache-2.0.

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

package com.jd.jdbc.engine.table;

import com.google.common.collect.Lists;
import com.jd.jdbc.IExecute;
import com.jd.jdbc.VcursorImpl;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.engine.AbstractMultiQueryEngine;
import com.jd.jdbc.engine.DMLEngine;
import com.jd.jdbc.engine.InsertEngine;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.BoundQuery;
import com.jd.jdbc.srvtopo.ResolvedShard;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TableQueryEngine extends AbstractMultiQueryEngine {

    private final Map<String, String> shardTableLTMap;

    public TableQueryEngine(List<PrimitiveEngine> primitiveEngineList, List<IExecute.ResolvedShardQuery> shardQueryList,
                            List<Map<String, BindVariable>> bindVariableMapList, Map<String, String> shardTableLTMap) {
        super(primitiveEngineList, shardQueryList, bindVariableMapList);
        this.shardTableLTMap = shardTableLTMap;
    }

    @Override
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        // 1.reorganizeQuries
        // resolvedShardQuery:
        // originSql1---> shards---sqls1
        // originSql2---> shards---sqls2
        // originSql3---> shards---sqls3
        // originSql4---> shards---sqls4
        //            |
        //            |
        //            V
        // rss      queries
        // shard1 sql1;sql2;sql3;
        // shard2       sql2;sql3;sql4
        // shard3 sql1;       sql3;sql4
        // shard4        sql2;sql3;
        Map<ResolvedShard, List<BoundQuery>> resolvedShardListMap = getResolvedShardListMap();

        List<ResolvedShard> rss;
        List<List<BoundQuery>> queries;

        // for transaction needed query, parallel is not suitable
        if (this.needsTransaction() || ((VcursorImpl) vcursor).getSafeSession().inTransaction()) {
            rss = new ArrayList<>(resolvedShardListMap.size());
            queries = new ArrayList<>(resolvedShardListMap.size());
            for (Map.Entry<ResolvedShard, List<BoundQuery>> rssQueriesEntry : resolvedShardListMap.entrySet()) {
                rss.add(rssQueriesEntry.getKey());
                queries.add(rssQueriesEntry.getValue());
            }
        } else {
            int maxParallelNum = vcursor.getMaxParallelNum();
            int maxRssSize = resolvedShardListMap.size() * maxParallelNum;
            rss = new ArrayList<>(maxRssSize);
            queries = new ArrayList<>(maxRssSize);
            for (Map.Entry<ResolvedShard, List<BoundQuery>> rssQueriesEntry : resolvedShardListMap.entrySet()) {
                int partitionSize = (rssQueriesEntry.getValue().size() / maxParallelNum) + ((rssQueriesEntry.getValue().size() % maxParallelNum) > 0 ? 1 : 0);
                List<List<BoundQuery>> rssQueries = Lists.partition(rssQueriesEntry.getValue(), partitionSize);
                for (int i = 0; i < rssQueries.size(); i++) {
                    rss.add(rssQueriesEntry.getKey());
                }
                queries.addAll(rssQueries);
            }
        }

        // 2.parallelToShards
        IExecute.ExecuteBatchMultiShardResponse response = parallelShardsExecute(vcursor, rss, queries);

        // 3.getQuriesResult
        List<List<VtResultSet>> vtResultSetList = response.getVtResultSetList();
        List<ResolvedShard> resulRss = response.getRss();

        // 4.merge results
        VtResultSet resultSet = new VtResultSet();
        for (int i = 0; i < resulRss.size(); i++) {
            IExecute.ExecuteBatchResultSet batchResultSet = new IExecute.ExecuteBatchResultSet(vtResultSetList.get(i));
            for (VtResultSet innerResult : batchResultSet.getVtResultSetList()) {
                resultSet.appendResultIgnoreTable(innerResult);
            }
        }

        // 5.modify field info
        if (isDmlEngine()) {
            return new IExecute.ExecuteMultiShardResponse(resultSet).setUpdate();
        }

        for (int i = 0; i < resultSet.getFields().length; i++) {
            Query.Field[] fields = resultSet.getFields();
            if (fields[i] == null) {
                continue;
            }
            String actualTableName = fields[i].getTable();
            if (shardTableLTMap.containsKey(actualTableName)) {
                String logicTable = shardTableLTMap.get(actualTableName);
                fields[i] = fields[i].toBuilder().setTable(logicTable).build();
            }
        }

        return new IExecute.ExecuteMultiShardResponse(resultSet);
    }

    private boolean isDmlEngine() {
        for (PrimitiveEngine primitiveEngine : primitiveEngineList) {
            if (primitiveEngine instanceof DMLEngine
                || primitiveEngine instanceof TableDMLEngine
                || primitiveEngine instanceof InsertEngine
                || primitiveEngine instanceof TableInsertEngine) {
                return true;
            }
        }
        return false;
    }
}
