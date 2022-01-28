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
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.BoundQuery;
import com.jd.jdbc.srvtopo.ResolvedShard;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiQueryEngine extends AbstractMultiQueryEngine implements PrimitiveEngine {

    public MultiQueryEngine(List<PrimitiveEngine> primitiveEngineList, List<IExecute.ResolvedShardQuery> shardQueryList,
                            List<Map<String, BindVariable>> bindVariableMapList) {
        super(primitiveEngineList, shardQueryList, bindVariableMapList);
    }

    @Override
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        throw new SQLFeatureNotSupportedException("unsupported execute");
    }

    @Override
    public List<IExecute.ExecuteMultiShardResponse> batchExecute(IContext ctx, Vcursor vcursor, boolean wantFields) throws SQLException {
        // 1.reorganizeQuries
        // resolvedShardQuery:
        // originSql1---> shards---sqls1
        // originSql2---> shards---sqls2
        // originSql3---> shards---sqls3
        // originSql4---> shards---sqls4
        Map<ResolvedShard, List<BoundQuery>> resolvedShardListMap = getResolvedShardListMap();

        List<ResolvedShard> rss = new ArrayList<>(resolvedShardListMap.size());
        List<List<BoundQuery>> queries = new ArrayList<>(resolvedShardListMap.size());
        resolvedShardListMap.forEach((k, v) -> {
            rss.add(k);
            queries.add(v);
        });

        // 2.parallelToShards
        // rss      queries
        // shard1 sql1;sql2;sql3;
        // shard2       sql2;sql3;sql4
        // shard3 sql1;       sql3;sql4
        // shard4        sql2;sql3;
        IExecute.ExecuteBatchMultiShardResponse response = parallelShardsExecute(vcursor, rss, queries);

        // 3.getQuriesResult
        List<List<VtResultSet>> vtResultSetList = response.getVtResultSetList();
        List<ResolvedShard> resulRss = response.getRss();

        Map<ResolvedShard, IExecute.ExecuteBatchResultSet> resolvedShardResultSetMap = new HashMap<>(resulRss.size());
        for (int i = 0; i < resulRss.size(); i++) {
            IExecute.ExecuteBatchResultSet batchResultSet = new IExecute.ExecuteBatchResultSet(vtResultSetList.get(i));
            resolvedShardResultSetMap.put(resulRss.get(i), batchResultSet);
        }

        // 最重要的是如何组织各个分片返回结果、尤其是涉及到了跨分片的复杂查询，需要能够使用到目前的执行计划框架、保证返回正确的结果集
        // 4.organize results
        List<IExecute.ExecuteMultiShardResponse> batchResult = new ArrayList<>();
        for (int i = 0; i < shardQueryList.size(); i++) {
            IExecute.ResolvedShardQuery resolvedShardQuery = shardQueryList.get(i);
            if (resolvedShardQuery == null) {
                continue;
            }
            VtResultSet resultSet = new VtResultSet();
            for (ResolvedShard rs : resolvedShardQuery.getRss()) {
                IExecute.ExecuteBatchResultSet batchResultSet = resolvedShardResultSetMap.get(rs);
                VtResultSet innerResultSet = batchResultSet.getAndMoveIndex();
                resultSet.appendResult(innerResultSet);
            }
            PrimitiveEngine primitive = primitiveEngineList.get(i);
            // organize results
            batchResult.add(primitive.mergeResult(resultSet, bindVariableMapList.get(i), wantFields));
        }
        return batchResult;
    }
}
