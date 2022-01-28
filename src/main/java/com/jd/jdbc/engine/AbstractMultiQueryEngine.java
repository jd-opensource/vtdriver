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

package com.jd.jdbc.engine;

import com.jd.jdbc.IExecute;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.BoundQuery;
import com.jd.jdbc.srvtopo.ResolvedShard;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractMultiQueryEngine implements PrimitiveEngine {

    protected final List<PrimitiveEngine> primitiveEngineList;

    protected final List<IExecute.ResolvedShardQuery> shardQueryList;

    protected final List<Map<String, BindVariable>> bindVariableMapList;

    protected AbstractMultiQueryEngine(List<PrimitiveEngine> primitiveEngineList, List<IExecute.ResolvedShardQuery> shardQueryList,
                                       List<Map<String, BindVariable>> bindVariableMapList) {
        this.primitiveEngineList = primitiveEngineList;
        this.shardQueryList = shardQueryList;
        this.bindVariableMapList = bindVariableMapList;
    }

    @Override
    public String getKeyspaceName() {
        return null;
    }

    @Override
    public String getTableName() {
        return null;
    }

    protected IExecute.ExecuteBatchMultiShardResponse parallelShardsExecute(Vcursor vcursor, List<ResolvedShard> rss, List<List<BoundQuery>> queries) throws SQLException {
        int sqlSize = 0;
        for (List<BoundQuery> boundQueryList : queries) {
            sqlSize += boundQueryList.size();
        }
        boolean autocommit = rss.size() == 1 && sqlSize == 1 && vcursor.autocommitApproval();
        return vcursor.executeBatchMultiShard(rss, queries, true, autocommit);
    }

    protected Map<ResolvedShard, List<BoundQuery>> getResolvedShardListMap() {
        Map<ResolvedShard, List<BoundQuery>> resolvedShardListMap = new HashMap<>(shardQueryList.size());

        for (IExecute.ResolvedShardQuery resolvedShardQuery : shardQueryList) {
            if (resolvedShardQuery == null) {
                continue;
            }
            for (int i = 0; i < resolvedShardQuery.getRss().size(); i++) {
                ResolvedShard resolvedShard = resolvedShardQuery.getRss().get(i);
                BoundQuery boundQuery = resolvedShardQuery.getQueries().get(i);
                resolvedShardListMap.computeIfAbsent(resolvedShard, k -> new ArrayList<>()).add(boundQuery);
            }
        }
        return resolvedShardListMap;
    }

    @Override
    public Boolean needsTransaction() {
        for (PrimitiveEngine primitiveEngine : primitiveEngineList) {
            if (primitiveEngine.needsTransaction()) {
                return true;
            }
        }
        return false;
    }
}
