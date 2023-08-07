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

package com.jd.jdbc.key;

import io.vitess.proto.Topodata;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CurrentShard implements Shard {
    private static final Map<String, List<Topodata.ShardReference>> SHARDREFERENCE_MAP = new ConcurrentHashMap<>();

    public static void setShardReferences(final String keyspace, final List<Topodata.ShardReference> shardReferences) {
        SHARDREFERENCE_MAP.put(keyspace, shardReferences);
    }

    public static void setShardReferences(final String keyspace, final Topodata.SrvKeyspace srvKeyspace) {
        List<Topodata.SrvKeyspace.KeyspacePartition> partitionsList = srvKeyspace.getPartitionsList();
        List<Topodata.ShardReference> shardReferencesList = null;
        for (Topodata.SrvKeyspace.KeyspacePartition keyspacePartition : partitionsList) {
            if (!Topodata.TabletType.MASTER.equals(keyspacePartition.getServedType())) {
                continue;
            }
            shardReferencesList = keyspacePartition.getShardReferencesList();
        }
        SHARDREFERENCE_MAP.put(keyspace, shardReferencesList);
    }

    @Override
    public List<Topodata.ShardReference> getShardReferences(final String keyspace, final int shardNumber) {
        return SHARDREFERENCE_MAP.get(keyspace);
    }
}