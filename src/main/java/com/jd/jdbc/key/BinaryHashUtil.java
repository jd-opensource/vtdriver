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

import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.vindexes.hash.BinaryHash;
import io.vitess.proto.Topodata;
import java.sql.SQLException;
import java.util.List;

public final class BinaryHashUtil {
    private static final String SINGLE_SHARD_REFERENCE_NAME = "0";

    private BinaryHashUtil() {
    }

    public static String getShardByVindex(final ShardEnum shards, final Object indexValue) throws SQLException {
        if (shards == ShardEnum.ONE_SHARDS) {
            return SINGLE_SHARD_REFERENCE_NAME;
        }
        Shard shard = new StandardShard();
        List<Topodata.ShardReference> shardReferenceList = shard.getShardReferences(null, shards.getShardNumber());
        return getShardName(indexValue, shardReferenceList);
    }

    public static String getShardByVindex(final String keyspace, final Object indexValue) throws SQLException {
        if (StringUtils.isEmpty(keyspace)) {
            throw new IllegalArgumentException("keyspace should not empty");
        }
        Shard shard = new CurrentShard();
        List<Topodata.ShardReference> shardReferenceList = shard.getShardReferences(keyspace, -1);
        return getShardName(indexValue, shardReferenceList);
    }

    private static String getShardName(Object indexValue, List<Topodata.ShardReference> shardReferenceList) throws SQLException {
        VtValue vtValue = VtValue.toVtValue(indexValue);
        Destination[] destinationsArray = new BinaryHash().map(new VtValue[] {vtValue});
        if (destinationsArray.length != 1) {
            throw new RuntimeException("BinaryHash map error!");
        }
        Destination destination = destinationsArray[0];
        if (!(destination instanceof DestinationKeyspaceID)) {
            throw new RuntimeException("should be DestinationKeyspaceID!");
        }
        DestinationKeyspaceID keyspaceId = (DestinationKeyspaceID) destination;
        return DestinationKeyspaceID.getShardForKeyspaceId(shardReferenceList, keyspaceId.getValue());
    }
}