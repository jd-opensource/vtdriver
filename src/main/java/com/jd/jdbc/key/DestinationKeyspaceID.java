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
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * DestinationKeyspaceID is the destination for a single KeyspaceID.
 * It implements the Destination interface.
 */
public class DestinationKeyspaceID implements Destination {

    private final byte[] value;

    public DestinationKeyspaceID(byte[] value) {
        this.value = Arrays.copyOf(value, value.length);
    }

    /**
     * finds the right shard for a keyspace id.
     *
     * @param allShards  List&lt;{@link Topodata.ShardReference}&gt;
     * @param keyspaceID byte[]
     * @return string
     * @throws Exception when an error occurs
     */
    public static String getShardForKeyspaceId(List<Topodata.ShardReference> allShards, byte[] keyspaceID) throws SQLException {
        if (allShards.isEmpty()) {
            throw new SQLException("no shard in keyspace");
        }

        for (Topodata.ShardReference shardReference : allShards) {
            if (Key.keyRangeContains(shardReference.getKeyRange(), keyspaceID)) {
                return shardReference.getName();
            }
        }
        throw new SQLException("KeyspaceId %v didn't match any shards " + allShards);
    }

    public byte[] getValue() {
        return value;
    }

    @Override
    public void resolve(List<Topodata.ShardReference> allShards, DestinationResolve resolve) throws SQLException {
        resolve.resolve(getShardForKeyspaceId(allShards, value));
    }

    @Override
    public Boolean isUnique() {
        return true;
    }

    @Override
    public String toString() {
        return "DestinationKeyspaceID(" + Arrays.toString(value) + ")";
    }
}
