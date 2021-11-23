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
import java.util.List;

/**
 * DestinationAllShards is the destination for all the shards in the
 * keyspace. This usually maps to the first one in the list.
 * It implements the Destination interface.
 */
public class DestinationAllShard implements Destination {

    @Override
    public void resolve(List<Topodata.ShardReference> allShards, DestinationResolve resolve) throws SQLException {
        for (Topodata.ShardReference shard : allShards) {
            resolve.resolve(shard.getName());
        }
    }

    @Override
    public Boolean isUnique() {
        return false;
    }

    @Override
    public String toString() {
        return "DestinationAllShard()";
    }
}
