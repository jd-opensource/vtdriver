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

import com.google.protobuf.ByteString;
import io.vitess.proto.Topodata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StandardShard implements Shard {

    private static final Map<Integer, List<Topodata.ShardReference>> SHARDS_MAP = new ConcurrentHashMap<>(16);

    private static final String SINGLE_SHARD_REFERENCE_NAME = "0";

    private static final int SHARD_TOTAL = 256;

    private static List<Topodata.ShardReference> buildReferences(final int shards) {
        if (shards == 1) {
            Topodata.ShardReference shardReference = Topodata.ShardReference.newBuilder().setName(SINGLE_SHARD_REFERENCE_NAME).build();
            return Collections.singletonList(shardReference);
        }
        int avg = SHARD_TOTAL / shards;
        byte[][] bytes = new byte[shards - 1][1];
        for (int i = 1; i < shards; i++) {
            byte[] innerBytes = new byte[1];
            innerBytes[0] = (byte) (avg * i);
            bytes[i - 1][0] = innerBytes[0];
        }
        List<Topodata.ShardReference> shardReferenceList = new ArrayList<>(shards);
        for (int index = 0; index < shards; index++) {
            Topodata.KeyRange.Builder builder = Topodata.KeyRange.newBuilder();
            String prefix = "";
            if (index != 0) {
                builder.setStart(ByteString.copyFrom(bytes[index - 1]));
                prefix = Integer.toHexString((bytes[index - 1][0] & 0x000000ff) | 0xffffff00).substring(6);
            }
            String postfix = "";
            if (index != shards - 1) {
                builder.setEnd(ByteString.copyFrom(bytes[index]));
                postfix = Integer.toHexString((bytes[index][0] & 0x000000ff) | 0xffffff00).substring(6);
            }
            Topodata.KeyRange keyRange = builder.build();
            String name = prefix + "-" + postfix;
            Topodata.ShardReference shardReference = Topodata.ShardReference.newBuilder().setName(name).setKeyRange(keyRange).build();
            shardReferenceList.add(shardReference);
        }
        return shardReferenceList;
    }

    @Override
    public List<Topodata.ShardReference> getShardReferences(String keyspace, int shardNumber) {
        ShardEnum shardEnum = ShardEnum.getShardEnum(shardNumber);
        if (shardEnum == null) {
            throw new IllegalArgumentException("shards error! shardNumber=" + shardNumber);
        }
        List<Topodata.ShardReference> shardReferences = SHARDS_MAP.get(shardNumber);
        if (shardReferences != null && !shardReferences.isEmpty()) {
            return shardReferences;
        }
        synchronized (BinaryHashUtil.class) {
            if (SHARDS_MAP.get(shardNumber) != null && !SHARDS_MAP.get(shardNumber).isEmpty()) {
                return SHARDS_MAP.get(shardNumber);
            }
            List<Topodata.ShardReference> references = buildReferences(shardNumber);
            SHARDS_MAP.put(shardNumber, references);
            return references;
        }
    }
}