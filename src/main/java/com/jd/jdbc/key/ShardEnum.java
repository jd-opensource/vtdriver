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

public enum ShardEnum {
    /**
     * no shards
     */
    ONE_SHARDS(1),
    /**
     * 2 shards
     */
    TWO_SHARDS(2),
    /**
     * 4 shards
     */
    FOUR_SHARDS(4),
    /**
     * 8 shards
     */
    EIGHT_SHARDS(8),
    /**
     * 16 shards
     */
    SIXTEEN_SHARDS(16),
    /**
     * 32 shards
     */
    THIRTY_TWO_SHARDS(32),
    /**
     * 64 shards
     */
    SIXTY_FOUR_SHARDS(64),
    /**
     * 128 shards
     */
    ONE_HUNDRED_TWENTY_EIGHT_SHARDS(128),
    /**
     * 256 shards
     */
    TWO_HUNDRED_FIFTY_SIX_SHARDS(256);

    private static final ShardEnum[] SHARD_ENUMS = values();

    private final int shardNumber;

    ShardEnum(final int shardNumber) {
        this.shardNumber = shardNumber;
    }

    public static ShardEnum getShardEnum(final int shardNumber) {
        for (ShardEnum shardEnum : SHARD_ENUMS) {
            if (shardEnum.getShardNumber() == shardNumber) {
                return shardEnum;
            }
        }
        return null;
    }

    public int getShardNumber() {
        return shardNumber;
    }
}
