/*
Copyright 2021 JD Project Authors.

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

package testsuite.internal;

public enum TestSuiteShardSpec {
    /**
     * no shards
     */
    NO_SHARDS(0),
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
    SIXTY_FOUR_SHARDS(64);

    public final int shardNumber;

    TestSuiteShardSpec(int shardNumber) {
        this.shardNumber = shardNumber;
    }
}
