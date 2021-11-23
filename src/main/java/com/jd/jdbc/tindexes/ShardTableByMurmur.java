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

package com.jd.jdbc.tindexes;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;

public class ShardTableByMurmur implements TableIndex {
    @Override
    public int getIndex(final ByteString value, final int tablesNum) {
        HashFunction hashFunction = Hashing.murmur3_128();
        HashCode hashCode = hashFunction.hashString(value.toStringUtf8(), Charsets.UTF_8);
        long num = Math.abs(hashCode.asLong()) % tablesNum;
        return (int) num;
    }
}
