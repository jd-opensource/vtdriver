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

package com.jd.jdbc.shards.hash;

import com.google.protobuf.ByteString;
import com.jd.jdbc.key.Key;
import io.vitess.proto.Topodata;
import org.junit.Assert;
import org.junit.Test;

public class KeyTest {

    @Test
    public void testKey() {
        Topodata.KeyRange keyRange = Topodata.KeyRange.newBuilder().setStart(ByteString.copyFrom(new byte[] {00})).setEnd(ByteString.copyFrom(new byte[] {40})).build();
        Assert.assertTrue(Key.keyRangeContains(keyRange, new byte[] {0x00}));
        Assert.assertTrue(Key.keyRangeContains(keyRange, new byte[] {0x01}));
        Assert.assertTrue(Key.keyRangeContains(keyRange, new byte[] {0x02}));
        Assert.assertTrue(Key.keyRangeContains(keyRange, new byte[] {0x03}));
        Assert.assertTrue(Key.keyRangeContains(keyRange, new byte[] {0x04}));
        Assert.assertTrue(Key.keyRangeContains(keyRange, new byte[] {0x05}));

        Assert.assertFalse(Key.keyRangeContains(keyRange, new byte[] {0x40}));
        Assert.assertFalse(Key.keyRangeContains(keyRange, new byte[] {0x41}));
    }
}
