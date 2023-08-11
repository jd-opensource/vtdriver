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

import io.vitess.proto.Topodata.KeyRange;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Key {

    public static final Map<KeyRange, byte[]> keyRangeStartCache = new ConcurrentHashMap<>(16, 1);

    public static final Map<KeyRange, byte[]> keyRangeEndCache = new ConcurrentHashMap<>(16, 1);

    // KeyRangeContains returns true if the provided id is in the keyrange.
    public static boolean keyRangeContains(KeyRange kr, byte[] id) {
        if (kr == null) {
            return true;
        }

        byte[] startCache = keyRangeStartCache.get(kr);
        if (startCache == null || startCache.length == 0) {
            startCache = kr.getStart().toByteArray();
            keyRangeStartCache.put(kr, startCache);
        }

        byte[] endCache = keyRangeEndCache.get(kr);
        if (endCache == null || endCache.length == 0) {
            endCache = kr.getEnd().toByteArray();
            keyRangeEndCache.put(kr, endCache);
        }

        return Bytes.compare(startCache, id) <= 0 && ((kr.getEnd()).size() == 0 || Bytes.compare(id, endCache) < 0);
    }
}
