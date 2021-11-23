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

package com.jd.jdbc.vindexes.hash;

import com.jd.jdbc.key.Destination;
import com.jd.jdbc.key.DestinationKeyspaceID;
import com.jd.jdbc.sqltypes.VtValue;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * BinaryHash defines vindex that hashes an int64 to a KeyspaceId
 * by using null-key 3DES hash. It's Unique, Reversible and
 * Functional.
 */
public class BinaryHash extends AbstractHash {

    @Override
    public String toString() {
        return "binaryhash";
    }

    @Override
    public Integer cost() {
        return 1;
    }

    @Override
    public Boolean isUnique() {
        return Boolean.TRUE;
    }

    @Override
    public Boolean needsVcursor() {
        return Boolean.FALSE;
    }

    public Boolean isFunctional() {
        return Boolean.TRUE;
    }

    @Override
    public Destination[] map(VtValue[] ids) {
        List<Destination> out = new ArrayList<>(ids.length);
        VtValueIndex index = new MurmurVtValueIndex();
        Arrays.stream(ids).forEach(id -> {
            BigInteger bi = index.getIndex(id);
            out.add(new DestinationKeyspaceID(vhash(bi)));
        });
        Destination[] outDest = new Destination[ids.length];
        return out.toArray(outDest);
    }

    @Override
    public Boolean[] verify(VtValue[] ids, byte[][] ksids) throws SQLException {
        return super.verify(ids, ksids);
    }

    @Override
    public VtValue[] reverseMap(byte[][] ksids) throws SQLException {
        return super.reverseMap(ksids);
    }
}
