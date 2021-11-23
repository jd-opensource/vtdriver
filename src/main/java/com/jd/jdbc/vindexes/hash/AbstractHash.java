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

import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.vindexes.Reversible;
import com.jd.jdbc.vindexes.SingleColumn;
import com.jd.jdbc.vindexes.cryto.TripleDES;
import io.vitess.proto.Query;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractHash implements SingleColumn, Reversible {

    private static final TripleDES BLOCK3_DES = TripleDES.getInstance();

    protected static byte[] vhash(BigInteger shardKey) {
        return BLOCK3_DES.encrypt(shardKey);
    }

    protected static long vunhash(byte[] k) {
        return k.length != 8 ? 0 : BLOCK3_DES.decrypt(k);
    }

    /**
     * String returns the name of the Vindex instance.
     * It's used for testing and diagnostics. Use pointer
     * comparison to see if two objects refer to the same
     * Vindex.
     *
     * @return String
     */
    @Override
    public abstract String toString();

    /**
     * Cost is used by planbuilder to prioritize vindexes.
     * The cost can be 0 if the id is basically a keyspace id.
     * The cost can be 1 if the id can be hashed to a keyspace id.
     * The cost can be 2 or above if the id needs to be looked up
     * from an external data source. These guidelines are subject
     * to change in the future.
     *
     * @return Integer
     */
    @Override
    public abstract Integer cost();

    /**
     * IsUnique returns true if the Vindex is unique.
     * A Unique Vindex is allowed to return non-unique values like
     * a keyrange. This is in situations where the vindex does not
     * have enough information to map to a keyspace id. If so, such
     * a vindex cannot be primary.
     *
     * @return Boolean
     */
    @Override
    public abstract Boolean isUnique();

    /**
     * NeedsVCursor returns true if the Vindex makes calls into the
     * VCursor. Such vindexes cannot be used by vreplication.
     *
     * @return Boolean
     */
    @Override
    public abstract Boolean needsVcursor();

    /**
     * Map can map ids to key.Destination objects.
     * If the Vindex is unique, each id would map to either
     * a KeyRange, or a single KeyspaceID.
     * If the Vindex is non-unique, each id would map to either
     * a KeyRange, or a list of KeyspaceID.
     *
     * @param ids array of {@link VtValue}
     * @return array of {@link Destination}
     */
    @Override
    public abstract Destination[] map(VtValue[] ids);

    @Override
    public Boolean[] verify(VtValue[] ids, byte[][] ksids) throws SQLException {
        Boolean[] out = new Boolean[ids.length];
        for (int i = 0; i < ids.length; i++) {
            BigInteger num = EvalEngine.toUint64(ids[i]);
            out[i] = new String(vhash(num)).equals(new String(ksids[i]));
        }
        return out;
    }

    @Override
    public VtValue[] reverseMap(byte[][] ksids) throws SQLException {
        List<VtValue> reverseIdList = new ArrayList<>();
        for (byte[] keyspaceId : ksids) {
            long val = vunhash(keyspaceId);
            reverseIdList.add(VtValue.newVtValue(Query.Type.UINT64, String.valueOf(val).getBytes()));
        }
        return reverseIdList.toArray(new VtValue[0]);
    }
}
