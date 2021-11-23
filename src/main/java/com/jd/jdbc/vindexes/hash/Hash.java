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
import com.jd.jdbc.key.DestinationNone;
import com.jd.jdbc.sqltypes.VtValue;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.SQLException;

/**
 * Hash defines vindex that hashes an int64 to a KeyspaceId
 * by using null-key DES hash. It's Unique, Reversible and
 * Functional.
 * Note that at once stage we used a 3DES-based hash here,
 * but for a null key as in our case, they are completely equivalent.
 */
public class Hash extends AbstractHash {

    @Override
    public String toString() {
        return "hash";
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

    @Override
    public Destination[] map(VtValue[] ids) {
        Destination[] out = new Destination[ids.length];
        for (int i = 0; i < ids.length; i++) {
            VtValue id = ids[i];
            if (id.isSigned() || id.isUnsigned()) {
                ByteBuffer buffer = ByteBuffer.allocate(8);
                buffer.put(id.raw(), 0, id.raw().length);
                buffer.flip();
                long num = buffer.getLong();
                out[i] = new DestinationKeyspaceID(vhash(new BigInteger(String.valueOf(num))));
                continue;
            }
            out[i] = new DestinationNone();
        }
        return out;
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
