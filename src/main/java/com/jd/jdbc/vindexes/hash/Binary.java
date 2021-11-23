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
import io.vitess.proto.Query;
import java.sql.SQLException;

/**
 * Binary is a vindex that converts binary bits to a keyspace id.
 */
public class Binary extends AbstractHash {

    @Override
    public String toString() {
        return "binary";
    }

    @Override
    public Integer cost() {
        return 0;
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
            out[i] = new DestinationKeyspaceID(ids[i].toBytes());
        }
        return out;
    }

    @Override
    public Boolean[] verify(VtValue[] ids, byte[][] ksids) throws SQLException {
        return super.verify(ids, ksids);
    }

    @Override
    public VtValue[] reverseMap(byte[][] ksids) throws SQLException {
        VtValue[] reverseIds = new VtValue[ksids.length];
        for (int i = 0; i < ksids.length; i++) {
            byte[] keyspaceIds = ksids[i];
            if (keyspaceIds == null) {
                throw new SQLException("Binary.reverseMap: keyspaceId is nil");
            }
            reverseIds[i] = VtValue.newVtValue(Query.Type.VARBINARY, keyspaceIds);
        }
        return reverseIds;
    }
}
