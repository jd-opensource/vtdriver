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

package com.jd.jdbc.vindexes;

import com.jd.jdbc.key.Destination;
import com.jd.jdbc.sqltypes.VtValue;
import java.sql.SQLException;

public interface SingleColumn extends Vindex {

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
    Destination[] map(VtValue[] ids);

    /**
     * Verify returns true for every id that successfully maps to the
     * specified keyspace id.
     *
     * @param ids   array of {@link VtValue}
     * @param ksids two dimensional array or byte
     * @return array of {@link Boolean}
     * @throws SQLException when an error occurs
     */
    Boolean[] verify(VtValue[] ids, byte[][] ksids) throws SQLException;
}
