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

import com.jd.jdbc.sqltypes.VtValue;
import java.sql.SQLException;

/**
 * A Reversible vindex is one that can perform a
 * reverse lookup from a keyspace id to an id. This
 * is optional. If present, VTGate can use it to
 * fill column values based on the target keyspace id.
 * Reversible is supported only for SingleColumn vindexes.
 */
public interface Reversible {

    /**
     * reverse map
     *
     * @param ksids two dimensional array or byte
     * @return array of {@link VtValue}
     * @throws SQLException when an error occurs
     */
    VtValue[] reverseMap(byte[][] ksids) throws SQLException;
}
