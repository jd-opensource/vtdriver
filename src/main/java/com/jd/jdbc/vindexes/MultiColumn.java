/*
Copyright 2023 JD Project Authors. Licensed under Apache-2.0.

Copyright 2022 The Vitess Authors.

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

public interface MultiColumn extends Vindex{
    /*
    Map(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value) ([]key.Destination, error)
    Verify(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte) ([]bool, error)
     */
    /**
     *
     * @param rowsColValues array of {@link VtValue}
     * @return array of {@link Destination}
     */
    Destination[] map(VtValue[][] rowsColValues);

    /**
     *
     * @param rowsColValues   array of {@link VtValue}
     * @param ksids two dimensional array or byte
     * @return array of {@link Boolean}
     * @throws SQLException when an error occurs
     */
    Boolean[] verify(VtValue[][] rowsColValues, byte[][] ksids) throws SQLException;

    /**
     * PartialVindex returns true if subset of columns can be passed in to the vindex Map and Verify function.
     * @return
     */
    //
    Boolean PartialVindex();
}
