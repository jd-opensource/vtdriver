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

package com.jd.jdbc.engine.gen4;

import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.sqltypes.VtResultValue;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ProbeTable {
    Map<Long, List<List<VtResultValue>>> seenRows;

    List<CheckCol> checkCols;

    public ProbeTable(Map<Long, List<List<VtResultValue>>> seenRows, List<CheckCol> checkCols) {
        this.seenRows = seenRows;
        this.checkCols = checkCols;
    }

    public boolean exists(List<VtResultValue> inputRow) throws SQLException {
        // the two prime numbers used here (17 and 31) are used to
        // calculate hashcode from all column values in the input sqltypes.Row
        long code = hashCodeForRow(inputRow);
        List<List<VtResultValue>> existingRows = seenRows.get(code);
        if (existingRows == null) {
            // nothing with this hash code found, we can be sure it's a not seen sqltypes.Row
            List<List<VtResultValue>> lists = new ArrayList<>();
            lists.add(inputRow);
            seenRows.put(code, lists);
            return false;
        }

        // we found something in the map - still need to check all individual values
        // so we don't just fall for a hash collision
        for (List<VtResultValue> existingRow : existingRows) {
            boolean exists = equal(existingRow, inputRow);
            if (exists) {
                return true;
            }
        }
        existingRows.add(inputRow);
        return false;
    }

    private boolean equal(List<VtResultValue> a, List<VtResultValue> b) throws SQLException {
        for (int i = 0; i < this.checkCols.size(); i++) {
            CheckCol checkCol = this.checkCols.get(i);
            int cmp = EvalEngine.nullSafeCompare(a.get(i), b.get(i));
            if (cmp != 0) {
                return false;
            }
        }
        return true;
    }

    private long hashCodeForRow(List<VtResultValue> inputRow) throws SQLException {
        long code = 17;
        for (int i = 0; i < checkCols.size(); i++) {
            CheckCol checkCol = checkCols.get(i);
            if (i >= inputRow.size()) {
                throw new RuntimeException("Distinct check cols is larger than its input row.");
            }
            VtResultValue col = inputRow.get(checkCol.getCol());
            long hashcode = EvalEngine.nullsafeHashcode(col, checkCol.getCollation(), col.getVtType());
            code = code * 31 + hashcode;
        }
        return code;
    }
}
