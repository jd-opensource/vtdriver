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
import lombok.Getter;

public class VitessCompare {
    private int orderBy;

    private int weightString;

    private final int starColFixedIndex;

    //    collationID collations.ID

    @Getter
    private final boolean desc;

    public VitessCompare(int orderBy, int weightString, boolean desc, int starColFixedIndex) {
        this.orderBy = orderBy;
        this.weightString = weightString;
        this.desc = desc;
        this.starColFixedIndex = starColFixedIndex;
    }

    public int compare(List<VtResultValue> r1, List<VtResultValue> r2) throws SQLException {
        int colIndex;
        if (this.starColFixedIndex > this.orderBy && this.starColFixedIndex < r1.size()) {
            colIndex = this.starColFixedIndex;
        } else {
            colIndex = this.orderBy;
        }
        int cmp = 0;
        if (this.weightString != -1) {
            // in case of a comparison or collation error switch to using the weight string column for ordering
            this.orderBy = this.weightString;
            this.weightString = -1;
            cmp = EvalEngine.nullSafeCompare(r1.get(this.orderBy), r2.get(this.orderBy));
        } else {
            cmp = EvalEngine.nullSafeCompare(r1.get(colIndex), r2.get(colIndex));
        }
        // change the result if descending ordering is required
        if (this.desc) {
            cmp = -cmp;
        }
        return cmp;
    }

    /**
     * extractSlices extracts the three fields of OrderByParams into a slice of comparers
     * @param orderBy
     * @return
     */
    public static List<VitessCompare> extractSlices(List<OrderByParamsGen4> orderBy) {
        List<VitessCompare> compares = new ArrayList<>(orderBy.size());
        for (OrderByParamsGen4 order : orderBy) {
            VitessCompare vitessCompare = new VitessCompare(order.getCol(), order.getWeightStrCol(), order.isDesc(), order.getStarColFixedIndex());
            compares.add(vitessCompare);
        }
        return compares;
    }
}
