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

package com.jd.jdbc.engine.gen4;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OrderByParamsGen4 {
    private int col;

    private boolean desc;

    // WeightStringCol is the weight_string column that will be used for sorting.
    // It is set to -1 if such a column is not added to the query
    private int weightStrCol;

    private int starColFixedIndex;

    // v3 specific boolean. Used to also add weight strings originating from GroupBys to the Group by clause
    private boolean fromGroupBy;

    // Collation ID for comparison using collation
    private Integer collationID;

    public OrderByParamsGen4(int col, boolean desc, int weightStringCol, int starColFixedIndex, Integer collationID) {
        this.col = col;
        this.desc = desc;
        this.weightStrCol = weightStringCol;
        this.starColFixedIndex = starColFixedIndex;
        this.collationID = collationID;
    }

    public OrderByParamsGen4(int col, boolean desc, int weightStrCol, Integer collationID) {
        this.col = col;
        this.desc = desc;
        this.weightStrCol = weightStrCol;
        this.collationID = collationID;
    }
}
