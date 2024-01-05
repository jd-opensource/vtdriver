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

import com.jd.jdbc.sqlparser.ast.SQLExpr;
import lombok.Getter;
import lombok.Setter;

/**
 * GroupByParams specify the grouping key to be used.
 */
@Getter
@Setter
public class GroupByParams {

    private Integer keyCol;

    private Integer weightStringCol;

    private SQLExpr expr;

    private Boolean fromGroupBy;

    private int collationID;

    public GroupByParams() {

    }

    public GroupByParams(SQLExpr expr, Boolean fromGroupBy) {
        this.expr = expr;
        this.fromGroupBy = fromGroupBy;
    }

    public GroupByParams(Integer keyCol, Integer weightStringCol) {
        this.keyCol = keyCol;
        this.weightStringCol = weightStringCol;
    }
}
