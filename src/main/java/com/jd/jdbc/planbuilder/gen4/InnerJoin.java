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

package com.jd.jdbc.planbuilder.gen4;

import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

public class InnerJoin {

    @Getter
    private TableSet deps;

    @Getter
    private List<SQLExpr> exprs = new ArrayList<>();

    public InnerJoin(TableSet ts, List<SQLExpr> asList) {
        this.deps = ts;
        this.exprs = asList;
    }
}
