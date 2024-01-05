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

package com.jd.jdbc.planbuilder.gen4.logical;

import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.gen4.MemorySortGen4Engine;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import java.sql.SQLException;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MemorySortGen4Plan extends ResultsBuilder implements LogicalPlan {

    private MemorySortGen4Engine eMemorySort;

    public MemorySortGen4Plan() {

    }

    private int findColNumber(MemorySortGen4Plan ms, SQLSelectItem expr) {
        return -1;
    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        PrimitiveEngine primitiveEngine = this.getInput().getPrimitiveEngine();
        this.getEMemorySort().setInput(primitiveEngine);
        return this.eMemorySort;
    }

    @Override
    public void setUpperLimit(SQLExpr count) throws SQLException {
        throw new SQLException("memorySort.Limit: unreachable");
    }
}
