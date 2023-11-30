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

import com.jd.jdbc.common.tuple.Pair;
import com.jd.jdbc.context.PlanningContext;
import com.jd.jdbc.evalengine.TranslationLookup;
import com.jd.jdbc.planbuilder.gen4.ProjectionPushing;
import com.jd.jdbc.planbuilder.gen4.logical.LogicalPlan;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import java.sql.SQLException;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SimpleConverterLookup implements TranslationLookup {

    private PlanningContext ctx;

    private LogicalPlan plan;

    private boolean canPushProjection;

    public SimpleConverterLookup(PlanningContext ctx, LogicalPlan plan) {
        this.ctx = ctx;
        this.plan = plan;
    }

    public SimpleConverterLookup(PlanningContext ctx, LogicalPlan plan, boolean canPushProjection) {
        this.ctx = ctx;
        this.plan = plan;
        this.canPushProjection = canPushProjection;
    }

    @Override
    public int columnLookup(SQLName col) throws SQLException {
        Pair<Integer, Integer> result = ProjectionPushing.pushProjection(ctx, new SQLSelectItem(col), plan, true, true, false);
        if (result.getRight() == 1 && !this.canPushProjection) {
            throw new SQLException("column should not be pushed to projection while doing a column lookup");
        }
        return result.getLeft();
    }

    @Override
    public int collationForExpr(SQLExpr expr) {
        return this.getCtx().getSemTable().collationForExpr(expr);
    }

    @Override
    public int defaultCollation() {
        return this.getCtx().getSemTable().getCollation();
    }
}
