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

import com.jd.jdbc.context.PlanningContext;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import java.sql.SQLException;
import java.util.List;

/**
 * hashJoin is used to build a HashJoin primitive.
 */
public class HashJoinPlan extends AbstractGen4Plan{
    @Override
    public void wireupGen4(PlanningContext ctx) throws SQLException {

    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        return null;
    }

    @Override
    public LogicalPlan[] inputs() throws SQLException {
        return new LogicalPlan[0];
    }

    @Override
    public LogicalPlan[] rewrite(LogicalPlan... inputs) throws SQLException {
        return new LogicalPlan[0];
    }

    @Override
    public TableSet containsTables() {
        return null;
    }

    @Override
    public List<SQLSelectItem> outputColumns() throws SQLException {
        return null;
    }
}
