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
import com.jd.jdbc.engine.gen4.SimpleProjectionGen4Engine;
import com.jd.jdbc.planbuilder.Builder;
import com.jd.jdbc.planbuilder.Jointab;
import com.jd.jdbc.planbuilder.ResultColumn;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;


// simpleProjection is used for wrapping a derived table.
// This primitive wraps any derived table that results
// in something that's not a route. It builds a
// 'table' for the derived table allowing higher level
// constructs to reference its columns. If a derived table
// results in a route primitive, we instead build
// a new route that keeps the subquery in the FROM
// clause, because a route is more versatile than
// a simpleProjection.
// this should not be used by the gen4 planner
@Getter
@Setter
public class SimpleProjectionGen4Plan extends LogicalPlanCommon implements LogicalPlan {

    private List<ResultColumn> resultColumns;

    private SimpleProjectionGen4Engine eSimpleProjection;

    public SimpleProjectionGen4Plan(LogicalPlan plan) {
        this.setInput(plan);
        this.setESimpleProjection(new SimpleProjectionGen4Engine());
    }

    @Override
    public Integer order() {
        return null;
    }

    @Override
    public List<ResultColumn> resultColumns() {
        return this.getResultColumns();
    }

    @Override
    public void reorder(Integer order) {

    }

    @Override
    public void wireup(Builder bldr, Jointab jt) throws SQLException {

    }

    @Override
    public void supplyVar(Integer from, Integer to, SQLName colName, String varName) throws SQLException {

    }

    @Override
    public Builder.SupplyColResponse supplyCol(SQLName col) throws SQLException {
        return null;
    }

    @Override
    public Integer supplyWeightString(Integer colNumber) throws SQLException {
        return null;
    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        this.getESimpleProjection().setInput(this.getInput().getPrimitiveEngine());
        return this.getESimpleProjection();
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
    public void setUpperLimit(SQLExpr count) throws SQLException {

    }

    @Override
    public List<SQLSelectItem> outputColumns() throws SQLException {
        List<SQLSelectItem> exprs = new ArrayList<>(this.getESimpleProjection().getCols().size());
        List<SQLSelectItem> outputCols = this.getInput().outputColumns();
        for (Integer colID : this.getESimpleProjection().getCols()) {
            exprs.add(outputCols.get(colID));
        }
        return exprs;
    }
}
