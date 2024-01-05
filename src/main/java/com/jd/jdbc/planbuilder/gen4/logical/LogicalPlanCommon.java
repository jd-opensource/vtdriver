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
import com.jd.jdbc.planbuilder.Builder;
import com.jd.jdbc.planbuilder.Jointab;
import com.jd.jdbc.planbuilder.ResultColumn;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import java.sql.SQLException;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class LogicalPlanCommon implements LogicalPlan {

    private int order;

    protected LogicalPlan input;

    @Override
    public Integer order() {
        return input.order();
    }

    @Override
    public List<ResultColumn> resultColumns() {
        return null;
    }

    @Override
    public void reorder(Integer order) {
        this.getInput().reorder(order);
        this.order = this.getInput().order() + 1;
    }

    @Override
    public void wireup(Builder bldr, Jointab jt) throws SQLException {
        this.getInput().wireup(bldr, jt);
    }

    @Override
    public void wireupGen4(PlanningContext ctx) throws SQLException {
        this.getInput().wireupGen4(ctx);
    }

    @Override
    public void supplyVar(Integer from, Integer to, SQLName colName, String varName) throws SQLException {
        this.getInput().supplyVar(from, to, colName, varName);
    }

    @Override
    public Builder.SupplyColResponse supplyCol(SQLName col) throws SQLException {
        return this.getInput().supplyCol(col);
    }

    @Override
    public Integer supplyWeightString(Integer colNumber) throws SQLException {
        return this.getInput().supplyWeightString(colNumber);
    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        return this.getInput().getPrimitiveEngine();
    }

    @Override
    public LogicalPlan[] inputs() throws SQLException {
        return new LogicalPlan[] {this.getInput()};
    }

    @Override
    public LogicalPlan[] rewrite(LogicalPlan... inputs) throws SQLException {
        if (inputs.length != 1) {
            throw new SQLException("builderCommon: wrong number of inputs");
        }
        this.input = inputs[0];
        return null;
    }

    @Override
    public TableSet containsTables() {
        return this.getInput().containsTables();
    }

    @Override
    public List<SQLSelectItem> outputColumns() throws SQLException {
        return this.getInput().outputColumns();
    }

    @Override
    public void setUpperLimit(SQLExpr count) throws SQLException {
        this.getInput().setUpperLimit(count);
    }
}
