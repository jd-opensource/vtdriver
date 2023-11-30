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

public abstract class AbstractGen4Plan implements LogicalPlan {

    public AbstractGen4Plan() {
    }

    @Override
    public Integer order() {
        return null;
    }

    @Override
    public List<ResultColumn> resultColumns() {
        return null;
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
        throw new SQLException("[BUG]: should not be called. This is a Gen4 primitive");
       //return null;
    }

    // abstract

    public abstract void wireupGen4(PlanningContext ctx) throws SQLException;

    public abstract PrimitiveEngine getPrimitiveEngine() throws SQLException;

    public abstract LogicalPlan[] inputs() throws SQLException;

    public abstract LogicalPlan[] rewrite(LogicalPlan... inputs) throws SQLException;

    public abstract TableSet containsTables();

    public abstract List<SQLSelectItem> outputColumns() throws SQLException;

    public void setUpperLimit(SQLExpr count) {
    }
}
