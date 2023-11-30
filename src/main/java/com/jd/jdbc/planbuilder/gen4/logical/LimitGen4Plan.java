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
import com.jd.jdbc.engine.gen4.LimitGen4Engine;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.planbuilder.Builder;
import com.jd.jdbc.planbuilder.Jointab;
import com.jd.jdbc.planbuilder.ResultColumn;
import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLLimit;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import java.sql.SQLException;
import java.util.List;

public class LimitGen4Plan extends LogicalPlanCommon implements LogicalPlan {
    private LimitGen4Engine elimit;

    // newLimit builds a new limit.
    public LimitGen4Plan(LogicalPlan plan) {
        super.input = plan;
        this.elimit = new LimitGen4Engine();
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
        return null;
    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        this.elimit.setInput(this.getInput().getPrimitiveEngine());
        return this.elimit;
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

    public static LogicalPlan createLimitPlan(LogicalPlan plan, SQLLimit limit) throws SQLException {
        LimitGen4Plan limitPlan = new LimitGen4Plan(plan);
        limitPlan.setLimit(limit);
        return limitPlan;
    }

    private void setLimit(SQLLimit limit) throws SQLException {
        EvalEngine.Expr pv;
        SemTable emptySemTable = new SemTable();
        try {
            pv = EvalEngine.translate(limit.getRowCount(), emptySemTable);
        } catch (SQLException e) {
            throw new SQLException("unexpected expression in LIMIT: " + e.getMessage(), e);
        }
        this.elimit.setCount(pv);

        if (limit.getOffset() != null) {
            EvalEngine.Expr pvOffset;
            try {
                pvOffset = EvalEngine.translate(limit.getRowCount(), emptySemTable);
            } catch (SQLException e) {
                throw new SQLException("unexpected expression in OFFSET: " + e.getMessage(), e);
            }
            this.elimit.setOffset(pvOffset);
        }
    }

    @Override
    public void setUpperLimit(SQLExpr count) {
    }

}
