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

package com.jd.jdbc.planbuilder;

import com.google.common.collect.Lists;
import com.jd.jdbc.engine.ConcatenateEngine;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import java.sql.SQLException;
import java.util.List;

public class ConcatenatePlan implements Builder {
    private final Builder lhs;

    private final Builder rhs;

    private Integer order;

    public ConcatenatePlan(Builder lhs, Builder rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public Integer order() {
        return this.order;
    }

    @Override
    public List<ResultColumn> resultColumns() {
        return this.lhs.resultColumns();
    }

    @Override
    public void reorder(Integer order) {
        this.lhs.reorder(order);
        this.rhs.reorder(this.lhs.order());
        this.order = this.rhs.order() + 1;
    }

    @Override
    public Builder first() throws SQLException {
        throw new SQLException("implement me");
    }

    @Override
    public void pushFilter(PrimitiveBuilder pb, SQLExpr filter, String whereType, Builder origin) throws SQLException {
        throw new SQLException("concatenate.Filter: unreachable");
    }

    @Override
    public PushSelectResponse pushSelect(PrimitiveBuilder pb, SQLSelectItem expr, Builder origin) throws SQLException {
        throw new SQLException("concatenate.Select: unreachable");
    }

    @Override
    public void makeDistinct() throws SQLException {
        throw new SQLException("only union-all is supported for this operator");
    }

    @Override
    public void pushGroupBy(SQLSelectGroupByClause groupBy) throws SQLException {
        throw new SQLException("concatenate.GroupBy: unreachable");
    }

    @Override
    public Builder pushOrderBy(SQLOrderBy orderBy) throws SQLException {
        if (orderBy == null) {
            return this;
        }
        throw new SQLException("concatenate.OrderBy: unreachable");
    }

    @Override
    public void setUpperLimit(SQLExpr count) throws SQLException {
        // not doing anything by design
    }

    @Override
    public void pushMisc(MySqlSelectQueryBlock sel) {
        this.lhs.pushMisc(sel);
        this.rhs.pushMisc(sel);
    }

    @Override
    public void wireup(Builder bldr, Jointab jt) throws SQLException {
        // TODO systay should we do something different here?
        this.lhs.wireup(bldr, jt);
        this.rhs.wireup(bldr, jt);
    }

    @Override
    public void supplyVar(Integer from, Integer to, SQLName colName, String varName) throws SQLException {
        throw new SQLException("implement me");
    }

    @Override
    public SupplyColResponse supplyCol(SQLName col) throws SQLException {
        throw new SQLException("implement me");
    }

    @Override
    public Integer supplyWeightString(Integer colNumber) throws SQLException {
        throw new SQLException("implement me");
    }

    @Override
    public void pushLock(String lock) throws SQLException {
        this.lhs.pushLock(lock);
        this.rhs.pushLock(lock);
    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        PrimitiveEngine lhs = this.lhs.getPrimitiveEngine();
        PrimitiveEngine rhs = this.rhs.getPrimitiveEngine();

        return new ConcatenateEngine(Lists.newArrayList(lhs, rhs));
    }

    @Override
    public boolean isSplitTablePlan() {
        return lhs.isSplitTablePlan() || lhs.isSplitTablePlan();
    }
}
