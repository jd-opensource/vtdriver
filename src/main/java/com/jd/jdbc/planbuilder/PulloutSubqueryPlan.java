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

import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.PulloutSubqueryEngine;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import java.sql.SQLException;
import java.util.List;

public class PulloutSubqueryPlan implements Builder {
    private final Builder subquery;

    private final PulloutSubqueryEngine subqueryEngine;

    private Integer order;

    private Builder underlying;

    public PulloutSubqueryPlan(Engine.PulloutOpcode opcode, String sqName, String hasValues, Builder subquery) {
        this.subquery = subquery;
        this.subqueryEngine = new PulloutSubqueryEngine(opcode, sqName, hasValues);
    }

    public void setUnderlying(Builder underlying) {
        this.underlying = underlying;
        this.underlying.reorder(this.subquery.order());
        this.order = this.underlying.order() + 1;
    }

    @Override
    public Integer order() {
        return this.order;
    }

    @Override
    public List<ResultColumn> resultColumns() {
        return this.underlying.resultColumns();
    }

    @Override
    public void reorder(Integer order) {
        this.subquery.reorder(order);
        this.underlying.reorder(this.subquery.order());
        this.order = this.underlying.order() + 1;
    }

    @Override
    public Builder first() throws SQLException {
        return this.underlying.first();
    }

    @Override
    public void pushFilter(PrimitiveBuilder pb, SQLExpr filter, String whereType, Builder origin) throws SQLException {
        this.underlying.pushFilter(pb, filter, whereType, origin);
    }

    @Override
    public PushSelectResponse pushSelect(PrimitiveBuilder pb, SQLSelectItem expr, Builder origin) throws SQLException {
        return this.underlying.pushSelect(pb, expr, origin);
    }

    @Override
    public void makeDistinct() throws SQLException {
        this.underlying.makeDistinct();
    }

    @Override
    public void pushGroupBy(SQLSelectGroupByClause groupBy) throws SQLException {
        this.underlying.pushGroupBy(groupBy);
    }

    @Override
    public Builder pushOrderBy(SQLOrderBy orderBy) throws SQLException {
        this.underlying = this.underlying.pushOrderBy(orderBy);
        return this;
    }

    @Override
    public void setUpperLimit(SQLExpr count) throws SQLException {
        this.underlying.setUpperLimit(count);
    }

    @Override
    public void pushMisc(MySqlSelectQueryBlock sel) {
        this.subquery.pushMisc(sel);
        this.underlying.pushMisc(sel);
    }

    @Override
    public void wireup(Builder bldr, Jointab jt) throws SQLException {
        this.underlying.wireup(bldr, jt);
        this.subquery.wireup(bldr, jt);
    }

    @Override
    public void supplyVar(Integer from, Integer to, SQLName colName, String varName) throws SQLException {
        if (from <= this.subquery.order()) {
            this.subquery.supplyVar(from, to, colName, varName);
            return;
        }
        this.underlying.supplyVar(from, to, colName, varName);
    }

    @Override
    public SupplyColResponse supplyCol(SQLName col) throws SQLException {
        return this.underlying.supplyCol(col);
    }

    @Override
    public Integer supplyWeightString(Integer colNumber) throws SQLException {
        return this.underlying.supplyWeightString(colNumber);
    }

    @Override
    public void pushLock(String lock) throws SQLException {
        this.subquery.pushLock(lock);
        this.underlying.pushLock(lock);
    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        this.subqueryEngine.setSubquery(this.subquery.getPrimitiveEngine());
        this.subqueryEngine.setUnderlying(this.underlying.getPrimitiveEngine());
        return this.subqueryEngine;
    }

    @Override
    public boolean isSplitTablePlan() {
        return underlying.isSplitTablePlan() || subquery.isSplitTablePlan();
    }
}
