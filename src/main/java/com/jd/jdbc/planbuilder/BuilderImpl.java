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

import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import java.sql.SQLException;
import java.util.List;
import lombok.Data;

/**
 * BuilderImpl implements some common functionality of builders.
 * Make sure to override in case behavior needs to be changed.
 */
@Data
public class BuilderImpl implements Builder {

    private Integer order;

    private Builder bldr;

    public BuilderImpl(Builder bldr) {
        this.bldr = bldr;
    }

    public static BuilderImpl newBuilderImpl(Builder input) {
        return new BuilderImpl(input);
    }

    @Override
    public Integer order() {
        return this.order;
    }

    @Override
    public void reorder(Integer order) {
        this.bldr.reorder(order);
        this.order = this.bldr.order() + 1;
    }

    @Override
    public List<ResultColumn> resultColumns() {
        return this.bldr.resultColumns();
    }

    @Override
    public Builder first() throws SQLException {
        return this.bldr.first();
    }

    @Override
    public void pushFilter(PrimitiveBuilder pb, SQLExpr filter, String whereType, Builder origin) throws SQLException {

    }

    @Override
    public PushSelectResponse pushSelect(PrimitiveBuilder pb, SQLSelectItem expr, Builder origin) throws SQLException {
        return null;
    }

    @Override
    public void makeDistinct() throws SQLException {

    }

    @Override
    public void pushGroupBy(SQLSelectGroupByClause groupBy) throws SQLException {

    }

    @Override
    public Builder pushOrderBy(SQLOrderBy orderBy) throws SQLException {
        return null;
    }

    @Override
    public void setUpperLimit(SQLExpr count) throws SQLException {
        this.bldr.setUpperLimit(count);
    }

    @Override
    public void pushMisc(MySqlSelectQueryBlock sel) {
        this.bldr.pushMisc(sel);
    }

    @Override
    public void wireup(Builder bldr, Jointab jt) throws SQLException {
        this.bldr.wireup(bldr, jt);
    }

    @Override
    public void supplyVar(Integer from, Integer to, SQLName colName, String varName) throws SQLException {
        this.bldr.supplyVar(from, to, colName, varName);
    }

    @Override
    public SupplyColResponse supplyCol(SQLName col) throws SQLException {
        return this.bldr.supplyCol(col);
    }

    @Override
    public Integer supplyWeightString(Integer colNumber) throws SQLException {
        return this.bldr.supplyWeightString(colNumber);
    }

    @Override
    public void pushLock(String lock) throws SQLException {
        this.bldr.pushLock(lock);
    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        return null;
    }

    @Override
    public boolean isSplitTablePlan() {
        return bldr.isSplitTablePlan();
    }
}
