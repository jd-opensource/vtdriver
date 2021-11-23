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
import com.jd.jdbc.engine.SetEngine;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSetStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import java.sql.SQLException;
import java.util.List;
import lombok.Setter;

public class SetPlan implements Builder {
    @Setter
    private SetEngine eSet;

    private Integer order;

    public static PrimitiveEngine buildSetPlan(SQLSetStatement stmt) {
        return new SetEngine(null, null, null, stmt);
    }

    @Override
    public Integer order() {
        return this.order;
    }

    @Override
    public List<ResultColumn> resultColumns() {
        return null;
    }

    @Override
    public void reorder(Integer order) {

    }

    @Override
    public Builder first() {
        return this;
    }

    @Override
    public void makeDistinct() {

    }

    @Override
    public void pushGroupBy(SQLSelectGroupByClause groupBy) {

    }

    @Override
    public Builder pushOrderBy(SQLOrderBy orderBy) throws SQLException {
        return null;
    }

    @Override
    public void setUpperLimit(SQLExpr count) {

    }

    @Override
    public void pushMisc(MySqlSelectQueryBlock sel) {

    }

    @Override
    public void wireup(Builder bldr, Jointab jt) {

    }

    @Override
    public void supplyVar(Integer from, Integer to, SQLName colName, String varName) throws SQLException {

    }

    @Override
    public SupplyColResponse supplyCol(SQLName col) {
        return null;
    }

    @Override
    public Integer supplyWeightString(Integer colNumber) {
        return null;
    }

    @Override
    public void pushLock(String lock) throws SQLException {

    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() {

        return eSet;
    }

    @Override
    public boolean isSplitTablePlan() {
        return false;
    }
}
