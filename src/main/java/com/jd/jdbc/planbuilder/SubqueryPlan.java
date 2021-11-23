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
import com.jd.jdbc.engine.SubQueryEngine;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SubqueryPlan extends BuilderImpl {
    private final List<ResultColumn> resultColumnList;

    private final SubQueryEngine subQueryEngine;

    public SubqueryPlan(Builder bldr) {
        super(bldr);
        this.resultColumnList = new ArrayList<>();
        this.subQueryEngine = new SubQueryEngine();
    }

    @Override
    public List<ResultColumn> resultColumns() {
        return this.resultColumnList;
    }

    @Override
    public Builder first() {
        return this;
    }

    @Override
    public void pushFilter(PrimitiveBuilder pb, SQLExpr filter, String whereType, Builder origin) throws SQLException {
        throw new SQLException("unsupported: filtering on results of cross-shard subquery");
    }

    @Override
    public PushSelectResponse pushSelect(PrimitiveBuilder pb, SQLSelectItem expr, Builder origin) throws SQLException {
        if (!(expr.getExpr() instanceof SQLName)) {
            throw new SQLException("unsupported: expression on results of a cross-shard subquery");
        }

        // colNumber should already be set for subquery columns.
        Integer inner = ((SQLName) expr.getExpr()).getMetadata().getColNumber();
        this.subQueryEngine.getCols().add(inner);

        // Build a new column reference to represent the result column.
        ResultColumn rc = Symtab.newResultColumn(expr, this);
        this.resultColumnList.add(rc);

        return new PushSelectResponse(rc, this.resultColumnList.size() - 1);
    }

    @Override
    public void makeDistinct() throws SQLException {
        throw new SQLException("unsupported: distinct on cross-shard subquery");
    }

    @Override
    public void pushGroupBy(SQLSelectGroupByClause groupBy) throws SQLException {
        if (groupBy == null) {
            return;
        }
        throw new SQLException("unsupported: group by on cross-shard subquery");
    }

    @Override
    public Builder pushOrderBy(SQLOrderBy orderBy) throws SQLException {
        if (orderBy == null || orderBy.getItems() == null || orderBy.getItems().isEmpty()) {
            return this;
        }
        return MemorySortPlan.newMemorySortPlan(this, orderBy);
    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        this.subQueryEngine.setSubqueryEngine(this.getBldr().getPrimitiveEngine());
        return this.subQueryEngine;
    }

    @Override
    public SupplyColResponse supplyCol(SQLName col) {
        Column c = col.getMetadata();
        for (int i = 0; i < this.resultColumnList.size(); i++) {
            ResultColumn rc = this.resultColumnList.get(i);
            if (rc.getColumn() == c) {
                return new SupplyColResponse(rc, i);
            }
        }

        // columns that reference subqueries will have their colNumber set.
        // Let's use it here.
        this.subQueryEngine.getCols().add(c.getColNumber());
        this.resultColumnList.add(new ResultColumn(c));
        return new SupplyColResponse(null, this.resultColumnList.size() - 1);
    }
}
