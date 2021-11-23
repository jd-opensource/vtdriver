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

import com.jd.jdbc.engine.LimitEngine;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.sqlparser.SqlParser;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLLimit;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqltypes.VtPlanValue;
import java.sql.SQLException;

public class LimitPlan extends BuilderImpl {
    private final LimitEngine limitEngine;

    public LimitPlan(Builder builder) {
        super(builder);
        this.limitEngine = new LimitEngine();
    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        this.limitEngine.setInput(this.getBldr().getPrimitiveEngine());
        return this.limitEngine;
    }

    @Override
    public void pushFilter(PrimitiveBuilder pb, SQLExpr filter, String whereType, Builder origin) throws SQLException {
        throw new SQLException("limit.PushFilter: unreachable");
    }

    @Override
    public PushSelectResponse pushSelect(PrimitiveBuilder pb, SQLSelectItem expr, Builder origin) throws SQLException {
        throw new SQLException("limit.PushSelect: unreachable");
    }

    @Override
    public void makeDistinct() throws SQLException {
        throw new SQLException("limit.MakeDistinct: unreachable");
    }

    @Override
    public void pushGroupBy(SQLSelectGroupByClause groupBy) throws SQLException {
        throw new SQLException("limit.PushGroupBy: unreachable");
    }

    @Override
    public Builder pushOrderBy(SQLOrderBy orderBy) throws SQLException {
        throw new SQLException("limit.PushOrderBy: unreachable");
    }

    /**
     * SetLimit sets the limit for the primitive. It calls the underlying
     * primitive's SetUpperLimit, which is an optimization hint that informs
     * the underlying primitive that it doesn't need to return more rows than
     * specified.
     *
     * @param limit
     * @throws SQLException
     */
    public void setLimit(SQLLimit limit) throws SQLException {
        VtPlanValue pv;
        try {
            pv = SqlParser.newPlanValue(limit.getRowCount());
        } catch (SQLException e) {
            throw new SQLException("unexpected expression in LIMIT: " + e.getMessage(), e);
        }
        this.limitEngine.setCount(pv);

        if (limit.getOffset() != null) {
            try {
                pv = SqlParser.newPlanValue(limit.getOffset());
            } catch (SQLException e) {
                throw new SQLException("unexpected expression in OFFSET: " + e.getMessage(), e);
            }
            this.limitEngine.setOffset(pv);
        }
        SQLVariantRefExpr upperLimitVar = new SQLVariantRefExpr();
        upperLimitVar.setName(":__upper_limit");
        upperLimitVar.setIndex(-2);
        this.getBldr().setUpperLimit(upperLimitVar);
    }


    /**
     * SetUpperLimit satisfies the builder interface.
     * This is a no-op because we actually call SetLimit for this primitive.
     * In the future, we may have to honor this call for subqueries.
     *
     * @param count
     */
    @Override
    public void setUpperLimit(SQLExpr count) {
    }

    @Override
    public void pushLock(String lock) throws SQLException {
        super.getBldr().pushLock(lock);
    }
}
