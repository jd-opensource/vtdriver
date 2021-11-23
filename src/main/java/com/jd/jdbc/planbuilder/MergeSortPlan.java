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

import com.jd.jdbc.engine.OrderByParams;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.planbuilder.tableplan.TableRoutePlan;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqltypes.VtType;
import java.sql.SQLException;
import java.util.List;

public class MergeSortPlan extends ResultsBuilder implements Builder, Truncater {

    private Integer truncateColumnCount = 0;

    public MergeSortPlan(RoutePlan builder) {
        super(builder, null);
        this.setTruncater(this);
    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        return this.getBldr().getPrimitiveEngine();
    }

    @Override
    public void pushFilter(PrimitiveBuilder pb, SQLExpr filter, String whereType, Builder origin) throws SQLException {
        this.getBldr().pushFilter(pb, filter, whereType, origin);
    }

    @Override
    public PushSelectResponse pushSelect(PrimitiveBuilder pb, SQLSelectItem expr, Builder origin) throws SQLException {
        return this.getBldr().pushSelect(pb, expr, origin);
    }

    @Override
    public void makeDistinct() throws SQLException {
        this.getBldr().makeDistinct();
    }

    @Override
    public void pushGroupBy(SQLSelectGroupByClause groupBy) throws SQLException {
        this.getBldr().pushGroupBy(groupBy);
    }

    @Override
    public Builder pushOrderBy(SQLOrderBy orderBy) throws SQLException {
        throw new SQLException("can't do ORDER BY on top of ORDER BY");
    }

    @Override
    public void wireup(Builder bldr, Jointab jt) throws SQLException {
        // If the route has to do the ordering, and if any columns are Text,
        // we have to request the corresponding weight_string from mysql
        // and use that value instead. This is because we cannot mimic
        // mysql's collation behavior yet.
        RoutePlan rb = (RoutePlan) this.getBldr();
        List<OrderByParams> innerOrderBy;
        if (rb instanceof TableRoutePlan) {
            innerOrderBy = ((TableRoutePlan) rb).getTableRouteEngine().getOrderBy();
            ((TableRoutePlan) rb).getTableRouteEngine().setTruncateColumnCount(this.truncateColumnCount);
        } else {
            innerOrderBy = rb.getRouteEngine().getOrderBy();
            rb.getRouteEngine().setTruncateColumnCount(this.truncateColumnCount);
        }

        for (int i = 0; i < innerOrderBy.size(); i++) {
            OrderByParams orderBy = innerOrderBy.get(i);
            ResultColumn rc = this.getResultColumnList().get(orderBy.getCol());
            if (VtType.isText(rc.getColumn().getType())) {
                // If a weight string was previously requested, reuse it.
                if (this.getWeightStrings().containsKey(rc)) {
                    Integer colNumber = this.getWeightStrings().get(rc);
                    innerOrderBy.get(i).setCol(colNumber);
                    continue;
                }
                innerOrderBy.get(i).setCol(rb.supplyWeightString(orderBy.getCol()));
                this.setTruncateColumnCount(this.getResultColumnList().size());
            }
        }

        if (rb instanceof TableRoutePlan) {
            ((TableRoutePlan) rb).getTableRouteEngine().setTruncateColumnCount(this.truncateColumnCount);
        } else {
            rb.getRouteEngine().setTruncateColumnCount(this.truncateColumnCount);
        }

        this.getBldr().wireup(bldr, jt);
    }

    /**
     * SetTruncateColumnCount satisfies the truncater interface.
     * This function records the truncate column count and sets
     * it later on the eroute during wire-up phase.
     *
     * @param count
     */
    @Override
    public void setTruncateColumnCount(Integer count) {
        this.truncateColumnCount = count;
    }
}
