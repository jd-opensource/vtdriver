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

import com.jd.jdbc.engine.MemorySortEngine;
import com.jd.jdbc.engine.OrderByParams;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.sqlparser.SqlParser;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.SQLOrderingSpecification;
import com.jd.jdbc.sqlparser.ast.expr.SQLLiteralExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectOrderByItem;
import com.jd.jdbc.sqltypes.VtType;
import java.sql.SQLException;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/**
 * memorySort is the builder for engine.Limit.
 * This gets built if a limit needs to be applied
 * after rows are returned from an underlying
 * operation. Since a limit is the final operation
 * of a SELECT, most pushes are not applicable.
 */
@Getter
@Setter
public class MemorySortPlan extends ResultsBuilder implements Builder {

    private MemorySortEngine memorySortEngine;

    private MemorySortPlan(Builder bldr, Truncater truncater) {
        super(bldr, truncater);
        this.memorySortEngine = (MemorySortEngine) truncater;
    }

    /**
     * newMemorySort builds a new memorySort.
     *
     * @param bldr
     * @param orderBy
     * @return
     * @throws SQLException
     */
    public static MemorySortPlan newMemorySortPlan(Builder bldr, SQLOrderBy orderBy) throws SQLException {
        MemorySortEngine memorySortEngine = new MemorySortEngine();
        MemorySortPlan ms = new MemorySortPlan(bldr, memorySortEngine);
        if (orderBy != null && orderBy.getItems() != null) {
            for (SQLSelectOrderByItem orderByItem : orderBy.getItems()) {
                SQLExpr expr = orderByItem.getExpr();
                int colNumber = -1;
                if (expr instanceof SQLLiteralExpr) {
                    colNumber = PlanBuilder.resultFromNumber(ms.getResultColumnList(), (SQLLiteralExpr) expr);
                } else if (expr instanceof SQLName) {
                    Column c = ((SQLName) expr).getMetadata();
                    for (int i = 0; i < ms.getResultColumnList().size(); i++) {
                        ResultColumn rc = ms.getResultColumnList().get(i);
                        if (rc.getColumn() == c) {
                            colNumber = i;
                            break;
                        }
                    }
                } else {
                    throw new SQLException("unsupported: memory sort: complex order by expression: " + expr.toString());
                }
                // If column is not found, then the order by is referencing
                // a column that's not on the select list.
                if (colNumber == -1) {
                    throw new SQLException("unsupported: memory sort: order by must reference a column in the select list: " + orderByItem);
                }
                OrderByParams ob = new OrderByParams(colNumber, orderByItem.getType() == SQLOrderingSpecification.DESC);
                ms.memorySortEngine.getOrderByParams().add(ob);
            }
        }
        return ms;
    }

    /**
     * SetUpperLimit satisfies the builder interface.
     * This is a no-op because we actually call SetLimit for this primitive.
     * In the future, we may have to honor this call for subqueries.
     *
     * @param count
     */
    @Override
    public void setUpperLimit(SQLExpr count) throws SQLException {
        this.memorySortEngine.setUpperLimit(SqlParser.newPlanValue(count));
    }

    @Override
    public void wireup(Builder bldr, Jointab jt) throws SQLException {
        for (int i = 0; i < this.memorySortEngine.getOrderByParams().size(); i++) {
            OrderByParams orderBy = this.memorySortEngine.getOrderByParams().get(i);
            ResultColumn rc = this.getResultColumnList().get(orderBy.getCol());
            if (VtType.isText(rc.getColumn().getType())) {
                // If a weight string was previously requested, reuse it.
                Map<ResultColumn, Integer> weightStrings = this.getWeightStrings();
                Integer weightColNumber;
                if (weightStrings.containsKey(rc)) {
                    weightColNumber = weightStrings.get(rc);
                    this.memorySortEngine.getOrderByParams().get(i).setCol(weightColNumber);
                    continue;
                }
                weightColNumber = this.getBldr().supplyWeightString(orderBy.getCol());
                this.getWeightStrings().put(rc, weightColNumber);
                this.memorySortEngine.getOrderByParams().get(i).setCol(weightColNumber);
                this.memorySortEngine.setTruncateColumnCount(this.getResultColumnList().size());
            }
        }
        this.getBldr().wireup(bldr, jt);
    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        this.memorySortEngine.setInput(this.getBldr().getPrimitiveEngine());
        return this.memorySortEngine;
    }
}
