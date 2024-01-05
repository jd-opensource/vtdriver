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

package com.jd.jdbc.planbuilder.gen4.operator.logical;

import com.jd.jdbc.context.PlanningContext;
import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import java.sql.SQLException;
import lombok.Getter;

/**
 * Derived represents a derived table in the query
 */
@Getter
public class Derived implements LogicalOperator {
    private SQLSelectStatement sel;

    private LogicalOperator inner;

    private String alias;

    private String ColumnAliases;

    @Override
    public TableSet tableID() {
        return this.inner.tableID();
    }

    @Override
    public SQLSelectItem unsolvedPredicates(SemTable semTable) {
        return this.inner.unsolvedPredicates(semTable);
    }

    @Override
    public void checkValid() throws SQLException {
        this.inner.checkValid();
    }

    @Override
    public LogicalOperator pushPredicate(SQLExpr expr, SemTable semTable) throws SQLException {
        //todo
        return null;
    }

    @Override
    public LogicalOperator compact(SemTable semTable) throws SQLException {
        return this;
    }

    /**
     * IsMergeable is not a great name for this function. Suggestions for a better one are welcome!
     * This function will return false if the derived table inside it has to run on the vtgate side, and so can't be merged with subqueries
     * This logic can also be used to check if this is a derived table that can be had on the left hand side of a vtgate join.
     * Since vtgate joins are always nested loop joins, we can't execute them on the RHS
     * if they do some things, like LIMIT or GROUP BY on wrong columns
     *
     * @param ctx
     * @return
     */
    public Boolean isMergeable(PlanningContext ctx) {
        // TODO
        return false;
    }
}
