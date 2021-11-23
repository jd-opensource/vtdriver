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
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

public interface Builder {

    /**
     * Order is the execution order of the primitive. If there are subprimitives,
     * the order is one above the order of the subprimitives.
     * This is because the primitive executes its subprimitives first and
     * processes their results to generate its own values.
     * Please copy code from an existing primitive to define this function.
     *
     * @return
     */
    Integer order();

    /**
     * ResultColumns returns the list of result columns the
     * primitive returns.
     * Please copy code from an existing primitive to define this function.
     *
     * @return
     */
    List<ResultColumn> resultColumns();

    /**
     * Reorder reassigns order for the primitive and its sub-primitives.
     * The input is the order of the previous primitive that should
     * execute before this one.
     *
     * @param order
     */
    void reorder(Integer order);

    /**
     * First returns the first builder of the tree,
     * which is usually the left most.
     *
     * @return
     * @throws SQLException
     */
    Builder first() throws SQLException;

    /**
     * pushes a WHERE or HAVING clause expression
     * to the specified origin.
     *
     * @param pb
     * @param filter
     * @param whereType
     * @param origin
     * @throws SQLException
     */
    default void pushFilter(PrimitiveBuilder pb, SQLExpr filter, String whereType, Builder origin) throws SQLException {

    }

    /**
     * PushSelect pushes the select expression to the specified
     * originator. If successful, the originator must create
     * a resultColumn entry and return it. The top level caller
     * must accumulate these result columns and set the symtab
     * after analysis.
     *
     * @param pb
     * @param expr
     * @param origin
     * @return
     * @throws SQLException
     */
    default PushSelectResponse pushSelect(PrimitiveBuilder pb, SQLSelectItem expr, Builder origin) throws SQLException {
        return null;
    }

    /**
     * MakeDistinct makes the primitive handle the distinct clause.
     *
     * @throws SQLException
     */
    void makeDistinct() throws SQLException;

    /**
     * PushGroupBy makes the primitive handle the GROUP BY clause.
     *
     * @param groupBy
     * @throws SQLException
     */
    default void pushGroupBy(SQLSelectGroupByClause groupBy) throws SQLException {
    }

    /**
     * PushOrderBy pushes the ORDER BY clause. It returns the
     * the current primitive or a replacement if a new one was
     *
     * @param orderBy
     * @return
     * @throws SQLException
     */
    default Builder pushOrderBy(SQLOrderBy orderBy) throws SQLException {
        return null;
    }

    /**
     * SetUpperLimit is an optimization hint that tells that primitive
     * that it does not need to return more than the specified number of rows.
     * A primitive that cannot perform this can ignore the request.
     *
     * @param count
     * @throws SQLException
     */
    void setUpperLimit(SQLExpr count) throws SQLException;

    /**
     * PushMisc pushes miscelleaneous constructs to all the primitives.
     *
     * @param sel
     */
    void pushMisc(MySqlSelectQueryBlock sel);


    /**
     * Wireup performs the wire-up work. Nodes should be traversed
     * from right to left because the rhs nodes can request vars from
     * the lhs nodes.
     *
     * @param bldr
     * @param jt
     * @throws SQLException
     */
    void wireup(Builder bldr, Jointab jt) throws SQLException;

    /**
     * SupplyVar finds the common root between from and to. If it's
     * the common root, it supplies the requested var to the rhs tree.
     * If the primitive already has the column in its list, it should
     * just supply it to the 'to' node. Otherwise, it should request
     * for it by calling SupplyCol on the 'from' sub-tree to request the
     * column, and then supply it to the 'to' node.
     *
     * @param from
     * @param to
     * @param colName
     * @param varName
     * @throws SQLException
     */
    void supplyVar(Integer from, Integer to, SQLName colName, String varName) throws SQLException;

    /**
     * SupplyCol is meant to be used for the wire-up process. This function
     * changes the primitive to supply the requested column and returns
     * the resultColumn and column number of the result. SupplyCol
     * is different from PushSelect because it may reuse an existing
     * resultColumn, whereas PushSelect guarantees the addition of a new
     * result column and returns a distinct symbol for it.
     *
     * @param col
     * @return
     * @throws SQLException
     */
    SupplyColResponse supplyCol(SQLName col) throws SQLException;

    /**
     * SupplyWeightString must supply a weight_string expression of the specified column.
     *
     * @param colNumber
     * @return
     * @throws SQLException
     */
    Integer supplyWeightString(Integer colNumber) throws SQLException;

    /**
     * PushLock pushes "FOR UPDATE", "LOCK IN SHARE MODE" down to all routes.
     *
     * @param lock
     * @throws SQLException
     */
    void pushLock(String lock) throws SQLException;

    /**
     * Primitive returns the underlying primitive.
     * This function should only be called after Wireup is finished.
     *
     * @return {@link PrimitiveEngine}
     */
    PrimitiveEngine getPrimitiveEngine() throws SQLException;

    boolean isSplitTablePlan();

    @Getter
    @Setter
    @AllArgsConstructor
    class PushSelectResponse {
        private ResultColumn resultColumn;

        private Integer colNumber;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    class SupplyColResponse {
        private ResultColumn rc;

        private Integer colNumber;
    }
}
