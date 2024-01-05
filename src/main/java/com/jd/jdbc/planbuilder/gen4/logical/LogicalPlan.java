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

package com.jd.jdbc.planbuilder.gen4.logical;

import com.jd.jdbc.common.tuple.Pair;
import com.jd.jdbc.context.PlanningContext;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.planbuilder.Builder;
import com.jd.jdbc.planbuilder.Jointab;
import com.jd.jdbc.planbuilder.ResultColumn;
import com.jd.jdbc.planbuilder.gen4.PlanVisitor;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import java.sql.SQLException;
import java.util.List;

/**
 * logicalPlan defines the interface that a primitive must satisfy.
 */
public interface LogicalPlan {
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
     * WireupGen4 does the wire up work for the Gen4 planner
     *
     * @param ctx
     * @throws SQLException
     */
    void wireupGen4(PlanningContext ctx) throws SQLException;

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
    Builder.SupplyColResponse supplyCol(SQLName col) throws SQLException;

    /**
     * SupplyWeightString must supply a weight_string expression of the
     * specified column. It returns an error if we cannot supply a weight column for it.
     *
     * @param colNumber
     * @return
     * @throws SQLException
     */
    Integer supplyWeightString(Integer colNumber) throws SQLException;

    /**
     * Primitive returns the underlying primitive.
     * This function should only be called after Wireup is finished.
     *
     * @return {@link PrimitiveEngine}
     */
    PrimitiveEngine getPrimitiveEngine() throws SQLException;

    /**
     * Inputs are the children of this plan
     *
     * @return
     * @throws SQLException
     */
    LogicalPlan[] inputs() throws SQLException;

    /**
     * Rewrite replaces the inputs of this plan with the ones provided
     *
     * @return
     * @throws SQLException
     */
    LogicalPlan[] rewrite(LogicalPlan... inputs) throws SQLException;

    /**
     * ContainsTables keeps track which query tables are being solved by this logical plan
     * This is only applicable for plans that have been built with the Gen4 planner
     *
     * @return
     */
    TableSet containsTables();

    /**
     * OutputColumns shows the columns that this plan will produce
     *
     * @return
     */
    List<SQLSelectItem> outputColumns() throws SQLException;

    /**
     * SetUpperLimit is an optimization hint that tells that primitive
     * that it does not need to return more than the specified number of rows.
     * A primitive that cannot perform this can ignore the request.
     *
     * @param count
     * @throws SQLException
     */
    void setUpperLimit(SQLExpr count) throws SQLException;

    static LogicalPlan visit(LogicalPlan node, PlanVisitor visitor) throws SQLException {
        if (visitor != null) {
            Pair<Boolean, LogicalPlan> pair = visitor.func(node);
            Boolean kontinue = pair.getLeft();
            LogicalPlan newNode = pair.getRight();
            if (!kontinue) {
                return newNode;
            }
        }
        LogicalPlan[] inputs = node.inputs();
        boolean rewrite = false;
        for (int i = 0; i < inputs.length; i++) {
            LogicalPlan input = inputs[i];
            LogicalPlan newInput = visit(input, visitor);
            if (newInput != input) {
                rewrite = true;
            }
            inputs[i] = newInput;
        }
        if (rewrite) {
            node.rewrite(inputs);
        }
        return node;
    }
}
