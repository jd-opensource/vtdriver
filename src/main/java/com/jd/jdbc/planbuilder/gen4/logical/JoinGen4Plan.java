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

import com.jd.jdbc.context.PlanningContext;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.gen4.JoinGen4Engine;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/**
 * joinGen4 is used to build a Join primitive.
 * It's used to build an inner join and only used by the Gen4 planner
 */
public class JoinGen4Plan extends AbstractGen4Plan {

    /**
     * Left and Right are the nodes for the join.
     */
    @Getter
    @Setter
    LogicalPlan left;

    /**
     * Left and Right are the nodes for the join.
     */
    @Getter
    @Setter
    LogicalPlan right;

    /**
     * The Opcode tells us if this is an inner or outer join
     */
    @Getter
    Engine.JoinOpcode opcode;

    /**
     * These are the columns that will be produced by this plan.
     * Negative offsets come from the LHS, and positive from the RHS
     */

    @Getter
    List<Integer> cols;

    /**
     * Vars are the columns that will be sent from the LHS to the RHS
     * the number is the offset on the LHS result, and the string is the bind variable name used in the RHS
     */
    @Getter
    Map<String, Integer> vars;

    /**
     * LHSColumns are the columns from the LHS used for the join.
     * These are the same columns pushed on the LHS that are now used in the Vars field
     */

    @Getter
    List<SQLName> lHSColumns;


    public JoinGen4Plan(LogicalPlan left, LogicalPlan right, Engine.JoinOpcode opcode, List<Integer> cols, Map<String, Integer> vars, List<SQLName> lHSColumns) {
        this.left = left;
        this.right = right;
        this.opcode = opcode;
        this.cols = cols;
        this.vars = vars;
        this.lHSColumns = lHSColumns;
    }

    @Override
    public void wireupGen4(PlanningContext ctx) throws SQLException {
        this.left.wireupGen4(ctx);
        this.right.wireupGen4(ctx);
    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        JoinGen4Engine engine = new JoinGen4Engine(this.opcode, this.vars);
        engine.setLeft(this.left.getPrimitiveEngine());
        engine.setRight(this.right.getPrimitiveEngine());
        engine.setCols(this.cols);
        return engine;
    }

    @Override
    public LogicalPlan[] inputs() throws SQLException {
        return new LogicalPlan[] {this.left, this.right};
    }

    @Override
    public LogicalPlan[] rewrite(LogicalPlan... inputs) throws SQLException {
        if (inputs.length != 2) {
            throw new SQLException("[JoinGen4]wrong number of children");
        }
        this.left = inputs[0];
        this.right = inputs[1];
        return null;
    }

    @Override
    public TableSet containsTables() {
        return this.left.containsTables().merge(this.right.containsTables());
    }

    @Override
    public List<SQLSelectItem> outputColumns() throws SQLException {
        return JoinGen4Plan.getOutputColumnsFromJoin(this.cols, this.left.outputColumns(), this.right.outputColumns());
    }

    public static List<SQLSelectItem> getOutputColumnsFromJoin(List<Integer> ints, List<SQLSelectItem> lhs, List<SQLSelectItem> rhs) {
        List<SQLSelectItem> cols = new ArrayList<>(ints.size());
        for (Integer col : ints) {
            if (col < 0) {
                col *= -1;
                cols.add(lhs.get(col - 1));
            } else {
                cols.add(rhs.get(col - 1));
            }
        }
        return cols;
    }
}
