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

package com.jd.jdbc.planbuilder.gen4.operator.physical;

import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * ApplyJoin is a nested loop join - for each row on the LHS,
 * we'll execute the plan on the RHS, feeding data from left to righ
 */

@Getter
@Setter
@AllArgsConstructor
public class ApplyJoin implements PhysicalOperator {

    private PhysicalOperator lHS, rHS;

    /**
     * Columns stores the column indexes of the columns coming from the left and right side
     * negative value comes from LHS and positive from RHS
     */
    private List<Integer> columns;

    /**
     * Vars are the arguments that need to be copied from the LHS to the RHS
     */
    private Map<String, Integer> vars;

    /**
     * LeftJoin will be true in the case of an outer join
     */
    private Boolean leftJoin;

    /**
     * JoinCols are the columns from the LHS used for the join.
     * These are the same columns pushed on the LHS that are now used in the Vars field
     */
    private List<SQLName> lhsColumns;

    private SQLExpr predicate;

    public ApplyJoin(PhysicalOperator lHS, PhysicalOperator rHS, Boolean leftJoin) {
        this.lHS = lHS;
        this.rHS = rHS;
        this.columns = new ArrayList<>();
        this.vars = new HashMap<>();
        this.leftJoin = leftJoin;
        this.lhsColumns = new ArrayList<>();
        this.predicate = null;
    }

    public ApplyJoin(PhysicalOperator lHS, PhysicalOperator rHS, Boolean leftJoin, SQLExpr predicate) {
        this.lHS = lHS;
        this.rHS = rHS;
        this.columns = new ArrayList<>();
        this.vars = new HashMap<>();
        this.leftJoin = leftJoin;
        this.lhsColumns = new ArrayList<>();
        this.predicate = predicate;
    }

    @Override
    public TableSet tableID() {
        return this.lHS.tableID().merge(this.rHS.tableID());
    }

    @Override
    public SQLSelectItem unsolvedPredicates(SemTable semTable) {
        return null;
    }

    @Override
    public void checkValid() throws SQLException {
        this.lHS.checkValid();
        this.rHS.checkValid();
    }

    @Override
    public Integer cost() {
        return this.lHS.cost() + this.rHS.cost();
    }

    @Override
    public PhysicalOperator clone() {
        // vars clone
        Map<String, Integer> varsClone = new HashMap<>();
        for (Map.Entry<String, Integer> entry : this.vars.entrySet()) {
            varsClone.put(entry.getKey(), entry.getValue());
        }

        // columns clone
        List<Integer> columnsClone = new ArrayList<>(this.columns.size());
        for (Integer col : this.columns) {
            columnsClone.add(col);
        }
        // lhs columns clone
        List<SQLName> lhsColNameClone = new ArrayList<>(this.lhsColumns.size());
        for (SQLName colName : this.lhsColumns) {
            lhsColNameClone.add(colName.clone());
        }

        return new ApplyJoin(
            this.lHS.clone(),
            this.rHS.clone(),
            columnsClone,
            varsClone,
            this.leftJoin,
            lhsColNameClone,
            this.predicate != null ? this.predicate.clone() : null
        );
    }
}
