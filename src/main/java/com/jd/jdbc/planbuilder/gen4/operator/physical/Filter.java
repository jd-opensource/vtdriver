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
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Filter implements PhysicalOperator {

    private PhysicalOperator source;

    private List<SQLExpr> predicates;

    public Filter() {

    }

    public Filter(PhysicalOperator source, List<SQLExpr> predicates) {
        this.source = source;
        this.predicates = predicates;
    }

    @Override
    public TableSet tableID() {
        return this.source.tableID();
    }

    @Override
    public SQLSelectItem unsolvedPredicates(SemTable semTable) {
        return null;
    }

    @Override
    public void checkValid() throws SQLException {

    }

    @Override
    public Integer cost() {
        return this.getSource().cost();
    }

    @Override
    public PhysicalOperator clone() {

        List<SQLExpr> predicatesClone = new ArrayList<>(this.predicates.size());
        for (SQLExpr pred : this.predicates) {
            predicatesClone.add(pred.clone());
        }
        return new Filter(source.clone(), predicatesClone);
    }
}
