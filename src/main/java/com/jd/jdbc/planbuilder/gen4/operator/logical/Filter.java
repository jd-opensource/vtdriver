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

import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import java.sql.SQLException;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Filter implements LogicalOperator {

    private LogicalOperator source;

    private List<SQLExpr> predicates;

    public Filter() {

    }

    public Filter(LogicalOperator source, List<SQLExpr> predicates) {
        this.source = source;
        this.predicates = predicates;
    }

    @Override
    public TableSet tableID() {
        return this.source.tableID();
    }

    @Override
    public SQLSelectItem unsolvedPredicates(SemTable semTable) {
        return this.source.unsolvedPredicates(semTable);
    }

    @Override
    public void checkValid() throws SQLException {
        this.source.checkValid();
    }

    @Override
    public LogicalOperator pushPredicate(SQLExpr expr, SemTable semTable) throws SQLException {
        LogicalOperator op = this.source.pushPredicate(expr, semTable);

        if (op instanceof Filter) {
            ((Filter) op).predicates.addAll(this.predicates);
            return op;
        }
        return new Filter(op, this.predicates);
    }

    @Override
    public LogicalOperator compact(SemTable semTable) throws SQLException {
        if (this.predicates.size() == 0) {
            return this.source;
        }
        return this;
    }
}
