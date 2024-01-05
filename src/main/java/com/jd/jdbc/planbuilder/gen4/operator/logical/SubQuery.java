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
// SubQuery stores the information about subquery
public class SubQuery implements LogicalOperator {
    private List<SubQueryInner> inner;

    private LogicalOperator outer;

    @Override
    public LogicalOperator pushPredicate(SQLExpr expr, SemTable semTable) throws SQLException {
        return null;
    }

    @Override
    public LogicalOperator compact(SemTable semTable) throws SQLException {
        return null;
    }

    @Override
    public TableSet tableID() {
        return null;
    }

    @Override
    public SQLSelectItem unsolvedPredicates(SemTable semTable) {
        return null;
    }

    @Override
    public void checkValid() throws SQLException {

    }

// SubQueryInner stores the subquery information for a select statement

    private static class SubQueryInner {
        // Inner is the Operator inside the parenthesis of the subquery.
        // i.e: select (select 1 union select 1), the Inner here would be
        // of type Concatenate since we have a Union.
        private LogicalOperator inner;

        // ExtractedSubquery contains all information we need about this subquery
        private Object ExtractedSubquery;
    }
}


