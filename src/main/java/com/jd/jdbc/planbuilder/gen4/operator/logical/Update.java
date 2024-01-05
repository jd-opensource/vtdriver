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

import com.jd.jdbc.planbuilder.gen4.QueryTable;
import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.planbuilder.semantics.TableInfo;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLUpdateStatement;
import java.sql.SQLException;
import java.util.Map;

public class Update implements LogicalOperator {
    private static QueryTable table;

    private static TableInfo tableInfo;

    private static Map<String, SQLExpr> assignments;

    private static SQLUpdateStatement ast;

    @Override
    public LogicalOperator pushPredicate(SQLExpr expr, SemTable semTable) throws SQLException {
        throw new SQLException("can't accept predicates");
    }

    @Override
    public LogicalOperator compact(SemTable semTable) throws SQLException {
        return this;
    }

    @Override
    public TableSet tableID() {
        return table.getId();
    }

    @Override
    public SQLSelectItem unsolvedPredicates(SemTable semTable) {
        return null;
    }

    @Override
    public void checkValid() throws SQLException {

    }
}
