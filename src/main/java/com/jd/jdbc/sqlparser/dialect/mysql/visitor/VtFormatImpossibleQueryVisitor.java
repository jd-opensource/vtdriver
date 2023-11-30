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

package com.jd.jdbc.sqlparser.dialect.mysql.visitor;

import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import static com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator.NotEqual;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntegerExpr;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

/**
* FormatImpossibleQuery creates an impossible query in a TrackedBuffer.
* An impossible query is a modified version of a query where all selects have where clauses that are
* impossible for mysql to resolve. This is used in the vtgate and vttablet:
*
* - In the vtgate it's used for joins: if the first query returns no result, then vtgate uses the impossible
* query just to fetch field info from vttablet
* - In the vttablet, it's just an optimization: the field info is fetched once form MySQL, cached and reused
* for subsequent queries
 */
public class VtFormatImpossibleQueryVisitor extends MySqlASTVisitorAdapter {

    public VtFormatImpossibleQueryVisitor() {
    }

    @Override
    public boolean visit(final MySqlSelectQueryBlock x) {
        // where 1 != 1
        x.setWhere(new SQLBinaryOpExpr(new SQLIntegerExpr(1),
                NotEqual,
                new SQLIntegerExpr(1)));
        // remove limit
        x.setLimit(null);

        // remove distinct
        x.setDistionOption(0);

        // remove order by
        x.setOrderBy(null);

        // remove hint
        x.setHints(null);

        // remove having
        if (x.getGroupBy() != null) {
            x.getGroupBy().setHaving(null);
        }

        return true;
    }
}
