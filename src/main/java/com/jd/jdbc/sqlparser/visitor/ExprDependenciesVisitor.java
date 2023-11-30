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

package com.jd.jdbc.sqlparser.visitor;

import com.jd.jdbc.planbuilder.semantics.ExprDependencies;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import lombok.Getter;

public class ExprDependenciesVisitor extends MySqlASTVisitorAdapter {
    private ExprDependencies exprDependencies;

    @Getter
    private TableSet deps;

    private final SQLExpr expr;

    public ExprDependenciesVisitor(ExprDependencies exprDependencies, SQLExpr expr) {
        this.exprDependencies = exprDependencies;
        this.deps = new TableSet(0L, null);
        this.expr = expr;
    }

    @Override
    public boolean visit(SQLIdentifierExpr x) {
        TableSet set = exprDependencies.get(x);
        if (set != null) {
            deps.mergeInPlace(set);
            exprDependencies.put(expr, deps);
            return false;
        }
        return true;
    }

    @Override
    public boolean visit(SQLPropertyExpr x) {
        TableSet set = exprDependencies.get(x);
        if (set != null) {
            deps.mergeInPlace(set);
            exprDependencies.put(expr, deps);
            return false;
        }
        return true;
    }
}
