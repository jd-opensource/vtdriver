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

package com.jd.jdbc.planbuilder.semantics;

import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.visitor.ExprDependenciesVisitor;
import java.util.HashMap;
import java.util.Map;

/**
 * ExprDependencies stores the tables that an expression depends on as a map
 */
public class ExprDependencies {
    private Map<SQLObjectExpr, TableSet> tableSetMap;

    public ExprDependencies() {
        this.tableSetMap = new HashMap<>(16);
    }

    public void put(SQLObject sqlObject, TableSet tableSet) {
        tableSetMap.put(new SQLObjectExpr(sqlObject), tableSet);
    }

    public TableSet get(SQLObject sqlObject) {
        return tableSetMap.get(new SQLObjectExpr(sqlObject));
    }

    /**
     * dependencies return the table dependencies of the expression. This method finds table dependencies recursively
     *
     * @param expr
     * @return
     */
    public TableSet dependencies(SQLExpr expr) {
        //todo  BUG:tableSetMap.get可能取不到对应的key值,对应operator测试用例 # we should remove the keyspace from predicates
        if (Scoper.validAsMapKey(expr)) {
            // we have something that could live in the cache
            TableSet deps = this.get(expr);
            if (deps != null) {
                return deps;
            }
        }

        // During the original semantic analysis, all ColNames were found and bound to the corresponding tables
        // Here, we'll walk the expression tree and look to see if we can find any sub-expressions
        // that have already set dependencies.
        ExprDependenciesVisitor visitor = new ExprDependenciesVisitor(this, expr);
        expr.accept(visitor);
        TableSet deps = visitor.getDeps();
        return deps;
    }
}
