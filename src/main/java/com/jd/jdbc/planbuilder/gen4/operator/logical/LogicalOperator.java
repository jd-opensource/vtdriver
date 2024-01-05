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

import com.jd.jdbc.planbuilder.gen4.operator.Operator;
import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import java.sql.SQLException;

public interface LogicalOperator extends Operator {
    default void isLogical() {

    }

    /**
     * PushPredicate pushes a predicate to the closest possible operator
     *
     * @param expr
     * @param semTable
     * @return
     */
    LogicalOperator pushPredicate(SQLExpr expr, SemTable semTable) throws SQLException;

    /**
     * Compact will optimise the operator tree into a smaller but equivalent version
     *
     * @param semTable
     * @return
     * @throws SQLException
     */
    LogicalOperator compact(SemTable semTable) throws SQLException;
}
