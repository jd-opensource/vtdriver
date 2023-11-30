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

package com.jd.jdbc.planbuilder.gen4.operator;

import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import java.sql.SQLException;

/**
 * Operator forms the tree of operators, representing the declarative query provided.
 */
public interface Operator {
    /**
     * TableID returns a TableSet of the tables contained within
     *
     * @return
     */
    TableSet tableID();

    /**
     * UnsolvedPredicates returns any predicates that have dependencies on the given Operator and
     * on the outside of it (a parent Select expression, any other table not used by Operator, etc).
     *
     * @param semTable
     * @return
     */
    SQLSelectItem unsolvedPredicates(SemTable semTable);

    /**
     * CheckValid checks if we have a valid operator tree, and returns an error if something is wrong
     *
     * @throws SQLException
     */
    void checkValid() throws SQLException;
}
