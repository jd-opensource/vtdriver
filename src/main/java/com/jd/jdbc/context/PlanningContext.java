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

package com.jd.jdbc.context;

import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.planbuilder.semantics.VSchema;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.vindexes.VKeyspace;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;

@Getter
public class PlanningContext {

    private Object reservedVars;

    private SemTable semTable;

    private VKeyspace keyspace;

    private VSchema vschema;

    // here we add all predicates that were created because of a join condition
    // e.g. [FROM tblA JOIN tblB ON a.colA = b.colB] will be rewritten to [FROM tblB WHERE :a_colA = b.colB],
    // if we assume that tblB is on the RHS of the join. This last predicate in the WHERE clause is added to the
    // map below

    private Map<SQLExpr, List<SQLExpr>> joinPredicates;

    private Map<SQLExpr, Object> skipPredicates;

    private boolean rewriteDerivedExpr;

    public PlanningContext(Object reservedVars, SemTable semTable, VSchema vschema, VKeyspace keyspace) {
        this.reservedVars = reservedVars;
        this.semTable = semTable;
        this.vschema = vschema;
        this.keyspace = keyspace;
        this.skipPredicates = new HashMap<>();
        this.joinPredicates = new HashMap<>();
        this.rewriteDerivedExpr = false;
    }
}
