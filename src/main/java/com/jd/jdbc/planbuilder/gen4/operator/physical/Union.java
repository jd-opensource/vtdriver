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
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import java.sql.SQLException;
import java.util.List;
import lombok.Getter;

public class Union implements PhysicalOperator {

    @Getter
    private List<PhysicalOperator> sources;

    @Getter
    private List<SQLSelectQuery> selectStmts;

    @Getter
    private boolean distinct;

    // TODO this should be removed. For now it's used to fail queries
    @Getter
    private SQLOrderBy orderBy;

    public Union(List<PhysicalOperator> sources, List<SQLSelectQuery> selectStmts, boolean distinct, SQLOrderBy orderBy) {
        this.sources = sources;
        this.selectStmts = selectStmts;
        this.distinct = distinct;
        this.orderBy = orderBy;
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

    @Override
    public Integer cost() {
        return null;
    }

    @Override
    public PhysicalOperator clone() {
        return null;
    }
}
