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
import com.jd.jdbc.sqlparser.ast.SQLLimit;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

/**
 * Concatenate represents a UNION ALL/DISTINCT.
 */
public class Concatenate implements LogicalOperator {

    @Getter
    private boolean distinct;

    @Getter
    private List<SQLSelectQuery> selectStmts;

    @Getter
    private List<LogicalOperator> sources;

    @Getter
    private SQLOrderBy orderBy;

    @Getter
    private SQLLimit limit;

    public Concatenate(boolean distinct, List<SQLSelectQuery> selectStmts, List<LogicalOperator> sources, SQLOrderBy orderBy, SQLLimit limit) {
        this.distinct = distinct;
        this.selectStmts = selectStmts;
        this.sources = sources;
        this.orderBy = orderBy;
        this.limit = limit;
    }

    @Override
    public TableSet tableID() {
//        TableSet tableSet
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
    public LogicalOperator pushPredicate(SQLExpr expr, SemTable semTable) throws SQLException {
        return null;
    }

    @Override
    public LogicalOperator compact(SemTable semTable) throws SQLException {
        List<LogicalOperator> newSources = new ArrayList<>();
        List<SQLSelectQuery> newSels = new ArrayList<>();

        for (int i = 0; i < this.sources.size(); i++) {
            LogicalOperator source = this.sources.get(i);
            if (!(source instanceof Concatenate)) {
                newSources.add(source);
                newSels.add(this.selectStmts.get(i));
                continue;
            }
            Concatenate other = (Concatenate) source;
            if (other.limit == null && other.orderBy == null && !other.distinct || this.distinct && other.limit == null) {
                // if the current UNION is a DISTINCT, we can safely ignore everything from children UNIONs, except LIMIT
                newSources.addAll(other.sources);
                newSels.addAll(other.selectStmts);
            } else {
                newSources.add(other);
                newSels.add(null);
            }
        }
        this.sources = newSources;
        this.selectStmts = newSels;
        return this;
    }
}
