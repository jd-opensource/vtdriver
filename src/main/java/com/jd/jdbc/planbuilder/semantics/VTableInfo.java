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

import com.jd.jdbc.common.tuple.Triple;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import java.sql.SQLException;
import java.util.List;
import lombok.Getter;
import vschema.Vschema;

/**
 * vTableInfo is used to represent projected results, not real tables. It is used for
 * ORDER BY, GROUP BY and HAVING that need to access result columns
 */
public class VTableInfo implements TableInfo {

    private String tableName;

    private List<String> columnNames;

    @Getter
    private List<SQLExpr> cols;

    private TableSet tables;

    public VTableInfo(List<String> columnNames, List<SQLExpr> cols, TableSet tables) {
        this.columnNames = columnNames;
        this.cols = cols;
        this.tables = tables;
    }

    @Override
    public SQLExprTableSource name() throws SQLException {
        throw new SQLException("oh noes");
    }

    @Override
    public Vschema.Table getVindexTable() {
        return null;
    }

    @Override
    public boolean isInfSchema() {
        return false;
    }

    @Override
    public boolean matches(SQLObject name) {
        return false;
    }

    @Override
    public boolean authoritative() {
        return false;
    }

    @Override
    public SQLTableSource getExpr() {
        return null;
    }

    @Override
    public List<ColumnInfo> getColumns() {
        return null;
    }

    /**
     * dependencies implements the TableInfo interface
     *
     * @param colName
     * @param org
     * @return
     */
    @Override
    public Dependencies dependencies(String colName, Originable org) {
        Dependencies deps = new Nothing();
        for (int i = 0; i < this.columnNames.size(); i++) {
            String name = this.columnNames.get(i);
            if (!SQLUtils.nameEquals(colName, name)) {
                continue;
            }
            Triple<TableSet, TableSet, Type> triple = org.depsForExpr(this.cols.get(i));

            Certain newDeps = Dependencies.createCertain(triple.getLeft(), triple.getMiddle(), triple.getRight());
            deps = deps.merge(newDeps, false);
        }
        if (deps.empty() && this.hasStar()) {
            return Dependencies.createUncertain(this.tables, this.tables);
        }
        return deps;
    }

    @Override
    public TableSet getTableSet(Originable org) {
        return null;
    }

    private boolean hasStar() {
        return this.tables.numberOfTables() > 0;
    }
}
