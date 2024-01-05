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

import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import java.sql.SQLException;
import java.util.List;
import lombok.Getter;
import vschema.Vschema;

/**
 * VindexTable contains a vindexes.Vindex and a TableInfo. The former represents the vindex
 * we are keeping information about, and the latter represents the additional table information
 * (usually a RealTable or an AliasedTable) of our vindex.
 */

@Getter
public class VindexTable implements TableInfo {
    private TableInfo table;

    private Vschema.ColumnVindex vindex;

    public VindexTable(TableInfo table, Vschema.ColumnVindex vindex) {
        this.table = table;
        this.vindex = vindex;
    }

    @Override
    public SQLExprTableSource name() throws SQLException {
        return table.name();
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

    @Override
    public Dependencies dependencies(String colNames, Originable org) throws SQLException {
        return null;
    }

    @Override
    public TableSet getTableSet(Originable org) {
        return null;
    }
}
