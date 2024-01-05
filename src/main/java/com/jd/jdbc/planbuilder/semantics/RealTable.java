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

import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import com.jd.jdbc.sqlparser.utils.TableNameUtils;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Setter;
import vschema.Vschema;

/**
 * RealTable contains the alias table expr and vindex table
 */
@Setter
public class RealTable implements TableInfo {
    @Setter
    private String dbName;

    @Setter
    private String tableName;

    private SQLTableSource astnode;

    private Vschema.Table table;

    private boolean isInfSchema;

    public RealTable(String tableName, SQLTableSource astnode, Vschema.Table table, boolean isInfSchema) {
        this.tableName = tableName;
        this.astnode = astnode;
        this.table = table;
        this.isInfSchema = isInfSchema;
    }

    /**
     * GetColumns implements the TableInfo interface
     *
     * @return
     */
    @Override
    public List<ColumnInfo> getColumns() {
        return vindexTableToColumnInfo(this.table);
    }

    @Override
    public SQLExprTableSource name() throws SQLException {
        if (astnode instanceof SQLExprTableSource) {
            return (SQLExprTableSource) astnode;
        }
        throw new SQLException();
    }

    @Override
    public Vschema.Table getVindexTable() {
        return this.table;
    }

    @Override
    public boolean isInfSchema() {
        return isInfSchema;
    }

    /**
     * Matches implements the TableInfo interface
     *
     * @param expr
     * @return
     */
    @Override
    public boolean matches(SQLObject expr) throws SQLException {
        if (expr instanceof SQLPropertyExpr) {
            boolean flag;
            String tableName;
            String databaseName;
            SQLPropertyExpr leftExpr = (SQLPropertyExpr) expr;
            SQLExpr owner = leftExpr.getOwner();
            if (owner instanceof SQLPropertyExpr) {
                tableName = ((SQLPropertyExpr) owner).getName();
                databaseName = ((SQLPropertyExpr) owner).getOwnernName();
                flag = SQLUtils.nameEquals(databaseName, this.dbName);
                String qualifier = TableNameUtils.getQualifier(astnode);
                flag = flag && (SQLUtils.nameEquals(tableName, qualifier) || SQLUtils.nameEquals(tableName, this.tableName));
                return flag;
            } else {
                tableName = leftExpr.getOwnernName();
                String qualifier = TableNameUtils.getQualifier(astnode);
                flag = SQLUtils.nameEquals(tableName, qualifier) || SQLUtils.nameEquals(tableName, this.tableName);
                return flag;
            }
        } else {
            return false;
        }
    }

    @Override
    public boolean authoritative() {
        return this.table != null && this.table.getColumnListAuthoritative();
    }

    @Override
    public SQLTableSource getExpr() {
        return this.astnode;
    }

    /**
     * dependencies implements the TableInfo interface
     *
     * @param colName
     * @param org
     * @return
     */
    @Override
    public Dependencies dependencies(String colName, Originable org) throws SQLException {
        TableSet ts = org.tableSetFor(this.astnode);

        for (ColumnInfo info : this.getColumns()) {
            if (SQLUtils.nameEquals(info.getName(), colName)) {
                return Dependencies.createCertain(ts, ts, info.getType());
            }
        }
        if (this.authoritative()) {
            return new Nothing();
        }
        return Dependencies.createUncertain(ts, ts);
    }

    @Override
    public TableSet getTableSet(Originable org) throws SQLException {
        return org.tableSetFor(this.astnode);
    }

    private List<ColumnInfo> vindexTableToColumnInfo(Vschema.Table tbl) {
        if (tbl == null) {
            return Collections.EMPTY_LIST;
        }
        Set<String> nameMap = new HashSet<>(16);
        List<ColumnInfo> cols = new ArrayList<>(tbl.getColumnsList().size() + tbl.getColumnVindexesList().size());

        for (Vschema.Column col : tbl.getColumnsList()) {
            ColumnInfo columnInfo = new ColumnInfo(col.getName(), new Type(col.getType()));
            cols.add(columnInfo);
            nameMap.add(col.getName());
        }

        // If table is authoritative, we do not need ColumnVindexes to help in resolving the unqualified columns.
        if (tbl.getColumnListAuthoritative()) {
            return cols;
        }
        // 把vindex的列放入cols后返回
        for (Vschema.ColumnVindex vindex : tbl.getColumnVindexesList()) {
            // List<String> columns = new ArrayList<>(vindex.getColumnsList());
            // vindex 都是单列的
            String column = vindex.getColumn();
            if (nameMap.contains(column)) {
                continue;
            }
            cols.add(new ColumnInfo(column));
            nameMap.add(column);
        }
        return cols;
    }
}
