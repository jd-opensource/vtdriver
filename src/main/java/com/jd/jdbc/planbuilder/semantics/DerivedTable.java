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
import com.jd.jdbc.sqlparser.SqlParser;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import com.jd.jdbc.sqlparser.utils.TableNameUtils;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import vschema.Vschema;

@Setter
@Getter
public class DerivedTable implements TableInfo {

    String tableName;

    SQLTableSource astnode;

    List<String> columnNames;

    List<SQLExpr> cols;

    TableSet tables;

    public DerivedTable() {
        this.tables = new TableSet();
        this.cols = new ArrayList<>();
        this.columnNames = new ArrayList<>();
    }

    @Override
    public SQLExprTableSource name() throws SQLException {
        if (astnode.getAlias() != null) {
            return new SQLExprTableSource(new SQLIdentifierExpr(astnode.getAlias()), null);
        }
        throw new SQLException("Every derived table must have its own alias");
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
    public boolean matches(SQLObject expr) throws SQLException {
        String tableName;
        SQLPropertyExpr leftExpr = (SQLPropertyExpr) expr;
        tableName = leftExpr.getOwnernName();
        String qualifier = TableNameUtils.getQualifier(astnode);
        return SQLUtils.nameEquals(tableName, qualifier) && SQLUtils.nameEquals(tableName, this.tableName);
    }

    @Override
    public boolean authoritative() {
        return false;
    }

    @Override
    public SQLTableSource getExpr() {
        return this.astnode;
    }

    @Override
    public List<ColumnInfo> getColumns() {
        return null;
    }

    @Override
    public Dependencies dependencies(String colNames, Originable org) throws SQLException {
        TableSet directDeps = org.tableSetFor(this.astnode);
        for (int i = 0; i < this.columnNames.size(); i++) {
            if (!this.columnNames.get(i).equals(colNames)) {
                continue;
            }
            Triple<TableSet, TableSet, Type> recursiveDeps = org.depsForExpr(this.cols.get(i));
            return Dependencies.createCertain(directDeps, recursiveDeps.getMiddle(), recursiveDeps.getRight());
        }
        if (!this.hasStar()) {
            return new Nothing();
        }

        return Dependencies.createUncertain(directDeps, this.tables);

    }

    public Boolean hasStar() {
        if (this.tables == null) {
            return false;
        }
        return this.tables.numberOfTables() > 0;
    }

    @Override
    public TableSet getTableSet(Originable org) {
        return this.tables;
    }

    //todo sqlparser.Columns不清楚什么类型节点用Object[]先代替
    public static DerivedTable createDerivedTableForExpressions(List<SQLSelectItem> expressions, Object[] cols, List<TableInfo> tables, Originable org) throws SQLException {
        DerivedTable vTbl = new DerivedTable();
        for (int i = 0; i < expressions.size(); i++) {
            SQLSelectItem expr = expressions.get(i);
            if (SqlParser.SelectExpr.StarExpr.equals(SqlParser.SelectExpr.type(expr))) {
                for (TableInfo table : tables) {
                    vTbl.tables.mergeInPlace(table.getTableSet(org));
                }
            } else {
                vTbl.cols.add(expr.getExpr());
                if (cols != null) {
                    vTbl.columnNames.add(cols[i].toString());
                } else if (expr.getAlias() == null) {
                    // for projections, we strip out the qualifier and keep only the column name
                    if (expr.getExpr() instanceof SQLPropertyExpr) {
                        vTbl.columnNames.add(((SQLPropertyExpr) expr.getExpr()).getName());
                    } else {
                        vTbl.columnNames.add(expr.getExpr().toString());
                    }
                } else {
                    vTbl.columnNames.add(expr.getAlias());
                }
            }
        }
        return vTbl;
    }

    public void checkForDuplicates() throws SQLException {
        for (int i = 0; i < this.columnNames.size(); i++) {
            String name = this.columnNames.get(i);
            for (int j = 0; j < this.columnNames.size(); j++) {
                if (i == j) {
                    continue;
                }
                if (this.columnNames.get(j).equals(name)) {
                    throw new SQLException("Duplicate column name '" + name + "'");
                }
            }
        }
    }
}