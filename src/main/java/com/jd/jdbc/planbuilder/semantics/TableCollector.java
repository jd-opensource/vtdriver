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

import com.jd.jdbc.planbuilder.RoutePlan;
import static com.jd.jdbc.planbuilder.semantics.DerivedTable.createDerivedTableForExpressions;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLSubqueryTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQuery;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqlparser.utils.TableNameUtils;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import vschema.Vschema;

/**
 * tableCollector is responsible for gathering information about the tables listed in the FROM clause,
 * and adding them to the current scope, plus keeping the global list of tables used in the query
 */
public class TableCollector {
    @Getter
    private List<TableInfo> tables;

    private Scoper scoper;

    private SchemaInformation si;

    private String currentDb;

    @Setter
    private Originable org;

    public TableCollector(Scoper scoper, SchemaInformation si, String currentDb) {
        this.scoper = scoper;
        this.si = si;
        this.currentDb = currentDb;
        this.tables = new ArrayList<>(10);
    }

    public void up(SQLObject cursor) throws SQLException {
        if (cursor instanceof SQLSubqueryTableSource) {
            if (((SQLSubqueryTableSource) cursor).getSelect().getQuery() instanceof SQLUnionQuery) {
                throw new SQLException(" unsupport union derived table ");
            } else if (((SQLSubqueryTableSource) cursor).getSelect().getQuery() instanceof MySqlSelectQueryBlock) {
                MySqlSelectQueryBlock queryBlock = (MySqlSelectQueryBlock) ((SQLSubqueryTableSource) cursor).getSelect().getQueryBlock();
                Scope tables = this.scoper.getWScope().get(queryBlock);
                DerivedTable tableInfo = createDerivedTableForExpressions(queryBlock.getSelectList(), null, tables.getTables(), this.org);
                tableInfo.checkForDuplicates();
                tableInfo.setAstnode((SQLSubqueryTableSource) cursor);
                tableInfo.setTableName(((SQLSubqueryTableSource) cursor).getAlias());
                this.tables.add(tableInfo);
                this.scoper.currentScope().addTable(tableInfo);

            } else {
                throw new SQLException("[BUG] " + cursor.toString() + " in a derived table");
            }
            return;
        }
        if (cursor instanceof SQLExprTableSource) {
            boolean isInfSchema = false;
            Vschema.Table tbl = null;
            Vschema.ColumnVindex vindex = null;
            if (RoutePlan.systemTable(TableNameUtils.getQualifier((SQLExprTableSource) cursor))) {
                isInfSchema = true;
            } else {
                SchemaInformation.SchemaInformationContext context = this.si.findTableOrVindex((SQLExprTableSource) cursor);
                if (context != null) {
                    tbl = context.getTable();
                    vindex = context.getVindex();
                }
             /* 生成一个VindexTable可以用sql查找用的hash算法计算对应值到哪个分片上
             https://vitess.io/docs/15.0/reference/features/vindexes/#query-vindex-functions*/

            }
            Scope scope = this.scoper.currentScope();
            TableInfo tableInfo = this.createTable((SQLExprTableSource) cursor, tbl, isInfSchema, vindex);
            tables.add(tableInfo);
            scope.addTable(tableInfo);
            return;
        }
    }

    private TableInfo createTable(SQLExprTableSource tableSource, Vschema.Table tbl, boolean isInfSchema, Vschema.ColumnVindex vindex) throws SQLException {
        String alias = TableNameUtils.getAlias(tableSource);
        RealTable table = new RealTable(alias, tableSource, tbl, isInfSchema);
        if (StringUtils.isEmpty(alias)) {
            String dbName = TableNameUtils.getDatabaseName(tableSource);
            if (StringUtils.isEmpty(dbName)) {
                dbName = currentDb;
            }
            table.setDbName(dbName);
            table.setTableName(TableNameUtils.getTableSimpleName(tableSource));
        }
        if (vindex != null) {
            return new VindexTable(table, vindex);
        }
        return table;
    }

    /**
     * tabletSetFor implements the originable interface, and that is why it lives on the analyser struct.
     * The code lives in this file since it is only touching tableCollector data
     *
     * @param t
     * @return
     */
    public TableSet tableSetFor(SQLTableSource t) throws SQLException {
        for (int i = 0; i < tables.size(); i++) {
            TableInfo t2 = tables.get(i);
            if (t == t2.getExpr()) {
                return TableSet.singleTableSet(i);
            }
        }
        throw new SQLException("unknown table");
    }

    /**
     * tableInfoFor returns the table info for the table set. It should contains only single table.
     *
     * @param id
     * @return
     */
    public TableInfo tableInfoFor(TableSet id) throws SQLException {
        int offset = id.tableOffset();
        if (offset < 0) {
            throw new Exception.ErrMultipleTablesException("[BUG] should only be used for single tables");
        }
        return tables.get(offset);
    }

}
