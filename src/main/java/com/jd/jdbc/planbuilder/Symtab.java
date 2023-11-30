/*
Copyright 2021 JD Project Authors. Licensed under Apache-2.0.

Copyright 2019 The Vitess Authors.

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

package com.jd.jdbc.planbuilder;

import com.jd.jdbc.planbuilder.tableplan.TableRoutePlan;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqlparser.utils.TableNameUtils;
import com.jd.jdbc.tindexes.LogicTable;
import com.jd.jdbc.tindexes.TableIndex;
import com.jd.jdbc.vindexes.SingleColumn;
import com.jd.jdbc.vindexes.hash.BinaryHash;
import io.netty.util.internal.StringUtil;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import vschema.Vschema;

@Getter
@Setter
public class Symtab {
    /**
     * only support BinaryHash which implements SingleColumn and named 'hash'
     */
    private static Set<String> allowedColumnVindexSet = new HashSet<String>() {{
        add("hash");
    }};

    private Map<String, Table> tables;

    private List<String> tableNames;

    private Map<String, Column> uniqueColumns;

    private AbstractRoutePlan singleRoute;

    private List<ResultColumn> resultColumns;

    private Symtab outer;
    //SQLColumnReference

    //Externs       []*sqlparser.ColName
    // ColName represents a column name.
    // Metadata is not populated by the parser.
    // It's a placeholder for analyzers to store
    // additional data, typically info about which
    // table or column this node references.
    private List<SQLName> externs;

    public Symtab() {
        this.tables = new HashMap<>();
        this.tableNames = new ArrayList<>();
        this.uniqueColumns = new HashMap<>();
        this.resultColumns = new ArrayList<>();
        this.externs = new ArrayList<>();
    }

    public Symtab(AbstractRoutePlan routeBuilder) {
        this.tables = new HashMap<>();
        this.tableNames = new ArrayList<>();
        this.uniqueColumns = new HashMap<>();
        this.singleRoute = routeBuilder;
        this.resultColumns = new ArrayList<>();
        this.externs = new ArrayList<>();
    }

    /**
     * For test case, we can registe another vindex which implements SingleColumn
     *
     * @param vindexName
     */
    public static void registSingleColumnVindex(String vindexName) {
        allowedColumnVindexSet.add(vindexName.toLowerCase());
    }

    /**
     * NewResultColumn creates a new resultColumn based on the supplied expression.
     * The created symbol is not remembered until it is later set as ResultColumns
     * after all select expressions are analyzed.
     *
     * @param expr
     * @param origin
     * @return
     */
    public static ResultColumn newResultColumn(SQLSelectItem expr, Builder origin) {
        ResultColumn resultColumn = new ResultColumn();
        resultColumn.setAlias(StringUtils.nullToEmpty(expr.getAlias()));
        if (expr.getExpr() instanceof SQLPropertyExpr
            || expr.getExpr() instanceof SQLIdentifierExpr) {
            // If no alias was specified, then the base name
            // of the column becomes the alias.
            if (StringUtil.isNullOrEmpty(resultColumn.getAlias())) {
                resultColumn.setAlias(((SQLName) expr.getExpr()).getSimpleName());
            }
            // If it's a col it should already have metadata.
            resultColumn.setColumn(((SQLName) expr.getExpr()).getMetadata());
        } else {
            // We don't generate an alias if the expression is non-trivial.
            // Just to be safe, generate an anonymous column for the expression.
            resultColumn.setColumn(new Column(origin));
        }
        return resultColumn;
    }

    /**
     * BuildColName builds a *sqlparser.ColName for the resultColumn specified
     * by the index. The built ColName will correctly reference the resultColumn
     * it was built from.
     *
     * @param rcs
     * @param index
     * @return
     * @throws SQLException
     */
    public static SQLName buildColName(List<ResultColumn> rcs, Integer index) throws SQLException {
        String alias = rcs.get(index).getAlias();
        if (StringUtil.isNullOrEmpty(alias)) {
            throw new SQLException("cannot reference a complex expression");
        }
        for (int i = 0; i < rcs.size(); i++) {
            ResultColumn rc = rcs.get(i);
            if (i == index) {
                continue;
            }
            if (rc.getAlias() != null && SQLUtils.nameEquals(rc.getAlias(), alias)) {
                throw new SQLException("ambiguous symbol reference: " + alias);
            }
        }
        SQLIdentifierExpr identifierExpr = new SQLIdentifierExpr();
        identifierExpr.setMetadata(rcs.get(index).getColumn());
        identifierExpr.setName(alias);
        return identifierExpr;
    }

    public void setResultColumns(List<ResultColumn> rcs) {
        for (ResultColumn rc : rcs) {
            rc.getColumn().setSt(this);
        }
        this.resultColumns = rcs;
    }

    /**
     * AddTSchemaTable adds a tschema table to symtab
     *
     * @param alias
     * @param ltb
     * @param tableRouteBuilder
     * @throws SQLException
     */
    public void addTSchemaTable(SQLExprTableSource alias, LogicTable ltb, TableRoutePlan tableRouteBuilder) throws SQLException {
        Table table = new Table(alias, tableRouteBuilder);

        Column col = new Column(tableRouteBuilder, this);
        col = table.mergeColumn(ltb.getTindexCol().getColumnName(), col);
        if (col.getTindex() == null) {
            col.setTindex(ltb.getTableIndex());
        }

        this.addTable(table);
    }

    /**
     * AddVSchemaTable adds a vschema table to symtab.
     *
     * @param alias
     * @param routeBuilder
     * @param vschemaTable
     * @return
     * @throws
     */
    public void addVSchemaTable(SQLExprTableSource alias, Vschema.Table vschemaTable, RoutePlan routeBuilder) throws SQLException {
        Table table = new Table(alias, routeBuilder, vschemaTable);

        for (Vschema.Column column : vschemaTable.getColumnsList()) {
            String columnName = column.getName();
            Column col = new Column(routeBuilder, this, column.getType());
            table.mergeColumn(columnName, col);
        }

        if (vschemaTable.getColumnListAuthoritative()) {
            table.isAuthoritative = true;
        }

        for (Vschema.ColumnVindex columnVindex : vschemaTable.getColumnVindexesList()) {
            if (!allowedColumnVindexSet.contains(columnVindex.getName().toLowerCase())) {
                continue;
            }

            Column col = new Column(routeBuilder, this);
            col = table.mergeColumn(columnVindex.getColumn(), col);
            if (col.getVindex() == null) {
                col.setVindex(new BinaryHash());
            }
        }

        //添加自增列
        Vschema.AutoIncrement autoIncrement = vschemaTable.getAutoIncrement();
        if (autoIncrement != Vschema.AutoIncrement.getDefaultInstance()) {
            String autoIncrementColumn = autoIncrement.getColumn();
            if (!StringUtil.isNullOrEmpty(autoIncrementColumn)) {
                if (table.getColumns().get(autoIncrementColumn.toLowerCase()) == null) {
                    Column col = new Column(routeBuilder, this);
                    table.mergeColumn(autoIncrementColumn, col);
                }
            }
        }

        this.addTable(table);
    }

    /**
     * Merge merges the new symtab into the current one.
     * Duplicate table aliases return an error.
     * uniqueColumns is updated, but duplicates are removed.
     * Merges are only performed during the FROM clause analysis.
     * At this point, only tables and uniqueColumns are set.
     * All other fields are ignored.
     *
     * @param newSymtab
     */
    public void merge(Symtab newSymtab) throws SQLException {
        if (this.tableNames == null || this.tableNames.isEmpty()
            || newSymtab.tableNames == null || newSymtab.tableNames.isEmpty()) {
            // If any side of symtab has anonymous tables,
            // we treat the merged symtab as having anonymous tables.
            return;
        }
        for (Table table : newSymtab.tables.values()) {
            this.addTable(table);
        }
    }

    /**
     * AddTable adds a table to symtab.
     *
     * @param table
     * @throws SQLException
     */
    public void addTable(Table table) throws SQLException {
        if (!(table.origin instanceof RoutePlan) || ((RoutePlan) table.origin).resolve() != this.singleRoute) {
            this.singleRoute = null;
        }

        if (this.tables.get(table.getTableAlias()) != null) {
            throw new SQLException("duplicate symbol: " + table.getTableAlias());
        }

        this.tables.put(table.getTableAlias().toLowerCase(), table);
        this.tableNames.add(table.getTableAlias().toLowerCase());

        // update the uniqueColumns list, and eliminate
        // duplicate symbols if found
        if (table.getColumns() == null) {
            return;
        }
        for (Map.Entry<String, Column> column : table.getColumns().entrySet()) {
            Column col = column.getValue();
            String colName = column.getKey();

            col.setSt(this);
            if (this.uniqueColumns.get(colName) != null) {
                this.uniqueColumns.put(colName, null);
                continue;
            }
            this.uniqueColumns.put(colName, col);
        }
    }

    /**
     * AllTables returns an ordered list of all current tables.
     *
     * @return
     */
    public List<Table> allTables() {
        if (this.tableNames.isEmpty()) {
            return null;
        }
        List<Table> tables = new ArrayList<>(this.tableNames.size());
        for (String tableAliasName : this.tableNames) {
            tables.add(this.tables.get(tableAliasName));
        }
        return tables;
    }

    /**
     * FindTable finds a table in symtab. This function is specifically used
     * for expanding 'select a.*' constructs. If you're in a subquery,
     * you're most likely referring to a table in the local 'from' clause.
     * For this reason, the search is only performed in the current scope.
     * This may be a deviation from the formal definition of SQL, but there
     * are currently no use cases that require the full support.
     *
     * @param tname
     * @return
     * @throws SQLException
     */
    public Table findTable(String tname) throws SQLException {
        if (this.tableNames == null) {
            // Unreachable because current code path checks for this condition
            // before invoking this function.
            return null;
        }
        if (!this.tables.containsKey(tname.toLowerCase())) {
            throw new SQLException("table " + tname + " not found");
        }
        return this.tables.get(tname.toLowerCase());
    }

    public Column getColumn(SQLExpr expr) throws SQLException {
        SQLName col;
        if (expr instanceof SQLName) {
            col = (SQLName) expr;
        } else {
            return null;
        }

        Column column = col.getMetadata();
        if (column == null) {
            // Find will set the Metadata.
            this.find(col);
        }

        column = col.getMetadata();

        return column;
    }

    public TableIndex getColumnTindex(SQLExpr expr, TableRoutePlan scope) throws SQLException {
        Column column = getColumn(expr);

        if (column == null) {
            return null;
        }

        Builder origin = column.origin();
        if (origin != scope) {
            return null;
        }

        return column.getTindex();
    }

    /**
     * Vindex returns the vindex if the expression is a plain column reference
     * that is part of the specified route, and has an associated vindex.
     *
     * @param expr
     * @param scope
     * @return
     */
    public BinaryHash getColumnVindex(SQLExpr expr, RoutePlan scope) throws SQLException {
        Column column = getColumn(expr);

        if (column == null) {
            return null;
        }

        Builder origin = column.origin();
        if (origin != scope) {
            return null;
        }

        return column.getVindex();
    }

    /**
     * Find returns the builder for the symbol referenced by col.
     * If a reference is found, col.Metadata is set to point
     * to it. Subsequent searches will reuse this metadata.
     * <p>
     * Unqualified columns are searched in the following order:
     * 1. ResultColumns
     * 2. uniqueColumns
     * 3. symtab has only one table. The column is presumed to
     * belong to that table.
     * 4. symtab has more than one table, but all tables belong
     * to the same route. An anonymous column is created against
     * the current route.
     * If all the above fail, an error is returned. This means
     * that an unqualified reference can only be locally resolved.
     * <p>
     * For qualified columns, we first look for the table. If one
     * is found, we look for a column in the pre-existing list.
     * If one is not found, we optimistically create an entry
     * presuming that the table has such a column. If this is
     * not the case, the query will fail when sent to vttablet.
     * If the table is not found in the local scope, the search
     * is continued in the outer scope, but only if ResultColumns
     * is not set (this is MySQL behavior).
     * <p>
     * For symbols that were found locally, isLocal is returned
     * as true. Otherwise, it's returned as false and the symbol
     * gets added to the Externs list, which can later be used
     * to decide where to push-down the subquery.
     *
     * @param col
     * @return
     */
    public FindResponse find(SQLName col) throws SQLException {
        Column metaData = col.getMetadata();
        if (metaData != null) {
            return new FindResponse(metaData.origin(), metaData.getSt() == this);
        }

        //unqualified column case.
        if (!(col instanceof SQLPropertyExpr)) {
            Column column = this.searchResultColumn(col);
            if (column != null) {
                col.setMetadata(column);
                return new FindResponse(column.origin(), true);
            }
        }

        // Steps 2-4 performed by searchTables.
        Column column = this.searchTables(col);
        if (column != null) {
            col.setMetadata(column);
            return new FindResponse(column.origin(), true);
        }

        if (this.outer == null) {
            throw new SQLException("symbol " + col + " not found");
        }

        // Search is not continued if ResultColumns already has values:
        // select a ... having ... (select b ... having a...). In this case,
        // a (in having) should not match the outer-most 'a'. This is to
        // match MySQL's behavior.
        if (!this.resultColumns.isEmpty()) {
            throw new SQLException("symbol " + col + " not found in subquery");
        }

        FindResponse outerFindResponse = this.outer.find(col);
        Builder origin = outerFindResponse.getOrigin();
        this.externs.add(col);
        return new FindResponse(origin, false);
    }

    public Table findOriginTable(SQLName col) throws SQLException {
        Table table = null;
        // @@ syntax is only allowed for dual tables, in which case there should be
        // only one in the symtab. So, such expressions will be implicitly matched.
        // 暂时没实现前缀为@@
        if (!(col instanceof SQLPropertyExpr)) {
            // Search uniqueColumns first. If found, our job is done.
            // Check for nil because there can be nil entries if there
            // are duplicate columns across multiple tables.
            if (this.uniqueColumns.containsKey(col.getSimpleName().toLowerCase())) {
                Column column = this.uniqueColumns.get(col.getSimpleName().toLowerCase());
                Map<String, Table> tableMap = column.getSt().getTables();
                for (Map.Entry<String, Table> tMap : tableMap.entrySet()) {
                    Table t = tMap.getValue();
                    for (String colName : t.getColumnNames()) {
                        if (colName.equalsIgnoreCase(col.getSimpleName())) {
                            table = t;
                        }
                    }
                }
            }

            if (this.tables.size() == 1) {
                // If there's only one table match against it.
                // Loop executes once to match the only table.
                for (Map.Entry<String, Table> stTableMap : this.tables.entrySet()) {
                    table = stTableMap.getValue();
                }
            } else {
                throw new SQLException("symbol " + col.getSimpleName() + " not found");
            }
        } else {
            SQLExpr qualifier = ((SQLPropertyExpr) col).getOwner();
            if (qualifier instanceof SQLPropertyExpr) {
                qualifier = new SQLIdentifierExpr(((SQLPropertyExpr) qualifier).getName());
            }

            if (qualifier instanceof SQLIdentifierExpr) {
                String qualifierName = ((SQLIdentifierExpr) qualifier).getSimpleName();
                if (SQLUtils.nameEquals("UNKNOWN", qualifierName)) {
                    return this.findOriginTable(new SQLIdentifierExpr(col.getSimpleName()));
                }
                if (this.tables.containsKey(qualifierName.toLowerCase())) {
                    table = this.tables.get(qualifierName.toLowerCase());
                } else {
                    return null;
                }
            } else {
                throw new SQLException("unsupport sql column qualifier: " + qualifier.toString());
            }
        }
        return table;
    }

    /**
     * searchTables looks for the column in the tables. The search order
     * is as described in Find.
     *
     * @param col
     * @return
     */
    private Column searchTables(SQLName col) throws SQLException {
        Table table = null;
        // @@ syntax is only allowed for dual tables, in which case there should be
        // only one in the symtab. So, such expressions will be implicitly matched.
        // 暂时没实现前缀为@@
        if (!(col instanceof SQLPropertyExpr)) {
            // Search uniqueColumns first. If found, our job is done.
            // Check for nil because there can be nil entries if there
            // are duplicate columns across multiple tables.
            if (this.uniqueColumns.containsKey(col.getSimpleName().toLowerCase())) {
                Column c = this.uniqueColumns.get(col.getSimpleName().toLowerCase());
                if (c != null) {
                    return c;
                }
            }

            if (this.tables.size() == 1) {
                // If there's only one table match against it.
                // Loop executes once to match the only table.
                for (Map.Entry<String, Table> stTableMap : this.tables.entrySet()) {
                    table = stTableMap.getValue();
                }
            } else if (this.singleRoute != null) {
                // If there's only one route, create an anonymous symbol.
                return new Column(this.singleRoute, this);
            } else {
                throw new SQLException("symbol " + col.getSimpleName() + " not found");
            }
        } else {
            SQLExpr qualifier = ((SQLPropertyExpr) col).getOwner();
            if (qualifier instanceof SQLPropertyExpr) {
                qualifier = new SQLIdentifierExpr(((SQLPropertyExpr) qualifier).getName());
            }

            if (qualifier instanceof SQLIdentifierExpr) {
                String qualifierName = ((SQLIdentifierExpr) qualifier).getSimpleName();
                if (SQLUtils.nameEquals("UNKNOWN", qualifierName)) {
                    return this.searchTables(new SQLIdentifierExpr(col.getSimpleName()));
                }
                if (this.tables.containsKey(qualifierName.toLowerCase())) {
                    table = this.tables.get(qualifierName.toLowerCase());
                } else {
                    return null;
                }
            } else {
                throw new SQLException("unsupport sql column qualifier: " + qualifier.toString());
            }
        }

        // At this point, t should be set.
        Column column;
        if (table.getColumns() == null || table.getColumns().isEmpty()
            || !(table.getColumns().containsKey(col.getSimpleName().toLowerCase()))) {
            // We know all the column names of a subquery. Might as well return an error if it's not found.
            if (table.getIsAuthoritative()) {
                throw new SQLException("symbol " + col + " not found in table or subquery");
            }
            column = new Column(table.getOrigin(), this);
            table.addColumn(col.getSimpleName(), column);
        } else {
            column = table.getColumns().get(col.getSimpleName().toLowerCase());
        }

        return column;
    }

    /**
     * searchResultColumn looks for col in the results columns.
     *
     * @param col
     * @return
     * @throws SQLException
     */
    private Column searchResultColumn(SQLName col) throws SQLException {
        ResultColumn cursym = null;
        for (ResultColumn resultColumn : this.resultColumns) {
            if (SQLUtils.nameEquals(resultColumn.getAlias(), col.getSimpleName())) {
                if (cursym != null) {
                    throw new SQLException("ambiguous symbol reference: " + col.getSimpleName());
                }
                cursym = resultColumn;
            }
        }
        if (cursym != null) {
            return cursym.getColumn();
        }
        return null;
    }

    /**
     * ResolveSymbols resolves all column references against symtab.
     * This makes sure that they all have their Metadata initialized.
     * If a symbol cannot be resolved or if the expression contains
     * a subquery, an error is returned.
     *
     * @param node
     * @throws SQLException
     */
    public void resolveSymbols(List<SQLExpr> node) throws SQLException {
        for (SQLExpr expr : node) {
            if (expr instanceof SQLName) {
                this.find((SQLName) expr);
            }
        }
    }

    public SingleColumn vindex(SQLExpr expr, RoutePlan scope) {
        if (!(expr instanceof SQLName)) {
            return null;
        }
        SQLName col = (SQLName) expr;
        if (col.getMetadata() == null) {
            try {
                this.find(col);
            } catch (SQLException e) {
                return null;
            }
        }
        Column c = col.getMetadata();
        if (c.origin() != scope) {
            return null;
        }
        return c.getVindex();
    }

    @Getter
    @AllArgsConstructor
    public static class FindResponse {
        private final Builder origin;

        private final Boolean isLocal;
    }

    /**
     * table is part of symtab.
     * It represents a table alias in a FROM clause. It points
     * to the builder that represents it.
     */
    @Data
    public static class Table {
        private SQLExprTableSource tableName;

        private Map<String, Column> columns;

        // ColIdent is a case insensitive SQL identifier. It will be escaped with
        // backquotes if necessary.
        private List<String> columnNames;

        private Boolean isAuthoritative;

        private Builder origin;

        private Vschema.Table vschemaTable;

        public Table(SQLExprTableSource tableName, Builder origin) {
            this.tableName = tableName;
            this.columns = new HashMap<>(16, 1);
            this.columnNames = new ArrayList<>();
            this.isAuthoritative = false;
            this.origin = origin;
            this.vschemaTable = Vschema.Table.newBuilder().build();
        }

        public Table(SQLExprTableSource tableName, Builder origin, Vschema.Table vschemaTable) {
            this.tableName = tableName;
            this.columns = new HashMap<>(16, 1);
            this.columnNames = new ArrayList<>();
            this.isAuthoritative = false;
            this.origin = origin;
            this.vschemaTable = vschemaTable;
        }

        /**
         * mergeColumn merges or creates a new column for the table.
         * If the table is authoritative and the column doesn't already
         * exist, it returns an error. If the table is not authoritative,
         * the column is added if not already present.
         *
         * @param alias
         * @param col
         * @return
         * @throws SQLException
         */
        public Column mergeColumn(String alias, Column col) throws SQLException {
            if (this.columns == null) {
                this.columns = new HashMap<>();
            }
            String lowered = alias.toLowerCase();
            if (this.columns.containsKey(lowered)) {
                return this.columns.get(lowered);
            }
            if (this.isAuthoritative != null && this.isAuthoritative) {
                throw new SQLException("column " + alias + " not found in " + this.tableName);
            }
            col.setColNumber(this.columnNames != null ? this.columnNames.size() : 0);
            this.columns.put(lowered, col);
            if (this.columnNames == null) {
                this.columnNames = new ArrayList<>();
            }

            this.columnNames.add(alias);
            return col;
        }

        public String getTableAlias() throws SQLException {
            String tableName = TableNameUtils.getTableSimpleName(this.tableName);
            String tableAlias = this.tableName.getAlias();

            if (tableAlias != null) {
                return tableAlias;
            }

            return tableName;
        }

        public void addColumn(String alias, Column c) {
            if (this.columns == null) {
                this.columns = new HashMap<>(16, 1);
            }
            String lowered = alias.toLowerCase();
            if (!this.columns.containsKey(lowered)) {
                c.setColNumber(this.columnNames.size());
                this.columns.put(lowered, c);
            }
            this.columnNames.add(alias);
        }
    }
}
