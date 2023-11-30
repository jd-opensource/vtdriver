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

import com.jd.jdbc.evalengine.TranslationLookup;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqltypes.VtType;
import static com.jd.jdbc.vindexes.VschemaConstant.TYPE_REFERENCE;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import vschema.Vschema;

@Getter
public class SemTable implements TranslationLookup {

    private List<TableInfo> tables;

    /**
     * NotSingleRouteErr stores any errors that have to be generated if the query cannot be planned as a single route.
     */
    private String notSingleRouteErr;

    /**
     * NotUnshardedErr stores any errors that have to be generated if the query is not unsharded.
     */
    private String notUnshardedErr;

    /**
     * Recursive contains the dependencies from the expression to the actual tables
     * in the query (i.e. not including derived tables). If an expression is a column on a derived table,
     * this map will contain the accumulated dependencies for the column expression inside the derived table
     */
    private ExprDependencies recursive;

    /**
     * Direct keeps information about the closest dependency for an expression.
     * It does not recurse inside derived tables and the like to find the original dependencies
     */
    private ExprDependencies direct;

    private Map<SQLObject, Type> exprTypes;

    private Map<SQLObject, Scope> selectScope;

    private String comments;

    private Map<MySqlSelectQueryBlock, Object[]> subqueryMap;

    private Map<MySqlSelectQueryBlock, Object> subqueryRef;

    /**
     * ColumnEqualities is used to enable transitive closures
     * if a == b and b == c then a == c
     */
    private Map<ColumnName, List<SQLExpr>> columnEqualities;

    //    // DefaultCollation is the default collation for this query, which is usually
//    // inherited from the connection's default collation.
//    Collation collations.ID
    private int collation;

    private String warning;

    public SemTable() {
        this.recursive = new ExprDependencies();
        this.direct = new ExprDependencies();
        this.columnEqualities = new HashMap<>(16);
    }

    public SemTable(ExprDependencies recursive, ExprDependencies direct, Map<SQLObject, Type> exprTypes, List<TableInfo> tables,
                    Map<SQLObject, Scope> selectScope, String notSingleRouteErr, String notUnshardedErr, String warning,
                    String comments, Map<MySqlSelectQueryBlock, Object[]> subqueryMap, Map<MySqlSelectQueryBlock, Object> subqueryRef,
                    Map<ColumnName, List<SQLExpr>> columnEqualities, Object collation) {
        this.tables = tables;
        this.notSingleRouteErr = notSingleRouteErr;
        this.notUnshardedErr = notUnshardedErr;
        this.recursive = recursive;
        this.direct = direct;
        this.exprTypes = exprTypes;
        this.selectScope = selectScope;
        this.comments = comments;
        this.subqueryMap = subqueryMap;
        this.subqueryRef = subqueryRef;
        this.columnEqualities = columnEqualities;
        this.warning = warning;
    }

    /**
     * TableSetFor returns the bitmask for this particular table
     *
     * @param t
     * @return
     */
    public TableSet tableSetFor(SQLTableSource t) {
        for (int i = 0; i < this.tables.size(); i++) {
            if (t == this.tables.get(i).getExpr()) {
                return TableSet.singleTableSet(i);
            }
        }
        return new TableSet();
    }

    /**
     * RecursiveDeps return the table dependencies of the expression.
     *
     * @param expr
     * @return
     */
    public TableSet recursiveDeps(SQLExpr expr) {
        return this.recursive.dependencies(expr);
    }

    /**
     * DirectDeps return the table dependencies of the expression.
     *
     * @param expr
     * @return
     */
    public TableSet directDeps(SQLExpr expr) {
        return this.direct.dependencies(expr);
    }

    // ReplaceTableSetFor replaces the given single TabletSet with the new *sqlparser.AliasedTableExpr
    public void replaceTableSetFor(TableSet id, SQLTableSource t) throws SQLException {
        if (id.numberOfTables() != 1) {
            throw new SQLException("BUG: tablet identifier should represent single table: table number =  " + id.numberOfTables());
        }

        int tblOffset = id.tableOffset();

        if (tblOffset < 0 || tblOffset >= this.tables.size()) {
            throw new SQLException("BUG: tablet identifier greater than number of tables: table size =  " + this.tables.size());
        }

        TableInfo ti = this.tables.get(tblOffset);

        if (ti instanceof RealTable) {
            ((RealTable) ti).setAstnode(t);
        } else if (ti instanceof DerivedTable) {
            ((DerivedTable) ti).setAstnode(t);
        } else {
            throw new SQLException("BUG: replacement not expected for   " + ti.toString());
        }
    }

    public TableInfo tableInfoFor(TableSet id) {
        int offset = id.tableOffset();
        if (offset < 0 || offset >= this.tables.size()) {
            // Log
            return null;
        }
        return this.tables.get(offset);

    }

    /**
     * TableInfoForExpr returns the table info of the table that this expression depends on.
     * Careful: this only works for expressions that have a single table dependency
     *
     * @param expr
     * @return
     */
    public TableInfo tableInfoForExpr(SQLExpr expr) {
        TableSet ts = this.direct.dependencies(expr);
        return this.tableInfoFor(ts);
    }

    /**
     * NeedsWeightString returns true if the given expression needs weight_string to do safe comparisons
     *
     * @param expr
     * @return
     */
    public boolean needsWeightString(SQLExpr expr) {
        Type typ = this.exprTypes.get(expr);
        if (typ == null) {
            return true;
        }
        // typ.Collation == collations.Unknown && !sqltypes.IsNumber(typ.Type)
        return false && !VtType.isNumber(typ.getType());
    }

    // AddColumnEquality adds a relation of the given colName to the ColumnEqualities map
    public void addColumnEquality(SQLName colName, SQLExpr expr) {
        TableSet tableSet = this.direct.dependencies(colName);
        ColumnName col = new ColumnName(tableSet, colName.getSimpleName());
        if (!this.columnEqualities.containsKey(col)) {
            this.columnEqualities.put(col, new ArrayList<>());
        }
        this.columnEqualities.get(col).add(expr);
    }

    // GetExprAndEqualities returns a slice containing the given expression, and it's known equalities if any
    public List<SQLExpr> getExprAndEqualities(SQLExpr expr) {
        List<SQLExpr> result = new ArrayList<>();
        result.add(expr);
        if (expr instanceof SQLName) {
            TableSet table = this.getDirect().dependencies(expr);
            ColumnName columnName = new ColumnName(table, ((SQLName) expr).getSimpleName());
            List<SQLExpr> exprs = this.getColumnEqualities().get(columnName);
            if (exprs != null) {
                result.addAll(exprs);
            }
        }
        return result;
    }

    public List<SQLExpr> getExprAndEqualities(List<SQLExpr> exprList) {
        List<SQLExpr> result = new ArrayList<>(exprList);
        return result;
    }

    @Override
    public int columnLookup(SQLName col) throws SQLException {
        throw new SQLException("column access not supported here");
    }


    // CollationForExpr returns the collation name of expressions in the query
    @Override
    public int collationForExpr(SQLExpr expr) {
        Type typ = this.getExprTypes().get(expr);
        if (typ == null) {
            return 0;
        }
        return typ.getCollations();
    }

    @Override
    public int defaultCollation() {
        return this.getCollation();
    }

    public Type typeFor(SQLExpr expr) {
        return this.exprTypes.get(expr);
    }

    public void copyExprInfo(SQLExpr src, SQLExpr dest) {
        Type srcType = this.exprTypes.get(src);
        if (srcType != null) {
            this.exprTypes.put(dest, srcType);
        }
    }

    /**
     * returns the single keyspace if all tables in the query are in the same, unsharded keyspace
     *
     * @return
     */
    public List<Vschema.Table> singleUnshardedKeyspace() {
        List<Vschema.Table> tables = new ArrayList<>();
        for (TableInfo table : this.tables) {
            Vschema.Table vindexTable = table.getVindexTable();
            if (vindexTable == null) {
                if (table instanceof DerivedTable) {
                    // derived tables are ok, as long as all real tables are from the same unsharded keyspace
                    // we check the real tables inside the derived table as well for same unsharded keyspace.
                    continue;
                }
                return null;
            }

            if (vindexTable.getType() != "") {
                // A reference table is not an issue when seeing if a query is going to an unsharded keyspace
                if (vindexTable.getType() == TYPE_REFERENCE) {
                    continue;
                }
                return null;
            }
            SQLTableSource tbl = table.getExpr();
            if (!(tbl instanceof SQLExprTableSource)) {
                return null;
            }
            tables.add(vindexTable);
        }
        return tables;
    }

    // CopyDependencies copies the dependencies from one expression into the other
    public void copyDependencies(SQLExpr from, SQLExpr to) {
        recursive.put(to,recursiveDeps(from));
        direct.put(to,directDeps(from));
    }

}
