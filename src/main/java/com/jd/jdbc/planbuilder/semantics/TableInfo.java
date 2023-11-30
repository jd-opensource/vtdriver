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
import vschema.Vschema;

/**
 * TableInfo contains information about tables
 */
public interface TableInfo {

    /**
     * Name returns the table name
     *
     * @return
     * @throws SQLException
     */
    SQLExprTableSource name() throws SQLException;

    /**
     * GetVindexTable returns the vschema version of this TableInfo
     *
     * @return
     */
    Vschema.Table getVindexTable();

    /**
     * IsInfSchema returns true if this table is information_schema
     *
     * @return
     */
    boolean isInfSchema();

    /**
     * matches returns true if the provided table name matches this TableInfo
     *
     * @param name
     * @return
     */
    boolean matches(SQLObject name) throws SQLException;


    /**
     * authoritative is true if we have exhaustive column information
     *
     * @return
     */
    boolean authoritative();

    /**
     * getExpr returns the AST struct behind this table
     *
     * @return
     */
    SQLTableSource getExpr();

    /**
     * getColumns returns the known column information for this table
     *
     * @return
     */
    List<ColumnInfo> getColumns();

    Dependencies dependencies(String colNames, Originable org) throws SQLException;

    TableSet getTableSet(Originable org) throws SQLException;
//    getExprFor(s string) (sqlparser.Expr, error)
//    getTableSet(org originable) TableSet
}
