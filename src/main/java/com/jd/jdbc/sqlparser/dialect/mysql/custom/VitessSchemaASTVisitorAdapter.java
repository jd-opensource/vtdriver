/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jd.jdbc.sqlparser.dialect.mysql.custom;

import static com.jd.jdbc.common.Constant.DEFAULT_DATABASE_PREFIX;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.MySqlASTVisitorAdapter;

public class VitessSchemaASTVisitorAdapter extends MySqlASTVisitorAdapter {

    private final ThreadLocal<String> sqlType = new ThreadLocal<>();

    @Override
    public boolean visit(SQLExprTableSource x) {
        String schemaName = x.getSchema();
        if (null != schemaName && !schemaName.startsWith(DEFAULT_DATABASE_PREFIX)) {
            schemaName = DEFAULT_DATABASE_PREFIX + schemaName;
            SQLPropertyExpr source = (SQLPropertyExpr) x.getExpr();
            String tableName = source.getName();
            schemaName = schemaName + "." + tableName;
            SQLExpr sqlExpr = SQLUtils.toMySqlExpr(schemaName);
            x.setExpr(sqlExpr);
        }
        return super.visit(x);
    }

    @Override
    public boolean visit(MySqlSelectQueryBlock s) {
        sqlType.set("SELECT");
        return true;
    }

    @Override
    public boolean visit(MySqlInsertStatement s) {
        sqlType.set("INSERT");
        return true;
    }

    @Override
    public boolean visit(MySqlUpdateStatement s) {
        sqlType.set("UPDATE");
        return true;
    }

    @Override
    public boolean visit(MySqlDeleteStatement s) {
        sqlType.set("DELETE");
        return true;
    }

    public synchronized String getSqlType() {
        return sqlType.get();
    }

}
