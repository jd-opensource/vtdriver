/*
Copyright 2021 JD Project Authors.

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

package com.jd.jdbc.table.table;

import com.google.common.collect.Lists;
import com.jd.jdbc.Executor;
import com.jd.jdbc.VSchemaManager;
import com.jd.jdbc.VcursorImpl;
import com.jd.jdbc.engine.TableShardQuery;
import com.jd.jdbc.session.SafeSession;
import com.jd.jdbc.sqlparser.Comment;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLDeleteStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLUpdateStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlInsertReplaceStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.srvtopo.BoundQuery;
import com.jd.jdbc.srvtopo.ResolvedShard;
import com.jd.jdbc.table.TableTestUtil;
import com.jd.jdbc.vitess.VitessConnection;
import com.jd.jdbc.vitess.VitessStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

public class TestTableSort extends TestSuite {

    @Before
    public void init() {
        TableTestUtil.setSplitTableConfig("table/shardTableByMode.yml");
    }

    @After
    public void after() {
        TableTestUtil.setDefaultTableConfig();
    }

    @Test
    public void testSelctTableSort() throws SQLException {
        List<String> sqls = Lists.newArrayList("select id from table_engine_test where f_int in (1,2,3);",
            "select id from table_engine_test where f_int in (3,2,1);",
            "select id from table_engine_test where f_int in (2,3,1);",
            "select id from table_engine_test where f_int in (1,3,2);",
            "select id from table_engine_test where f_int in (1,2,4);",
            "select id from table_engine_test where f_int in (4,2,1);",
            "select id from table_engine_test where f_int in (1,4,2);",
            "select id from table_engine_test where f_int in (2,3,4);",
            "select id from table_engine_test where f_int in (2,4,3);",
            "select id from table_engine_test where f_int in (4,3,2);",
            "select id from table_engine_test;");

        validateTableSort(sqls);
    }

    @Test
    public void testUpdateTableSort() throws SQLException {
        List<String> sqls = Lists.newArrayList("update table_engine_test set id=3 where f_int in (1,2,3);",
            "update table_engine_test set id=3  where f_int in (3,2,1);",
            "update table_engine_test set id=3  where f_int in (2,3,1);",
            "update table_engine_test set id=3  where f_int in (1,3,2);",
            "update table_engine_test set id=3  where f_int in (1,2,4);",
            "update table_engine_test set id=3  where f_int in (4,2,1);",
            "update table_engine_test set id=3  where f_int in (1,4,2);",
            "update table_engine_test set id=3  where f_int in (2,3,4);",
            "update table_engine_test set id=3  where f_int in (2,4,3);",
            "update table_engine_test set id=3  where f_int in (4,3,2);",
            "update table_engine_test set id=3;");

        validateTableSort(sqls);
    }

    @Test
    public void testDeleteTableSort() throws SQLException {
        List<String> sqls = Lists.newArrayList("delete table_engine_test where f_int in (1,2,3);",
            "delete table_engine_test where f_int in (3,2,1);",
            "delete table_engine_test where f_int in (2,3,1);",
            "delete table_engine_test where f_int in (1,3,2);",
            "delete table_engine_test where f_int in (1,2,4);",
            "delete table_engine_test where f_int in (4,2,1);",
            "delete table_engine_test where f_int in (1,4,2);",
            "delete table_engine_test where f_int in (2,3,4);",
            "delete table_engine_test where f_int in (2,4,3);",
            "delete table_engine_test where f_int in (4,3,2);",
            "delete table_engine_test where f_int in (9,8,7,6,5,4,3,2,1);",
            "delete table_engine_test;");

        validateTableSort(sqls);
    }

    @Test
    public void testInsertTableSort() throws SQLException {
        List<String> sqls = Lists.newArrayList("insert into table_engine_test(id,f_key,f_int) values (1, 'aaa', 1),(2, 'bbb', 2),(3, 'ccc', 3)",
            "insert into table_engine_test(id,f_key,f_int) values (1, 'aaa', 3),(2, 'bbb', 2),(3, 'ccc', 1)",
            "insert into table_engine_test(id,f_key,f_int) values (1, 'aaa', 2),(2, 'bbb', 3),(3, 'ccc', 1)",
            "insert into table_engine_test(id,f_key,f_int) values (1, 'aaa', 1),(2, 'bbb', 3),(3, 'ccc', 2)",
            "insert into table_engine_test(id,f_key,f_int) values (1, 'aaa', 1),(2, 'bbb', 2),(3, 'ccc', 4)",
            "insert into table_engine_test(id,f_key,f_int) values (1, 'aaa', 4),(2, 'bbb', 2),(3, 'ccc', 1)",
            "insert into table_engine_test(id,f_key,f_int) values (1, 'aaa', 1),(2, 'bbb', 4),(3, 'ccc', 2)",
            "insert into table_engine_test(id,f_key,f_int) values (1, 'aaa', 2),(2, 'bbb', 3),(3, 'ccc', 4)",
            "insert into table_engine_test(id,f_key,f_int) values (1, 'aaa', 2),(2, 'bbb', 4),(3, 'ccc', 3)",
            "insert into table_engine_test(id,f_key,f_int) values (1, 'aaa', 4),(2, 'bbb', 3),(3, 'ccc', 2)");

        validateTableSort(sqls);
    }

    private void validateTableSort(List<String> sqls) throws SQLException {
        for (String sql : sqls) {
            Map<ResolvedShard, List<BoundQuery>> shardQueryList1 = getShardQueryList(sql);
            for (Map.Entry<ResolvedShard, List<BoundQuery>> queriesMap : shardQueryList1.entrySet()) {
                ResolvedShard shard = queriesMap.getKey();
                List<BoundQuery> queries = queriesMap.getValue();
                System.out.println("target shard:" + shard.getTarget().getShard());
                for (BoundQuery query : queries) {
                    printComment(query.toString());
                }
                validateQueriesOrder(queries);
            }
        }
        printInfo("PASS");
    }

    private void validateQueriesOrder(List<BoundQuery> queries) {
        List<String> currentTables = null;
        for (BoundQuery query : queries) {
            List<String> tables = getTableFromSQL(query.getSql());
            if (currentTables == null) {
                currentTables = tables;
                continue;
            }
            int i = compare(currentTables, tables);
            if (i >= 1) {
                Assert.fail("table sort ");
            }
        }
    }

    private int compare(List<String> t1, List<String> t2) {
        if (t1.size() > t2.size()) {
            return 1;
        }
        if (t1.size() < t2.size()) {
            return -1;
        }
        for (int i = 0; i < t1.size(); i++) {
            int result = this.compare(t1.get(i), t2.get(i));
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    private int compare(String t1, String t2) {
        return t1.compareToIgnoreCase(t2);
    }

    private List<String> getTableFromSQL(String sql) {
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        if (stmt instanceof SQLSelectStatement) {
            SQLSelectQuery selectQuery = ((SQLSelectStatement) stmt).getSelect().getQuery();
            if (!(selectQuery instanceof MySqlSelectQueryBlock)) {
                return null;
            }
            SQLTableSource tableSource = ((MySqlSelectQueryBlock) selectQuery).getFrom();
            return getTableName(tableSource);
        } else if (stmt instanceof MySqlInsertReplaceStatement) {
            String tabletName = ((MySqlInsertReplaceStatement) stmt).getTableSource().getName().getSimpleName();
            return Lists.newArrayList(tabletName);
        } else if (stmt instanceof SQLUpdateStatement) {
            return getTableName(((SQLUpdateStatement) stmt).getTableSource());
        } else if (stmt instanceof SQLDeleteStatement) {
            return getTableName(((SQLDeleteStatement) stmt).getTableSource());
        }
        return null;
    }

    private List<String> getTableName(SQLTableSource tableExpr) {
        if (tableExpr instanceof SQLExprTableSource) {
            return Lists.newArrayList(((SQLExprTableSource) tableExpr).getName().getSimpleName());
        }
        return null;
    }

    private Map<ResolvedShard, List<BoundQuery>> getShardQueryList(String sql) throws SQLException {
        try (VitessConnection conn = (VitessConnection) getConnection(TestSuite.Driver.of(TestSuiteShardSpec.TWO_SHARDS));
             VitessStatement stmt = (VitessStatement) conn.createStatement()) {
            Executor executor = Executor.getInstance(300);
            SQLStatement queryStmt = SQLUtils.parseSingleMysqlStatement(sql);
            Executor.PlanResult ps = executor.getPlan(conn.getCtx(), conn.getDefaultKeyspace(), queryStmt, new HashMap<>(), true, SafeSession.newSafeSession(conn).getCharEncoding());
            if (!(ps.getPlan().getPrimitive() instanceof TableShardQuery) ) {
                Assert.fail("Plan should be Table Related Plan, but now it's " + ps.getPlan().getPrimitive().getClass().getName());
            }

            VcursorImpl vCursor = new VcursorImpl(conn.getCtx(), SafeSession.newSafeSession(conn), new Comment(sql), stmt.getExecutor(),
                (VSchemaManager) conn.getCtx().getContextValue(VitessConnection.ContextKey.CTX_VSCHEMA_MANAGER), conn.getResolver());

            return ((TableShardQuery) ps.getPlan().getPrimitive()).getShardQueryList(conn.getCtx(), vCursor, ps.getBindVariableMap());
        }
    }
}
