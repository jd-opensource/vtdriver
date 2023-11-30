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

package com.jd.jdbc.planbuilder.gen4;

import com.jd.BaseTest;
import com.jd.jdbc.planbuilder.semantics.Analyzer;
import com.jd.jdbc.planbuilder.semantics.FakeSI;
import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQuery;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Ignore;
import org.junit.Test;
import vschema.Vschema;

public class AnalyzerTest extends BaseTest {

    private TableSet none = TableSet.emptyTableSet();

    private TableSet t0 = new TableSet(0, null);

    private TableSet t1 = TableSet.singleTableSet(0);

    private TableSet t2 = TableSet.singleTableSet(1);

    private TableSet t3 = TableSet.singleTableSet(2);

    private TableSet t4 = TableSet.singleTableSet(3);

    private TableSet t5 = TableSet.singleTableSet(4);

    private Map<String, Vschema.Keyspace> ks1;

    private SQLExpr extract(SQLSelectStatement in, int idx) {
        return in.getSelect().getFirstQueryBlock().getSelectList().get(idx).getExpr();
    }

    private SQLExpr extract(MySqlSelectQueryBlock in, int idx) {
        return in.getSelectList().get(idx).getExpr();
    }

    @Test
    public void testBindingSingleTablePositive() throws SQLException {
        String[] queries = {
            "select col from tabl",
            "select uid from t2",
            "select tabl.col from tabl",
            "select d.tabl.col from tabl",
            "select col from d.tabl",
            "select tabl.col from d.tabl",
            "select d.tabl.col from d.tabl",
            "select col+col from tabl",
            "select max(col1+col2) from d.tabl",
            "select max(id) from t1",
        };
        for (String query : queries) {
            ParseAndAnalyze stmAndsem = parseAndAnalyze(query, "d");
            SemTable semTable = stmAndsem.getSemTable();
            SQLSelectStatement sel = (SQLSelectStatement) stmAndsem.getStm();
            SQLTableSource t1 = sel.getSelect().getFirstQueryBlock().getFrom();
            TableSet ts = semTable.tableSetFor(t1);
            assertEquals(printFail(query + ": get singleTableSet is [FAIL]"), TableSet.singleTableSet(0), ts);
            TableSet recursiveDeps = semTable.recursiveDeps(extract(sel, 0));
            assertEquals(printFail(query + ": get recursiveDeps is [FAIL]"), this.t1, recursiveDeps);
            assertEquals(printFail(query + ": get direct is [FAIL]"), this.t1, semTable.directDeps(extract(sel, 0)));
            assertEquals(printFail(query + ": numberOfTables is [FAIL]"), 1, recursiveDeps.numberOfTables());
            printOk("TestBindingSingleTablePositive is [OK],current sql = " + query);
        }
    }

    @Test
    public void testBindingSingleAliasedTablePositive() throws SQLException {
        String[] queries = {
            "select col from tabl as X",
            "select tabl.col from d.X as tabl",
            "select col from d.X as tabl",
            "select tabl.col from X as tabl",
            "select col+col from tabl as X",
            "select max(tabl.col1 + tabl.col2) from d.X as tabl",
            "select max(t.id) from t1 as t",
        };
        for (String query : queries) {
            ParseAndAnalyze stmAndsem = parseAndAnalyze(query, "d");
            SemTable semTable = stmAndsem.getSemTable();
            SQLSelectStatement sel = (SQLSelectStatement) stmAndsem.getStm();
            SQLTableSource t1 = sel.getSelect().getFirstQueryBlock().getFrom();
            TableSet ts = semTable.tableSetFor(t1);
            assertEquals(printFail(query + ": get singleTableSet is [FAIL]"), TableSet.singleTableSet(0), ts);
            TableSet recursiveDeps = semTable.recursiveDeps(extract(sel, 0));
            assertEquals(printFail(query + ": get recursiveDeps is [FAIL]"), this.t1, recursiveDeps);
            assertEquals(printFail(query + ":number of tables is wrong"), 1, recursiveDeps.numberOfTables());
            printOk("testBindingSingleAliasedTablePositive is [OK],current sql = " + query);
        }
    }

    @Test
    public void testBindingSingleTableNegative() {
        String[] queries = {
            "select foo.col from tabl",
            "select ks.tabl.col from tabl",
            "select ks.tabl.col from d.tabl",
            "select d.tabl.col from ks.tabl",
            "select foo.col from d.tabl",
            "select tabl.col from d.tabl as t",
        };
        for (String query : queries) {
            SQLStatement parse = SQLUtils.parseSingleMysqlStatement(query);
            try {
                Analyzer.analyze((SQLSelectStatement) parse, "d", new FakeSI(new HashMap<>(), null));
                fail("expected an error");
            } catch (SQLException e) {
                String message = e.getMessage();
                if (!(message.contains("symbol") && message.contains("not found"))) {
                    fail("unexpected error msg " + message);
                }
                printOk("testBindingSingleTableNegative is [OK],current sql = " + query);
            }
        }
    }

    @Test
    public void testBindingSingleAliasedTableNegative() {
        String[] queries = {
            "select tabl.col from d.tabl as t",
            "select foo.col from tabl",
            "select ks.tabl.col from tabl",
            "select ks.tabl.col from d.tabl",
            "select d.tabl.col from ks.tabl",
            "select foo.col from d.tabl",
            "select tabl.col from tabl as X",
            "select d.X.col from d.X as tabl",
            "select d.tabl.col from X as tabl",
            "select d.tabl.col from ks.X as tabl",
            "select d.tabl.col from d.X as tabl",
        };

        for (String query : queries) {
            SQLStatement parse = SQLUtils.parseSingleMysqlStatement(query);
            try {
                Map<String, Vschema.Table> map = new HashMap<>();
                map.put("t", Vschema.Table.newBuilder().build());
                Analyzer.analyze((SQLSelectStatement) parse, "d", new FakeSI(map, null));
                fail("expected an error");
            } catch (SQLException e) {
                String message = e.getMessage();
                if (!(message.contains("symbol") && message.contains("not found"))) {
                    fail("unexpected error msg " + message);
                }
                printOk(query + " testBindingSingleAliasedTableNegative" + " is [OK]");
            }
        }
    }

    @Test
    public void testBindingMultiTablePositive() throws SQLException {
        List<TestCase> testCases = new ArrayList<>();
        testCases.add(new TestCase("select t.col from t,s", t1, 1));
        testCases.add(new TestCase("select s.col from t join s", t2, 1));
        testCases.add(new TestCase("select max(t.col+s.col) from t,s", TableSet.mergeTableSets(t1, t2), 2));
        testCases.add(new TestCase("select max(t.col+s.col) from t join s", TableSet.mergeTableSets(t1, t2), 2));
        testCases.add(new TestCase("select case t.col when s.col then r.col else u.col end from t,s,r,w,u", TableSet.mergeTableSets(t1, t2, t3, t5), 4));
        testCases.add(new TestCase("select u1.a + u2.a from u1,u2", TableSet.mergeTableSets(t1, t2), 2));

        for (TestCase testCase : testCases) {
            String query = testCase.getQuery();
            ParseAndAnalyze stmAndsem = parseAndAnalyze(query, "user");
            SemTable semTable = stmAndsem.getSemTable();
            SQLSelectStatement sel = (SQLSelectStatement) stmAndsem.getStm();
            TableSet d = semTable.recursiveDeps(extract(sel, 0));
            assertEquals(query, testCase.getDeps(), d);
            assertEquals(query, testCase.getNumberOfTables(), d.numberOfTables());
            printOk("testBindingMultiTablePositive is [OK],current sql = " + query);
        }
    }

    @Test
    public void testBindingMultiAliasedTablePositive() throws SQLException {
        List<TestCase> testCases = new ArrayList<>();
        testCases.add(new TestCase("select X.col from t as X,s as S", t1, 1));
        testCases.add(new TestCase("select X.col + S.col from t as X, s as S", TableSet.mergeTableSets(t1, t2), 2));
        testCases.add(new TestCase("select max(X.col+S.col) from t as X,s as S", TableSet.mergeTableSets(t1, t2), 2));
        testCases.add(new TestCase("select max(X.col+s.col) from t as X,s", TableSet.mergeTableSets(t1, t2), 2));

        for (TestCase testCase : testCases) {
            String query = testCase.getQuery();
            ParseAndAnalyze stmAndsem = parseAndAnalyze(query, "user");
            SemTable semTable = stmAndsem.getSemTable();
            SQLSelectStatement sel = (SQLSelectStatement) stmAndsem.getStm();
            TableSet d = semTable.recursiveDeps(extract(sel, 0));
            assertEquals(query, testCase.getDeps(), d);
            assertEquals(query, testCase.getNumberOfTables(), d.numberOfTables());
            printOk("testBindingMultiAliasedTablePositive is [OK],current sql = " + query);
        }
    }

    @Test
    public void testBindingMultiTableNegative() {
    }

    @Test
    public void testBindingMultiAliasedTableNegative() {

    }

    @Test
    public void testNotUniqueTableName() {
        String[] queries = {
            "select * from t, t",
//            "select * from t, (select 1 from x) as t",
//            "select * from t join t",
//            "select * from t join (select 1 from x) as t",
        };
        for (String query : queries) {
            SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(query);
            try {
                Analyzer.analyze((SQLSelectStatement) stmt, "test", new FakeSI(new HashMap<>(), null));
                fail("expected an error");
            } catch (SQLException e) {
                String message = e.getMessage();
                if (!(message.contains("Not unique table/alias"))) {
                    fail("unexpected error msg " + message);
                }
                printOk("testNotUniqueTableName is [OK],current sql = " + query);
            }
        }
    }

    @Test
    public void testMissingTable() {
        String[] queries = {
            "select t.col from a",
        };
        for (String query : queries) {
            SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(query);
            try {
                Analyzer.analyze((SQLSelectStatement) stmt, "", new FakeSI(new HashMap<>(), null));
                fail("expected an error");
            } catch (SQLException e) {
                String message = e.getMessage();
                if (!(message.contains("symbol t.col not found"))) {
                    fail("unexpected error msg " + message);
                }
                printOk("testMissingTable is [OK],current sql = " + query);
            }
        }
    }

    @Test
    @Ignore
    public void testScoping() {
        List<TestCase> testCases = new ArrayList<>();
        testCases.add(new TestCase("select 1 from u1, u2 left join u3 on u1.a = u2.a", "symbol u1.a not found"));

        for (TestCase testCase : testCases) {
            SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(testCase.query);
            try {
                Map<String, Vschema.Table> map = new HashMap<>();
                map.put("t", Vschema.Table.newBuilder().build());
                Analyzer.analyze((SQLSelectStatement) stmt, "user", new FakeSI(map, null));
                Assert.fail("expected an error");
            } catch (SQLException e) {
                String message = e.getMessage();
                if (StringUtils.isEmpty(testCase.errorMessage)) {
                    Assert.fail();
                } else {
                    Assert.assertEquals(testCase.errorMessage, message);
                }
            }
        }
    }

    @Test
    @Ignore
    public void TestScopeForSubqueries() throws SQLException {
        TestCase[] tcases = {
            new TestCase("select t.col1, (select t.col2 from z as t) from x as t", t2),
            new TestCase("select t.col1, (select t.col2 from z) from x as t", t1),
            new TestCase("select t.col1, (select (select z.col2 from y) from z) from x as t", t2),
            new TestCase("select t.col1, (select (select y.col2 from y) from z) from x as t", none),
            new TestCase("select t.col1, (select (select (select (select w.col2 from w) from x) from y) from z) from x as t", none),
            new TestCase("select t.col1, (select id from t) from x as t", t2)
        };
        for (TestCase tc : tcases) {
            ParseAndAnalyze stmAndsem = parseAndAnalyze(tc.query, "d");
            SemTable semTable = stmAndsem.getSemTable();
            SQLSelectStatement sel = (SQLSelectStatement) stmAndsem.getStm();
            // extract the first expression from the subquery (which should be the second expression in the outer query)
            sel.getSelect().getFirstQueryBlock().getSelectList().get(1).getExpr();
        }
    }

    @Test
    public void testSubqueriesMappingWhereClause() {

    }

    @Test
    public void testSubqueriesMappingSelectExprs() {

    }

    @Test
    public void testSubqueryOrderByBinding() {

    }

    @Test
    public void testOrderByBindingTable() throws SQLException {
        List<TestCase> testCases = new ArrayList<>();
        testCases.add(new TestCase("select col from tabl order by col", t1));
        testCases.add(new TestCase("select tabl.col from d.tabl order by col", t1));
        testCases.add(new TestCase("select d.tabl.col from d.tabl order by col", t1));
        testCases.add(new TestCase("select col from tabl order by tabl.col", t1));
        testCases.add(new TestCase("select col from tabl order by d.tabl.col", t1));
        testCases.add(new TestCase("select col from tabl order by 1", t1));
        testCases.add(new TestCase("select col as c from tabl order by c", t1));
        testCases.add(new TestCase("select 1 as c from tabl order by c", none));
        testCases.add(new TestCase("select name, name from t1, t2 order by name", t2));
        testCases.add(new TestCase("(select id from t1) union (select uid from t2) order by id", TableSet.mergeTableSets(t1, t2)));
        testCases.add(new TestCase("select id from t1 union (select uid from t2) order by 1", TableSet.mergeTableSets(t1, t2)));
        testCases.add(new TestCase("select id from t1 union select uid from t2 union (select name from t) order by 1", TableSet.mergeTableSets(t1, t2, t3)));
        testCases.add(new TestCase("select a.id from t1 as a union (select uid from t2) order by 1", TableSet.mergeTableSets(t1, t2)));
        testCases.add(new TestCase("select b.id as a from t1 as b union (select uid as c from t2) order by 1", TableSet.mergeTableSets(t1, t2)));
        testCases.add(new TestCase("select a.id from t1 as a union (select uid from t2, t union (select name from t) order by 1) order by 1", TableSet.mergeTableSets(t1, t2, t4)));
        testCases.add(new TestCase("select a.id from t1 as a union (select uid from t2, t union (select name from t) order by 1) order by id", TableSet.mergeTableSets(t1, t2, t4)));
        for (TestCase testCase : testCases) {
            String query = testCase.getQuery();
            ParseAndAnalyze stmAndsem = parseAndAnalyze(query, "d");
            SemTable semTable = stmAndsem.getSemTable();
            SQLSelectStatement sel = (SQLSelectStatement) stmAndsem.getStm();
            SQLSelectQuery sqlSelectQuery = sel.getSelect().getQuery();
            SQLExpr order = null;
            if (sqlSelectQuery instanceof MySqlSelectQueryBlock) {
                order = ((MySqlSelectQueryBlock) sqlSelectQuery).getOrderBy().getItems().get(0).getExpr();
            } else if (sqlSelectQuery instanceof SQLUnionQuery) {
                order = ((SQLUnionQuery) sqlSelectQuery).getOrderBy().getItems().get(0).getExpr();
            } else {
                Assert.fail();
            }
            TableSet d = semTable.recursiveDeps(order);
            Assert.assertEquals(query, testCase.getDeps(), d);
            printOk("testOrderByBindingTable is [OK],current sql = " + query);
        }
    }

    @Test
    public void testGroupByBinding() throws SQLException {
        List<TestCase> testCases = new ArrayList<>();
        testCases.add(new TestCase("select col from tabl group by col", t1));
        testCases.add(new TestCase("select col from tabl group by tabl.col", t1));
        testCases.add(new TestCase("select col from tabl group by d.tabl.col", t1));
        testCases.add(new TestCase("select tabl.col as x from tabl group by x", t1));
        testCases.add(new TestCase("select tabl.col as x from tabl group by col", t1));
        testCases.add(new TestCase("select d.tabl.col as x from tabl group by x", t1));
        testCases.add(new TestCase("select d.tabl.col as x from tabl group by col", t1));
        testCases.add(new TestCase("select col from tabl group by 1", t1));
        testCases.add(new TestCase("select col as c from tabl group by c", t1));
        testCases.add(new TestCase("select 1 as c from tabl group by c", none));
        testCases.add(new TestCase("select t1.id from t1, t2 group by id", t1));
        testCases.add(new TestCase("select id from t, t1 group by id", t2));
        testCases.add(new TestCase("select id from t, t1 group by id", t2));
        testCases.add(new TestCase("select a.id from t as a, t1 group by id", t1));
        testCases.add(new TestCase("select a.id from t, t1 as a group by id", t2));
        for (TestCase testCase : testCases) {
            String query = testCase.getQuery();
            ParseAndAnalyze stmAndsem = parseAndAnalyze(query, "d");
            SemTable semTable = stmAndsem.getSemTable();
            SQLSelectStatement sel = (SQLSelectStatement) stmAndsem.getStm();
            SQLSelectGroupByClause grp = sel.getSelect().getFirstQueryBlock().getGroupBy();
            TableSet d = semTable.recursiveDeps(grp.getItems().get(0));
            assertEquals(query, testCase.getDeps(), d);
            printOk("testGroupByBinding is [OK],current sql = " + query);
        }
    }

    @Test
    public void testHavingBinding() throws SQLException {
        List<TestCase> testCases = new ArrayList<>();
        testCases.add(new TestCase("select col from tabl having col = 1", t1));
        testCases.add(new TestCase("select col from tabl having tabl.col = 1", t1));
        testCases.add(new TestCase("select col from tabl having d.tabl.col = 1", t1));
        testCases.add(new TestCase("select tabl.col as x from tabl having x = 1", t1));
        testCases.add(new TestCase("select tabl.col as x from tabl having col", t1));
        testCases.add(new TestCase("select col from tabl having 1 = 1", none));
        testCases.add(new TestCase("select col as c from tabl having c = 1", t1));
        testCases.add(new TestCase("select 1 as c from tabl having c = 1", none));
        testCases.add(new TestCase("select t1.id from t1, t2 having id = 1", t1));
        testCases.add(new TestCase("select t.id from t, t1 having id = 1", t1));
        testCases.add(new TestCase("select t.id, count(*) as a from t, t1 group by t.id having a = 1", TableSet.mergeTableSets(t1, t2)));
        testCases.add(new TestCase("select u2.a, u1.a from u1, u2 having u2.a = 2", t2));
        for (TestCase testCase : testCases) {
            String query = testCase.getQuery();
            ParseAndAnalyze stmAndsem = parseAndAnalyze(query, "d");
            SemTable semTable = stmAndsem.getSemTable();
            SQLSelectStatement sel = (SQLSelectStatement) stmAndsem.getStm();
            SQLExpr having = sel.getSelect().getFirstQueryBlock().getGroupBy().getHaving();
            TableSet d = semTable.recursiveDeps(having);
            assertEquals(query, testCase.getDeps(), d);
            printOk("testHavingBinding is [OK],current sql = " + query);
        }
    }

    @Test
    public void testGroupByHavingBinding() throws SQLException {
        @AllArgsConstructor
        class TmpCase {
            String query;

            TableSet groupByDeps;

            TableSet havingDeps;
        }

        List<TmpCase> testCases = new ArrayList<>();
        testCases.add(new TmpCase("select col from tabl group by col having col > 10", t1, t1));
        testCases.add(new TmpCase("select col from tabl group by tabl.col having tabl.col > 10", t1, t1));
        testCases.add(new TmpCase("select col from tabl group by d.tabl.col having d.tabl.col > 10", t1, t1));
        testCases.add(new TmpCase("select tabl.col as x from tabl group by x having x = 1000", t1, t1));
        testCases.add(new TmpCase("select tabl.col as x from tabl group by col having x = 1000", t1, t1));
        testCases.add(new TmpCase("select d.tabl.col as x from tabl group by x having x = 1000", t1, t1));
        testCases.add(new TmpCase("select d.tabl.col as x from tabl group by col having x = 1000", t1, t1));
        testCases.add(new TmpCase("select col from tabl group by 1 having col < 10000", t1, t1));
        testCases.add(new TmpCase("select col as c from tabl group by c having c < 10000", t1, t1));
        testCases.add(new TmpCase("select 1 as c from tabl group by c having c < 10000", none, none));
        testCases.add(new TmpCase("select t1.id from t1, t2 group by id having id < 10000", t1, t1));
        testCases.add(new TmpCase("select id from t, t1 group by id having id < 10000", t2, t2));
        testCases.add(new TmpCase("select id from t, t1 group by id having id < 10000", t2, t2));
        testCases.add(new TmpCase("select a.id from t as a, t1 group by id having id < 10000", t1, t1));
        testCases.add(new TmpCase("select a.id from t, t1 as a group by id having id < 10000", t2, t2));
        testCases.add(new TmpCase("select col from tabl group by col having col = 1", t1, t1));
        testCases.add(new TmpCase("select col from tabl group by tabl.col having tabl.col = 1", t1, t1));
        testCases.add(new TmpCase("select col from tabl group by d.tabl.col having d.tabl.col = 1", t1, t1));
        testCases.add(new TmpCase("select tabl.col as x from tabl group by x having x = 1", t1, t1));
        testCases.add(new TmpCase("select tabl.col as x from tabl group by col having x = 1", t1, t1));
        testCases.add(new TmpCase("select tabl.col as x from tabl group by col having col", t1, t1));
        testCases.add(new TmpCase("select tabl.col as x from tabl group by x having col", t1, t1));
        testCases.add(new TmpCase("select col from tabl group by col having 1 = 1", t1, none));
        testCases.add(new TmpCase("select col as c from tabl group by col having c = 1", t1, t1));
        testCases.add(new TmpCase("select 1 as c from tabl group by c having c = 1", none, none));
        testCases.add(new TmpCase("select t1.id from t1, t2 group by id having id = 1", t1, t1));
        testCases.add(new TmpCase("select t.id from t, t1 group by id having id = 1", t1, t1));

        for (TmpCase testCase : testCases) {
            String query = testCase.query;
            ParseAndAnalyze stmAndsem = parseAndAnalyze(query, "d");
            SemTable semTable = stmAndsem.getSemTable();
            SQLSelectStatement sel = (SQLSelectStatement) stmAndsem.getStm();
            SQLSelectGroupByClause groupBy = sel.getSelect().getFirstQueryBlock().getGroupBy();
            SQLExpr having = sel.getSelect().getFirstQueryBlock().getGroupBy().getHaving();
            TableSet groupByTableSet = semTable.recursiveDeps(groupBy.getItems().get(0));
            assertEquals(query, testCase.groupByDeps, groupByTableSet);
            TableSet havingTableSet = semTable.recursiveDeps(having);
            assertEquals(query, testCase.havingDeps, havingTableSet);
            printOk("testGroupByHavingBinding is [OK],current sql = " + query);
        }
    }

    @Test
    public void testUnionCheckFirstAndLastSelectsDeps() throws SQLException {
        String query = "select col1 from tabl1 union select col2 from tabl2";
        ParseAndAnalyze stmAndsem = parseAndAnalyze(query, "");
        SQLUnionQuery union = (SQLUnionQuery) ((SQLSelectStatement) stmAndsem.getStm()).getSelect().getQuery();
        MySqlSelectQueryBlock sel1 = (MySqlSelectQueryBlock) union.getLeft();
        MySqlSelectQueryBlock sel2 = (MySqlSelectQueryBlock) union.getRight();

        SemTable semTable = stmAndsem.getSemTable();
        TableSet ts1 = semTable.tableSetFor(sel1.getFrom());
        TableSet ts2 = semTable.tableSetFor(sel2.getFrom());
        Assert.assertEquals(t1, ts1);
        Assert.assertEquals(t2, ts2);

        TableSet d1 = semTable.recursiveDeps(extract(sel1, 0));
        TableSet d2 = semTable.recursiveDeps(extract(sel2, 0));
        Assert.assertEquals(t1, d1);
        Assert.assertEquals(t2, d2);
    }

    @Test
    public void testUnionOrderByRewrite() throws SQLException {
        String query = "select tabl1.id from tabl1 union select 1 order by 1";
        ParseAndAnalyze stmAndsem = parseAndAnalyze(query, "");

        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement("select tabl1.id from tabl1 union select 1 order by id");
        Assert.assertEquals(SQLUtils.toSQLString(sqlStatement), SQLUtils.toSQLString(stmAndsem.getStm()));
    }

    @Test
    public void testInvalidQueries() {
        TestCase[] tcases = {
            new TestCase("select t1.id, t1.col1 from t1 union select t2.uid from t2", "The used SELECT statements have a different number of columns"),
            new TestCase("select t1.id from t1 union select t2.uid, t2.price from t2", "The used SELECT statements have a different number of columns"),
            new TestCase("select t1.id from t1 union select t2.uid, t2.price from t2", "The used SELECT statements have a different number of columns"),
            new TestCase("(select 1,2 union select 3,4) union (select 5,6 union select 7)", "The used SELECT statements have a different number of columns"),
            new TestCase("select id from a union select 3 order by a.id", "Table 'a' from one of the SELECTs cannot be used in global ORDER clause"),
            new TestCase("select a.id, b.id from a, b union select 1, 2 order by id", "Column 'id' in field list is ambiguous"),
//            new TestCase("select sql_calc_found_rows id from a union select 1 limit 109", "SQL_CALC_FOUND_ROWS not supported with union"),
//            new TestCase("select * from (select sql_calc_found_rows id from a) as t", "Incorrect usage/placement of 'SQL_CALC_FOUND_ROWS'"),
//            new TestCase("select (select sql_calc_found_rows id from a) as t", "Incorrect usage/placement of 'SQL_CALC_FOUND_ROWS'"),
        };
        for (TestCase tcase : tcases) {
            SQLStatement parse = SQLUtils.parseSingleMysqlStatement(tcase.query);
            try {
                Analyzer.analyze((SQLSelectStatement) parse, "dbName", fakeSchemaInfo());
                Assert.fail("expected an error");
            } catch (SQLException e) {
                String message = e.getMessage();
                Assert.assertEquals(tcase.errorMessage, message);
                printOk("testInvalidQueries is [ok]current sql = " + tcase.query);
            }
        }
    }

    @Test
    public void testUnionWithOrderBy() throws SQLException {
        String query = "select col1 from tabl1 union (select col2 from tabl2) order by 1";
        ParseAndAnalyze stmAndsem = parseAndAnalyze(query, "");

        SQLUnionQuery union = (SQLUnionQuery) ((SQLSelectStatement) stmAndsem.getStm()).getSelect().getQuery();
        MySqlSelectQueryBlock sel1 = (MySqlSelectQueryBlock) union.getLeft();
        MySqlSelectQueryBlock sel2 = (MySqlSelectQueryBlock) union.getRight();

        SemTable semTable = stmAndsem.getSemTable();
        TableSet ts1 = semTable.tableSetFor(sel1.getFrom());
        TableSet ts2 = semTable.tableSetFor(sel2.getFrom());
        Assert.assertEquals(t1, ts1);
        Assert.assertEquals(t2, ts2);

        TableSet d1 = semTable.recursiveDeps(extract(sel1, 0));
        TableSet d2 = semTable.recursiveDeps(extract(sel2, 0));
        Assert.assertEquals(t1, d1);
        Assert.assertEquals(t2, d2);
    }

    @Test
    public void testScopingWDerivedTables() {
        TestCase[] tcases = {
            new TestCase("select id from (select x as id from user) as t", null, t1, t2),
            new TestCase("select id from (select foo as id from user) as t", null, t1, t2),
            new TestCase("select id from (select foo as id from (select x as foo from user) as c) as t", null, t1, t3),
            new TestCase("select t.id from (select foo as id from user) as t", null, t1, t2),
            new TestCase("select t.id2 from (select foo as id from user) as t", "symbol t.id2 not found", null, null),
            new TestCase("select id from (select 42 as id) as t", null, t0, t2),
            new TestCase("select t.id from (select 42 as id) as t", null, t0, t2),
            new TestCase("select ks.t.id from (select 42 as id) as t", "symbol ks.t.id not found", null, null),
            new TestCase("select * from (select id, id from user) as t", "Duplicate column name 'id'", null, null),
//            new TestCase("select t.baz = 1 from (select id as baz from user) as t", null, t2, t1),
//            new TestCase("select t.id from (select * from user, music) as t", null, t3, TableSet.mergeTableSets(t1, t2)),
//            new TestCase("select t.id from (select * from user, music) as t order by t.id", null, t3, TableSet.mergeTableSets(t1, t2)),
//            new TestCase("select t.id from (select * from user) as t join user as u on t.id = u.id", null, t2, t1),
//            new TestCase("select t.col1 from t3 ua join (select t1.id, t1.col1 from t1 join t2) as t", null, t4, t2),
            new TestCase("select uu.test from (select id from t1) uu", "symbol uu.test not found", null, null),
            new TestCase("select uu.id from (select id as col from t1) uu", "symbol uu.id not found", null, null),
//            new TestCase("select uu.id from (select id from t1) as uu where exists (select * from t2 as uu where uu.id = uu.uid)", null, t2, t1),
            new TestCase("select 1 from user uu where exists (select 1 from user where exists (select 1 from (select 1 from t1) uu where uu.user_id = uu.id))", null, t0, t0)
        };
        Map<String, Vschema.Table> tableMap = new HashMap<>();
        tableMap.put("t", null);
        Map<String, Vschema.ColumnVindex> vindexTables = new HashMap<>();
        for (TestCase query : tcases) {
            SQLSelectStatement sel = (SQLSelectStatement) SQLUtils.parseSingleMysqlStatement(query.query);
            try {
                SemTable st = Analyzer.analyze(sel, "user", new FakeSI(tableMap, vindexTables));
                st.recursiveDeps(extract(sel, 0));
                assertEquals("testScopingWDerivedTables is [FAIL]current sql = " + query.query, query.recursiveExpectation, st.recursiveDeps(extract(sel, 0)));
                assertEquals("testScopingWDerivedTables is [FAIL]current sql = " + query.query, query.expectation, st.directDeps(extract(sel, 0)));
            } catch (SQLException e) {
                if (query.errorMessage == null) {
                    System.out.println(printFail("testScopingWDerivedTables is [FAIL]current sql = " + query.query + " , error: " + e.getMessage()));
                    fail();
                }
                Assert.assertEquals(query.query + " is  [FAIL]", query.errorMessage, e.getMessage());
            }
            printOk("testScopingWDerivedTables is [ok]current sql = " + query.query);
        }
    }

    @Test
    public void testDerivedTablesOrderClause() {

    }

    @Test
    public void testScopingWComplexDerivedTables() {

    }

   /* @Test
    public void testScopingWVindexTables() {

        @AllArgsConstructor
        class TmpCase {
            String query;

            String erroMsg;

            TableSet recursiveExpectation;

            TableSet expectation;
        }

        TmpCase[] tcases = {
            new TmpCase("select id from user_index where id = 1", null, t1, t1),
            new TmpCase("select u.id + t.id from t as t join user_index as u where u.id = 1 and u.id = t.id", null, TableSet.mergeTableSets(t1, t2), TableSet.mergeTableSets(t1, t2)),
        };
        for (TmpCase query : tcases) {
            SQLSelectStatement sel = (SQLSelectStatement) SQLUtils.parseSingleMysqlStatement(query.query);
            Map<String, Vschema.Table> tableMap = new HashMap<>();
            tableMap.put("t", null);
            Map<String, Vschema.ColumnVindex> vindexTables = new HashMap<>();
            vindexTables.put("user_index", Vschema.ColumnVindex.newBuilder().setName("hash").build());
            try {
                SemTable st = Analyzer.analyze(sel, "user", new FakeSI(tableMap, vindexTables));
                assertEquals("testScopingWVindexTables is [[FAIL]current sql = " + query.query, query.recursiveExpectation, st.recursiveDeps(extract(sel, 0)));
                assertEquals("testScopingWVindexTables is [[FAIL]current sql = " + query.query, query.expectation, st.directDeps(extract(sel, 0)));
            } catch (SQLException e) {
                if (!query.erroMsg.equals("") && !e.getMessage().equals(query.erroMsg)) {
                    fail("unexpect error : current sql =" + query.query);
                }
            }
        }
    }*/

    private ParseAndAnalyze parseAndAnalyze(String query, String dbName) throws SQLException {
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(query);
        SemTable semTable = Analyzer.analyze((SQLSelectStatement) stmt, dbName, fakeSchemaInfo());
        return new ParseAndAnalyze(stmt, semTable);
    }

    private FakeSI fakeSchemaInfo() {
//        Vschema.ColumnVindex cols1 = Vschema.ColumnVindex.newBuilder().setName("hash").setColumn("id").build();
//        Vschema.ColumnVindex cols2 = Vschema.ColumnVindex.newBuilder().setName("hash").setColumn("uid").build();
//        Vschema.ColumnVindex cols3 = Vschema.ColumnVindex.newBuilder().setName("hash").setColumn("name").build();

        Map<String, Vschema.Table> tables = new HashMap<>();
//        tables.put("t", Vschema.Table.newBuilder().setColumnListAuthoritative(false).build());
//        tables.put("t1", Vschema.Table.newBuilder().setColumnListAuthoritative(true).addColumnVindexes(cols1).build());
//        tables.put("t2", Vschema.Table.newBuilder().setColumnListAuthoritative(true).addColumnVindexes(cols2).addColumnVindexes(cols3).build());

        Vschema.Column column1 = Vschema.Column.newBuilder().setName("id").setType(Query.Type.INT64).build();
        Vschema.Column column2 = Vschema.Column.newBuilder().setName("uid").setType(Query.Type.INT64).build();
        Vschema.Column column3 = Vschema.Column.newBuilder().setName("name").setType(Query.Type.VARCHAR).build();
        tables.put("t", Vschema.Table.newBuilder().build());
        tables.put("t1", Vschema.Table.newBuilder().setColumnListAuthoritative(true).addColumns(column1).build());
        tables.put("t2", Vschema.Table.newBuilder().setColumnListAuthoritative(true).addColumns(column2).addColumns(column3).build());

        return new FakeSI(tables, null);
    }

    @Data
    @AllArgsConstructor
    private static class ParseAndAnalyze {
        private SQLStatement stm;

        private SemTable semTable;
    }

    @Data
    @AllArgsConstructor
    private static class TestCase {
        String query;

        String errorMessage;

        TableSet recursiveExpectation;

        TableSet expectation;

        TableSet deps;

        int numberOfTables;

        TestCase(String query, TableSet deps) {
            this.query = query;
            this.deps = deps;
        }

        TestCase(String query, TableSet deps, int numberOfTables) {
            this.query = query;
            this.deps = deps;
            this.numberOfTables = numberOfTables;
        }

        TestCase(String query, String errorMessage, TableSet recursiveExpectation, TableSet expectation) {
            this.query = query;
            this.errorMessage = errorMessage;
            this.recursiveExpectation = recursiveExpectation;
            this.expectation = expectation;
        }

        public TestCase(String query, String errorMessage) {
            this.query = query;
            this.errorMessage = errorMessage;
        }
    }
}
