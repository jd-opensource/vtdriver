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

package com.jd.jdbc.vitess.parser.test;

import com.jd.jdbc.planbuilder.RoutePlan;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQueryBlock;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.repository.SchemaRepository;
import com.jd.jdbc.sqlparser.stat.TableStat;
import com.jd.jdbc.sqlparser.utils.JdbcConstants;
import com.jd.jdbc.sqlparser.visitor.ParameterizedOutputVisitorUtils;
import com.jd.jdbc.sqlparser.visitor.SchemaStatVisitor;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

public class SelectTest extends TestSuite {

    @Test
    public void testComment() {
        String sql = " select distinct id, name, a, count(*) from tb where ks.tb.id is null and tb.id is not null and id = 1 and name = 'abc' or age = 3 and email = null";

    }

    @Test
    public void testLiteral() {
        String sql = "select 1, 1.2, 'cdc'";
        SQLSelectStatement selectStatement = (SQLSelectStatement) SQLUtils.parseSingleMysqlStatement(sql);
        System.out.println("selectStatement = " + selectStatement);
    }

    @Test
    public void testSomething() {
        SQLExpr customer = SQLUtils.toSQLExpr("c.id = 1");
        SQLExprTableSource sqlExprTableSource = new SQLExprTableSource(customer);
        System.out.println("sqlExprTableSource = " + sqlExprTableSource);

        String sql = "select * from test limit 10;";
        SQLSelectStatement selectStatement = (SQLSelectStatement) SQLUtils.parseSingleMysqlStatement(sql);
        System.out.println("selectStatement = " + selectStatement);
    }

    @Test
    public void testAnd() {
        String sql = "select distinct id, name, a, count(*) from tb where ks.tb.id is null and tb.id is not null and id = 1 and name = 'abc' or age = 3 and email = null";
        SQLSelectStatement selectStatement = (SQLSelectStatement) SQLUtils.parseSingleMysqlStatement(sql);

        SQLIdentifierExpr newIdentifier = new SQLIdentifierExpr("price");
        SQLSelectQueryBlock select = selectStatement.getSelect().getQueryBlock();
        select.addSelectItem(newIdentifier);

        System.out.println(SQLUtils.toMySqlString(selectStatement.getSelect().getQueryBlock(), SQLUtils.NOT_FORMAT_OPTION));

        SQLExpr where = selectStatement.getSelect().getQueryBlock().getWhere();
        List<SQLExpr> filters = splitAndExpression(new ArrayList<>(), where);
        filters.forEach(System.out::println);
    }

    @Test
    public void testGenerate() throws SQLException {
        String sql =
            "select ks.tb.id, tb.name, a, ks.tb.count(*), tb.count(*) from ks.tb where ks.tb.id is null and tb.id is not null and id = 1 and tb.name = 'abc' or ks.tb.age = 3 and ks.tb.email = null group by ks.tb.a, tb.id, name order by ks.tb.a, tb.id, name";
        SQLSelectStatement selectStatement = (SQLSelectStatement) SQLUtils.parseSingleMysqlStatement(sql);
        MySqlSelectQueryBlock select = (MySqlSelectQueryBlock) selectStatement.getSelect().getQueryBlock();
        RoutePlan routePlan = new RoutePlan(null);
        MySqlSelectQueryBlock query = routePlan.generateQuery(select, false);
        System.out.println("query = " + query);
    }

    @Test
    public void testGroupBy() {
        // String sql = "select t.email from customer t group by t.email";
        String sql = "select 1 from customer t group by 123";
        SQLSelectStatement selectStatement = (SQLSelectStatement) SQLUtils.parseSingleMysqlStatement(sql);

        SQLSelectGroupByClause groupBy = selectStatement.getSelect().getQueryBlock().getGroupBy();
        System.out.println("groupBy = " + groupBy);
    }

    @Test
    public void testVisit() {
        String sql =
            "select *, t.*, id, t.name as nnn, count(*), count(t.*) from tb as t where t.id = 1 and t.name = 2 or t.age = 3 and t.price in (1,2,3,4) group by t.email, addr having t.addr = 'abc' order by t.id, name";
        SQLSelectStatement selectStatement = (SQLSelectStatement) SQLUtils.parseSingleMysqlStatement(sql);
        SQLSelectQueryBlock selectQueryBlock = selectStatement.getSelect().getQueryBlock();

        SchemaStatVisitor visitor = new SchemaStatVisitor();
        SQLExpr where = selectQueryBlock.getWhere();
        where.accept(visitor);
        Collection<TableStat.Column> columns = visitor.getColumns();
        columns.forEach(System.out::println);

        SQLSelectGroupByClause groupBy = selectQueryBlock.getGroupBy();
        System.out.println("groupBy = " + groupBy);
        SQLOrderBy orderBy = selectQueryBlock.getOrderBy();
        System.out.println("orderBy = " + orderBy);
    }

    @Test
    public void testRepository() {
        String sql = "select t.id, t.name from tb as t where t.id = 1 and t.name = 2 or t.age = 3";
        SQLSelectStatement selectStatement = (SQLSelectStatement) SQLUtils.parseSingleMysqlStatement(sql);
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        repository.resolve(selectStatement);

        SQLSelectQueryBlock selectQueryBlock = selectStatement.getSelect().getQueryBlock();
        SQLExpr where = selectQueryBlock.getWhere();
        System.out.println("where = " + where);
    }

    @Test
    public void testOne() throws SQLException {
        Connection conn = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        Statement stmt = conn.createStatement();

        String sql = "SELECT count( distinct email ) FROM user_extra";
        stmt.executeQuery(sql);
    }

    @Test
    public void testLock() {
        String sql = "select get_lock()";
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        System.out.println("stmt = " + stmt);
    }

    @Test
    public void testWeightString() {
        String sql = "select count(*), a, textcol1, b, weight_string(n.textcol1) from user n group by a, textcol1, b order by a asc, textcol1 asc, b asc";
        SQLSelectStatement selectStatement = (SQLSelectStatement) SQLUtils.parseSingleMysqlStatement(sql);
        selectStatement.getSelect();
    }

    @Test
    public void testParameters() {
        final String dbType = JdbcConstants.MYSQL;

        List<Object> outParameters = new ArrayList<Object>();

        String sql = "select * from t where id = 101 and age = 102 or name = 'wenshao'";
//        SQLSelectStatement selectStatement = (SQLSelectStatement) SQLUtils.parseSingleMysqlStatement(sql);
//        SQLSelectQueryBlock selectQueryBlock = selectStatement.getSelect().getQueryBlock();

        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, dbType, outParameters);

//        ParameterizedVisitor visitor = new MySqlExportParameterVisitor();
//        MySqlSelectQueryBlock mySqlSelectQueryBlock = (MySqlSelectQueryBlock)selectQueryBlock;

//        mySqlSelectQueryBlock.accept(visitor);
//        int psql = visitor.getReplaceCount();
        System.out.println(psql);
    }

    private List<SQLExpr> splitAndExpression(List<SQLExpr> filters, SQLExpr node) {
        if (node == null) {
            return filters;
        }
        if (node instanceof SQLBinaryOpExpr) {
            if (((SQLBinaryOpExpr) node).getOperator().equals(SQLBinaryOperator.BooleanAnd)) {
                filters = splitAndExpression(filters, ((SQLBinaryOpExpr) node).getLeft());
                return splitAndExpression(filters, ((SQLBinaryOpExpr) node).getRight());
            }
        }
        filters.add(node);
        return filters;
    }

}
