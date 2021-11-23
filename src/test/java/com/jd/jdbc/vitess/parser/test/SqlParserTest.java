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

import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLJoinTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import com.jd.jdbc.sqlparser.dialect.mysql.custom.VitessSchemaASTVisitorAdapter;
import com.jd.jdbc.sqlparser.dialect.mysql.parser.MySqlStatementParser;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.MySqlOutputVisitor;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.jd.jdbc.sqlparser.utils.JdbcConstants;
import com.jd.jdbc.sqlparser.visitor.SQLASTOutputVisitor;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class SqlParserTest {

    int times = 1;

    private String sql;

    @Before
    public void setUp() throws Exception {
        sql = "SELECT * FROM T";
        sql = "SELECT ID, NAME, AGE FROM USER WHERE ID = ?";
        sql = "insert into customer.custom(a, b, c) values(?, ?, ?)";
        sql = "delete FROM customer.T";

        sql = "select * from a where ab = ab union select * from b where ac = ac";

//        sql = Utils.readFromResource("benchmark/sql/ob_sql.txt");
    }

    @Test
    public void test_pert() throws Exception {
        for (int i = 0; i < times; ++i) {
            perfMySql(sql);
        }
    }

    long perfMySql(String sql) {
        long startYGC = TestUtils.getYoungGC();
        long startYGCTime = TestUtils.getYoungGCTime();
        long startFGC = TestUtils.getFullGC();
        long startMillis = System.currentTimeMillis();
        for (int i = 0; i < times * times; ++i) {
            String result = execMySql(sql);
            System.out.println("res:" + result);
        }
        long millis = System.currentTimeMillis() - startMillis;

        long ygc = TestUtils.getYoungGC() - startYGC;
        long ygct = TestUtils.getYoungGCTime() - startYGCTime;
        long fgc = TestUtils.getFullGC() - startFGC;

        System.out.println("MySql\t" + millis + ", ygc " + ygc + ", ygct " + ygct + ", fgc " + fgc);
        return millis;
    }

    private String execMySql(String sql) {
        List<Object> params = new ArrayList<>();
        params.add(1);
        StringBuilder out = new StringBuilder();
        VitessSchemaASTVisitorAdapter vitessSchemaASTVisitorAdapter = new VitessSchemaASTVisitorAdapter();
        MySqlOutputVisitor visitor = new MySqlOutputVisitor(out, false);
        visitor.setInputParameters(params);
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        for (SQLStatement statement : statementList) {
            statement.accept(vitessSchemaASTVisitorAdapter);
            statement.accept(visitor);
            System.out.println(vitessSchemaASTVisitorAdapter.getSqlType());
            visitor.println();
        }
        return out.toString();
    }

    @Test
    public void testCount() {
        // String sql = "select count(*), col from unsharded where 1 != 1 and 2 != 2 group by a having b=3 union select sum(a) from b where 3 = 3 and 4 = 4";
        // String sql = "select count(*)  from customer where 1 = 1 and 2=2 or 3=3 group by customer_id order by customer_id";
        String sql = "select * from a,(b,c)";

        SQLSelectStatement sqlStatement = (SQLSelectStatement) SQLUtils.parseSingleMysqlStatement(sql);
        SQLTableSource from = sqlStatement.getSelect().getQueryBlock().getFrom();
        SQLJoinTableSource joinTableSource = (SQLJoinTableSource) from;
        System.out.println("joinTableSource = " + joinTableSource);

        StringBuilder sb = new StringBuilder();
        SQLASTOutputVisitor outputVisitor = SQLUtils.createOutputVisitor(sb, JdbcConstants.MYSQL);
        sqlStatement.accept(outputVisitor);

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        sqlStatement.accept(visitor);

        visitor.getColumns().forEach(System.out::println);
        System.out.println();
        visitor.getTables().forEach((name, tableStat) -> System.out.println(name));
        System.out.println();
        visitor.getParameters().forEach(System.out::println);
        System.out.println();
        visitor.getRelationships().forEach(System.out::println);
    }

    @Test
    public void testBigInteger() {
        BigInteger value = new BigInteger("1111111111111111111111111");
        System.out.println(value.longValue());
    }
}
