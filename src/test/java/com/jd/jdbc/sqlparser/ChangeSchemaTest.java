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

package com.jd.jdbc.sqlparser;

import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtChangeSchemaVisitor;
import com.jd.jdbc.util.KeyspaceUtil;
import com.jd.jdbc.vitess.VitessConnection;
import com.jd.jdbc.vitess.VitessStatement;
import io.netty.util.internal.StringUtil;
import java.sql.SQLException;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import testsuite.TestSuite;
import static testsuite.internal.TestSuiteShardSpec.TWO_SHARDS;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ChangeSchemaTest extends TestSuite {
    private final VtChangeSchemaVisitor visitor = new VtChangeSchemaVisitor(StringUtil.EMPTY_STRING);

    protected VitessConnection vitessConnection;

    protected void init() throws SQLException {
        vitessConnection = (VitessConnection) getConnection(Driver.of(TWO_SHARDS));
    }

    protected void clearAll() throws SQLException {
        vitessConnection.close();
    }

    @Test
    public void case01() {
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement("select ks.tb.col, tb.col, col");
        checkSql(stmt, "select vt_ks.tb.col, tb.col, col");

        stmt = SQLUtils.parseSingleMysqlStatement("select ks.tb.*, tb.*, *");
        checkSql(stmt, "select vt_ks.tb.*, tb.*, *");

        stmt = SQLUtils.parseSingleMysqlStatement("select ks.tb.col as t1, tb.col as t2, col as t3");
        checkSql(stmt, "select vt_ks.tb.col as t1, tb.col as t2, col as t3");

        stmt = SQLUtils.parseSingleMysqlStatement("select count(ks.tb.*), count(tb.*), count(*)");
        checkSql(stmt, "select count(vt_ks.tb.*), count(tb.*), count(*)");

        stmt = SQLUtils.parseSingleMysqlStatement("select substr(ks.tb.col, 1, 3), trim(ks.tb.col), concat(ks.tb.col1, ks.tb.co2)");
        checkSql(stmt, "select substr(vt_ks.tb.col, 1, 3), trim(vt_ks.tb.col), concat(vt_ks.tb.col1, vt_ks.tb.co2)");

        stmt = SQLUtils.parseSingleMysqlStatement("select ks.tb.col > 1, 1 < ks.tb.col, ks.tb.col1 = ks.tb.col2");
        checkSql(stmt, "select vt_ks.tb.col > 1, 1 < vt_ks.tb.col, vt_ks.tb.col1 = vt_ks.tb.col2");

        stmt = SQLUtils.parseSingleMysqlStatement("select ks.tb.col > sum(ks.tb.col), avg(ks.tb.col) < ks.tb.col, ks.tb.col1 = trim(ks.tb.col2)");
        checkSql(stmt, "select vt_ks.tb.col > sum(vt_ks.tb.col), avg(vt_ks.tb.col) < vt_ks.tb.col, vt_ks.tb.col1 = trim(vt_ks.tb.col2)");

        stmt = SQLUtils.parseSingleMysqlStatement("select case when ks.tb.col > 1 then ks.tb.col when ks.tb.col = min(ks.tb.col) then 'xxx' else ucase(ks.tb.col) end");
        checkSql(stmt, "select case  when vt_ks.tb.col > 1 then vt_ks.tb.col when vt_ks.tb.col = min(vt_ks.tb.col) then 'xxx' else ucase(vt_ks.tb.col) end");
    }

    @Test
    public void case02() {
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement("insert into ks.tb(ks.tb.col1, ks.tb.col2, ks.tb.col3)");
        checkSql(stmt, "insert into vt_ks.tb (vt_ks.tb.col1, vt_ks.tb.col2, vt_ks.tb.col3)");

        stmt = SQLUtils.parseSingleMysqlStatement("insert into ks.tb values (ks.tb.col1, ks.tb.col2, ks.tb.col3)");
        checkSql(stmt, "insert into vt_ks.tb values (vt_ks.tb.col1, vt_ks.tb.col2, vt_ks.tb.col3)");

        stmt = SQLUtils.parseSingleMysqlStatement("insert into ks.tb(ks.tb.col1) select ks.tb.col1");
        checkSql(stmt, "insert into vt_ks.tb (vt_ks.tb.col1) select vt_ks.tb.col1");

        stmt = SQLUtils.parseSingleMysqlStatement("insert into ks.tb(ks.tb.col1) values ( (select ks.tb.col1), (select ks.tb.col1) )");
        checkSql(stmt, "insert into vt_ks.tb (vt_ks.tb.col1) values ( (select vt_ks.tb.col1) ,  (select vt_ks.tb.col1) )");

        stmt = SQLUtils.parseSingleMysqlStatement("insert into ks.tb(ks.tb.col1) values (1) on duplicate key update ks.tb.col1 = 1, ks.tb.col1 = ks.tb.col2");
        checkSql(stmt, "insert into vt_ks.tb (vt_ks.tb.col1) values (1) on duplicate key update vt_ks.tb.col1 = 1, vt_ks.tb.col1 = vt_ks.tb.col2");
    }

    @Test
    public void case03() {
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement("replace into ks.tb(ks.tb.col1, ks.tb.col2, ks.tb.col3)");
        checkSql(stmt, "replace into vt_ks.tb (vt_ks.tb.col1, vt_ks.tb.col2, vt_ks.tb.col3)");

        stmt = SQLUtils.parseSingleMysqlStatement("replace into ks.tb values (ks.tb.col1, ks.tb.col2, ks.tb.col3)");
        checkSql(stmt, "replace into vt_ks.tb values (vt_ks.tb.col1, vt_ks.tb.col2, vt_ks.tb.col3)");

        stmt = SQLUtils.parseSingleMysqlStatement("replace into ks.tb(ks.tb.col1) select ks.tb.col1");
        checkSql(stmt, "replace into vt_ks.tb (vt_ks.tb.col1) select vt_ks.tb.col1");

        stmt = SQLUtils.parseSingleMysqlStatement("replace into ks.tb(ks.tb.col1) values ( (select ks.tb.col1), (select ks.tb.col1) )");
        checkSql(stmt, "replace into vt_ks.tb (vt_ks.tb.col1) values ( (select vt_ks.tb.col1) ,  (select vt_ks.tb.col1) )");

        stmt = SQLUtils.parseSingleMysqlStatement("replace into ks.tb set ks.tb.col1 = 1, ks.tb.col2 = 2");
        checkSql(stmt, "replace into vt_ks.tb (vt_ks.tb.col1, vt_ks.tb.col2) values (1, 2)");
    }

    @Test
    public void case04() {
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement("update ks.tb set ks.tb.col1 = ks.tb.col2, ks.tb.col1 = 1");
        checkSql(stmt, "update vt_ks.tb set vt_ks.tb.col1 = vt_ks.tb.col2, vt_ks.tb.col1 = 1");

        stmt = SQLUtils.parseSingleMysqlStatement("update ks.tb set ks.tb.col1 = replace(ks.tb.col1, ks.tb.col2, ks.tb.col3)");
        checkSql(stmt, "update vt_ks.tb set vt_ks.tb.col1 = replace(vt_ks.tb.col1, vt_ks.tb.col2, vt_ks.tb.col3)");

        stmt = SQLUtils.parseSingleMysqlStatement("update ks.tb1, ks.tb2 set ks.tb1.col1 = ks.tb2.col2 where ks.tb1.key = ks.tb2.key");
        checkSql(stmt, "update vt_ks.tb1, vt_ks.tb2 set vt_ks.tb1.col1 = vt_ks.tb2.col2 where vt_ks.tb1.key = vt_ks.tb2.key");

        stmt = SQLUtils.parseSingleMysqlStatement("update ks.tb1 inner join ks.tb2 on ks.tb1.key = ks.tb2.key set ks.tb1.col = 1");
        checkSql(stmt, "update vt_ks.tb1 inner join vt_ks.tb2 on vt_ks.tb1.key = vt_ks.tb2.key set vt_ks.tb1.col = 1");

        stmt = SQLUtils.parseSingleMysqlStatement("update ks.tb1 left join ks.tb2 on ks.tb1.key = ks.tb2.key set ks.tb1.col = 1");
        checkSql(stmt, "update vt_ks.tb1 left join vt_ks.tb2 on vt_ks.tb1.key = vt_ks.tb2.key set vt_ks.tb1.col = 1");
    }

    @Test
    public void case05() {
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement("delete from ks.tb");
        checkSql(stmt, "delete from vt_ks.tb");

        stmt = SQLUtils.parseSingleMysqlStatement("delete ks.tb1, ks.tb2 from ks.tb1, ks.tb2 where ks.tb1.key = ks.tb2.key");
        checkSql(stmt, "delete vt_ks.tb1, vt_ks.tb2 from vt_ks.tb1, vt_ks.tb2 where vt_ks.tb1.key = vt_ks.tb2.key");

        stmt = SQLUtils.parseSingleMysqlStatement("delete ks.tb1, ks.tb2 from ks.tb1 inner join ks.tb2 on ks.tb1.key = ks.tb2.key");
        checkSql(stmt, "delete vt_ks.tb1, vt_ks.tb2 from vt_ks.tb1 inner join vt_ks.tb2 on vt_ks.tb1.key = vt_ks.tb2.key");

        stmt = SQLUtils.parseSingleMysqlStatement("delete ks.tb1, ks.tb2 from ks.tb1 left join ks.tb2 on ks.tb1.key = ks.tb2.key");
        checkSql(stmt, "delete vt_ks.tb1, vt_ks.tb2 from vt_ks.tb1 left join vt_ks.tb2 on vt_ks.tb1.key = vt_ks.tb2.key");
    }

    @Test
    public void case06() {
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement("select * from tb where ks.tb.col1 = 1 and 1 = ks.tb.col1 or ks.tb.col1 = ks.tb.col2");
        checkSql(stmt, "select * from tb where vt_ks.tb.col1 = 1 and 1 = vt_ks.tb.col1 or vt_ks.tb.col1 = vt_ks.tb.col2");

        stmt = SQLUtils.parseSingleMysqlStatement("select * from tb where ks.tb.col1 between ks.tb.col2 and ks.tb.col3");
        checkSql(stmt, "select * from tb where vt_ks.tb.col1 between vt_ks.tb.col2 and vt_ks.tb.col3");

        stmt = SQLUtils.parseSingleMysqlStatement("select * from tb where ks.tb.col1 in (ks.tb.col2, ks.tb.col3, ks.tb.col4)");
        checkSql(stmt, "select * from tb where vt_ks.tb.col1 in (vt_ks.tb.col2, vt_ks.tb.col3, vt_ks.tb.col4)");

        stmt = SQLUtils.parseSingleMysqlStatement("select * from tb where ks.tb.col1 is null");
        checkSql(stmt, "select * from tb where vt_ks.tb.col1 is null");
    }

    @Test
    public void case07() {
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement("select * from tb order by ks.tb.col1, ks.tb.col2 asc, ks.tb.col3 desc");
        checkSql(stmt, "select * from tb order by vt_ks.tb.col1, vt_ks.tb.col2 asc, vt_ks.tb.col3 desc");

        stmt = SQLUtils.parseSingleMysqlStatement("update tb set col = 1 order by ks.tb.col");
        checkSql(stmt, "update tb set col = 1 order by vt_ks.tb.col");

        stmt = SQLUtils.parseSingleMysqlStatement("delete from tb order by ks.tb.col");
        checkSql(stmt, "delete from tb order by vt_ks.tb.col");
    }

    @Test
    public void case08() {
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement("select * from tb group by ks.tb.col1, ks.tb.col2, ks.tb.col3");
        checkSql(stmt, "select * from tb group by vt_ks.tb.col1, vt_ks.tb.col2, vt_ks.tb.col3");

        stmt = SQLUtils.parseSingleMysqlStatement("select * from tb group by ks.tb.col1, ks.tb.col2, ks.tb.col3 having ks.tb.col1 = 2");
        checkSql(stmt, "select * from tb group by vt_ks.tb.col1, vt_ks.tb.col2, vt_ks.tb.col3 having vt_ks.tb.col1 = 2");
    }

    @Test
    public void case09() {
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement("select 1");
        checkSql(stmt, "select 1");

        stmt = SQLUtils.parseSingleMysqlStatement("select 'x' from dual");
        checkSql(stmt, "select 'x' from dual");

        stmt = SQLUtils.parseSingleMysqlStatement("select table_schema from information_schema.tables");
        checkSql(stmt, "select table_schema from information_schema.tables");

        stmt = SQLUtils.parseSingleMysqlStatement("select table_schema from information_schema.tables where information_schema.tables.table_schema = 'vt_vtdriver2'");
        checkSql(stmt, "select table_schema from information_schema.tables where information_schema.tables.table_schema = 'vt_vtdriver2'");
    }

    @Test
    public void case10_ParseStatements() throws SQLException {
        init();
        String defaultKeyspace = vitessConnection.getDefaultKeyspace();

        // unsupported
        try (VitessStatement vitessStatement = new VitessStatement(vitessConnection, null)) {
            vitessStatement.parseStatements("create table tb (id int)");
        } catch (SQLException e) {
            Assert.assertEquals("not supported sql: create table tb ( id int )", e.getMessage());
        }

        // no database expr in SQL - no replace, defaultKeyspace is 'vt_' + defualt
        try (VitessStatement vitessStatement = (VitessStatement) vitessConnection.createStatement()) {
            VitessStatement.ParseResult parseResult = vitessStatement.parseStatements("select * from t");
            Assert.assertEquals(defaultKeyspace, parseResult.getSchema());
            Assert.assertEquals("select * from t", SQLUtils.toMySqlString(parseResult.getStatement(), SQLUtils.NOT_FORMAT_OPTION));
        }

        // database expr in SQL - replace database expr, defaultKeyspace is 'vt_' + defualt
        try (VitessStatement vitessStatement = (VitessStatement) vitessConnection.createStatement()) {
            VitessStatement.ParseResult parseResult = vitessStatement.parseStatements("select * from " + defaultKeyspace + ".t");
            Assert.assertEquals(defaultKeyspace, parseResult.getSchema());
            Assert.assertEquals("select * from " + KeyspaceUtil.getRealSchema(defaultKeyspace) + ".t", SQLUtils.toMySqlString(parseResult.getStatement(), SQLUtils.NOT_FORMAT_OPTION));
        }

        // different database expr no watch
        try (VitessStatement vitessStatement = (VitessStatement) vitessConnection.createStatement()) {
            vitessStatement.parseStatements("select * from ks.tb");
        } catch (SQLException e) {
            Assert.assertEquals("unexpected keyspace (ks) in sql: select * from ks.tb", e.getMessage());
        }

        clearAll();
    }

    private void checkSql(SQLStatement actualStmt, String execptSql) {
        actualStmt.accept(visitor);
        String actualSql = SQLUtils.toMySqlString(actualStmt, SQLUtils.NOT_FORMAT_OPTION);
        Assert.assertEquals(execptSql, actualSql);
    }
}
