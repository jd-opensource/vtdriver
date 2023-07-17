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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

@Ignore // todo 未执行对应的表初始化脚本，暂时忽略
public class KeywordTest extends TestSuite {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void test() throws SQLException {
        Connection conn = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("delete from keyword_test");

        // "sql", "select", "all", "int1", "int2", "int3", "int4" 都是保留字, 作为标识符使用时需要加反引号
        stmt.executeQuery("select `sql` from keyword_test limit 1");
        stmt.executeQuery("select `sql` from keyword_test where `sql`='a' limit 1");
        stmt.executeQuery("select `select` from keyword_test limit 1");
        stmt.executeQuery("select `all` from keyword_test limit 1");
        stmt.executeQuery("select `int1`, `int2`, `int3`, `int4` from keyword_test limit 1");
        stmt.executeUpdate("insert into keyword_test (id, `sql`, `select`, `all`) values (1, 'sql', 2, 3)");

        // "表名.字段名" 可以不加反引号
        stmt.executeQuery("select keyword_test.sql from keyword_test limit 1");
        stmt.executeQuery("select keyword_test.all from keyword_test limit 1");

        // "any", "bit" 是关键字, 但不是保留字, 可以直接作为标识符使用
        stmt.executeQuery("select any from keyword_test limit 1");
        stmt.executeQuery("select bit from keyword_test limit 1");

        // "int5" 不是关键字
        stmt.executeQuery("select int5 from keyword_test limit 1");

        // 错误验证
        //thrown.expect(java.sql.SQLSyntaxErrorException.class);
        //thrown.expectMessage("You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'sql from keyword_test limit 1' at line 1");
        stmt.executeQuery("select sql from keyword_test limit 1");

        stmt.close();
        closeConnection(conn);
    }
}
