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

import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.SqlParser;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.vitess.VitessConnection;
import com.jd.jdbc.vitess.VitessStatement;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

public class PlanCacheTest extends TestSuite {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private String[] unionSQL;

    private String[] queryWithoutUnion;

    private String[] subQueryWithoutUnion;

    private Connection conn;

    public void initTestCase() throws IOException {
        unionSQL = new String[] {
            "select * from (select id from user_extra where user_id = 22 union all select id from user_metadata where user_id = 55) t",
            "select id from user_extra union all select id from user_metadata",
            "select id from user_extra where user_id = 22 union all select id from user_metadata where user_id = 55",
            "(select id, email from user_extra order by id asc limit 1) union all (select id, email from user_metadata order by id desc limit 1)",
            "select id, email from user_extra union all select id, email from user_metadata",
            "select * from (select id, email from user_extra union all select id, email from user_metadata) as t",
            "(select id from user_extra order by id limit 5) union all (select id from user_extra order by id desc limit 5)",
            "select id from user_extra where user_id = 22 union select id from user_extra where user_id = 22 union all select id from user_extra",
            "select id from user_extra where user_id = 222 union all select id from user_metadata where user_id = 55",
            "select id from user_extra where user_id = 222 union all select id from user_metadata where user_id = 555",
            "select id from user_extra where user_id = 22 union all select id from user_metadata where user_id = 55",
            "select id from user_extra where user_id = 22 union all select id from user_metadata where user_id = 555",
        };

        queryWithoutUnion = new String[] {
            "select count(id) from user_extra",
            "select count(*) from user_extra",
            "select distinct(id) from user_extra",
            "select count(distinct(user_id)) from user_extra",
            "select count(distinct(id)) from user_extra",
            "SELECT INDEX_LENGTH FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'USER'",
            "select id from user where database()",
            "select id from music where id = null",
            "select user.costly from user use index(user_costly_uindex)",
            "select * from user_extra group by user_id having sum(id) > 50",
            // join
            "select user_extra.id from user join user_extra on user.name = user_extra.user_id where user.name = 105",
            "select user_extra.id from user join user_extra on user.name = user_extra.user_id where user_extra.user_id = 105",
            "select user_extra.id from user left join user_extra on user.name = user_extra.user_id where user.name = 105",
            "select user_extra.id from user left join user_extra on user.name = user_extra.user_id where user_extra.user_id = 105",
            "select user_extra.id from user join user_extra on user.costly = user_extra.extra_id where user.name = 105",
            "select user_extra.id from user join user_extra on user.costly = user_extra.extra_id where user.name = 105 and user_extra.user_id = 105",
            "select user_extra.id from user join user_extra on user.costly = user_extra.extra_id where user_extra.user_id = user.costly",
            "select user_extra.id from user join user_extra on user.costly = user_extra.extra_id where 1 = 1",
            "select user_extra.Id from user join user_extra on user.nAME = user_extra.User_Id where user.Name = 105",
            "select music.col from user join music",
            "select music.col from user, music",
            "select user.costly from user join user_extra on user.name = user_extra.user_id",
            "select user.costly from user join user_extra on (user.name = user_extra.user_id)",
            "select user.costly from user join user_extra on user.costly between 100 and 200 and user.name = user_extra.user_id",
            "select user.costly from user join user_extra on user_extra.user_id = user.name",
            "select user.costly from user join user_extra on user.name = 105 and user.name = user_extra.user_id",
            "select user.costly from user join user_extra on user.name < user_extra.user_id",
            "select user.costly from user join user_extra on user.name = 105",
            "select user.costly from user join user_extra on 105 = user.name",
        };

        subQueryWithoutUnion = new String[] {
            "select email from user_extra where email in ( select email from user_extra ) order by email limit 3",
            "select email from user_extra where email in ( select email from user_extra ) order by email limit 1,1",
            "select cnt from (select count(*) as cnt from user) t",
            "select PLUGIN_NAME pluginName from information_schema.PLUGINS where PLUGIN_NAME in (select PLUGIN_NAME from information_schema.PLUGINS) and PLUGIN_NAME IN ('mysql_native_password','sha256_password')",
            "select u.id from user_extra join user u where u.name in (select name from user where user.name = u.name and user_extra.extra_id = user.predef1) and u.name in (user_extra.extra_id, 1)",
            "select u.id from user_extra join user u where u.name in (select costly from user where user.name = 105) and u.name = 105",
            "select u.id from user_extra join user u where u.name in (select predef2 from user where user.name = u.name and user_extra.extra_id = user.predef1 and user.name in (select extra_id from user_extra where user_extra.user_id = user.name)) and u.name in (user_extra.extra_id, 1)",
            "select id from user where user.predef1 in (select user_extra.extra_id from user_extra where user_extra.user_id = user.name)",
            "select id from user where name = 105 and user.predef1 in (select user_extra.extra_id from user_extra where user_extra.user_id = 105)",
            "select id from user where name = '103' and user.predef1 in (select user_extra.extra_id from user_extra where user_extra.user_id = '103')",
            "select id from user uu where name in (select name from user where name = uu.name and user.predef1 in (select user_extra.extra_id from user_extra where user_extra.user_id = uu.name))",
            "select name from user where name in (select costly from user)",
            "select id, name from user where id not in (select id from user where id = 72 or id = 13)",
            "select id from user where exists (select predef1 from user)",
            "select id from (select id, textcol1 from user where name = 5) as t",
            "select t.name from ( select name from user where name = 105 ) as t join user_extra on t.name = user_extra.user_id",
            "select t.name from ( select user.name from user where user.name = 105 ) as t join user_extra on t.name = user_extra.user_id",
            "select t.name from user_extra join ( select name from user where name = 105 ) as t on t.name = user_extra.user_id",
            "select t.name from ( select name from user where name = 105 ) as t join user_extra on t.name = user_extra.user_id",
            "select u.predef1, e.extra_id from ( select predef1 from user where name = 105 ) as u join ( select extra_id from user_extra where user_id = 105 ) as e",
            "select t.id from ( select user.id, user.predef2 from user join user_extra on user_extra.extra_id = user.costly ) as t",
            "select id, t.id from ( select user.id from user join user_extra on user.id < 3) as t",
            "select name from ( select user.name, user.costly from user join user_extra ) as t order by name",
            "select name from user having name in ( select costly from user )",
            "select textcol1 from user where textcol1 in ( select textcol1 from user ) order by textcol1",
            "select textcol2 from user where textcol2 in ( select textcol2 from user ) order by null",
            "select costly from user where costly in ( select costly from user ) order by rand()",
            "select u.name from user u join ( select user_id from user_extra where user_id = 105 ) eu on u.name = eu.user_id where u.name = 105 order by eu.user_id",
            "select textcol2 from user where textcol2 in ( select textcol2 from user ) order by textcol2 limit 1",
            "select name from user where name in ( select name from user ) order by name",
            "select name from user where name in ( select name from user ) order by name limit 2,2",
            "select name from user where name in ( select name from user where id > 2) order by name",
            "select id from (select * from user_extra group by user_id having sum(id) > 500) as id",
            "select user_id from user_extra where exists (select user_id from user_extra where user_id >101)",
            "select id from user_extra where exists (select id from user_extra where id >91)",
        };
    }

    @Before
    public void init() throws SQLException, IOException {
        conn = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        initTestCase();
        ((VitessConnection) conn).getExecutor().getPlans().clear();
    }

    @After
    public void close() throws SQLException {
        if (null != conn) {
            conn.close();
        }
    }

    public String getPlanCacheKey(final String sql, final VitessStatement stmt) throws SQLException {
        VitessStatement.ParseResult parseResult = stmt.parseStatements(sql);
        SqlParser.PrepareAstResult prepareAstResult = SqlParser.prepareAst(parseResult.getStatement(), null, "utf-8");
        SQLStatement ast = prepareAstResult.getAst();
        return parseResult.getSchema() + ":" + SQLUtils.toMySqlString(ast, SQLUtils.NOT_FORMAT_OPTION);
    }

    private void assertAfterRewrite(final String[] sqls, final boolean combineRepeatBindVars) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            for (String sql : sqls) {
                printNormal(sql);

                String key = getPlanCacheKey(sql, (VitessStatement) stmt);
                printNormal(key);
                if (key.contains("?")) {
                    Assert.assertEquals(combineRepeatBindVars, key.matches(".*\\?[0-9]+.*"));
                }

                stmt.execute(sql);
                Assert.assertNotNull(((VitessConnection) conn).getExecutor().getPlans().get(key));

                printOk("[OK]\n");
            }
        }
    }

    @Test
    public void testUnion() throws SQLException {
        assertAfterRewrite(unionSQL, true);
    }

    @Test
    public void testSubQuery() throws SQLException {
        assertAfterRewrite(subQueryWithoutUnion, true);
    }

    @Test
    public void testQueryWithoutUnion() throws SQLException {
        assertAfterRewrite(queryWithoutUnion, false);
    }

    @Test
    @Ignore
    public void testGroupByHaving() throws SQLException {
        String[] having = new String[] {
            "select id, user_id from user_extra group by 1, 2 having sum(id) > 50",
            "select id, user_id from user_extra group by user_id having sum(id) > 50",

            "select id, user_id from user_extra group by id, user_id",
            "select id, user_id from user_extra group by 1, 2",
        };

        Statement stmt = conn.createStatement();
        for (int i = 0; i < having.length; i++) {
            String sql = having[i];
            printNormal(sql);
            String key = getPlanCacheKey(sql, (VitessStatement) stmt);
            printNormal(key);

            if (i < 2) {
                Assert.assertTrue(key.contains("?"));
                Assert.assertFalse(key.matches(".*\\?[0-9]+.*"));
                Assert.assertEquals(key.indexOf("?"), key.lastIndexOf("?"));
            } else {
                Assert.assertFalse(key.contains("?"));
            }

            stmt.execute(sql);
            Assert.assertNotNull(((VitessConnection) conn).getExecutor().getPlans().get(key));
            printOk("[OK]\n");
        }

        stmt.close();
    }

    @Test
    public void testNullBindVariable() throws SQLException {
        String sql = "INSERT INTO `type_test` (`id`, `f_key`, `f_decimal`) VALUES (?, ?, ?)";

        Statement stmt = conn.createStatement();
        stmt.executeUpdate("delete from type_test");

        PreparedStatement pstmt = conn.prepareStatement(sql);

        pstmt.setNull(1, java.sql.Types.NULL);

        thrown.expect(java.sql.SQLException.class);
        thrown.expectMessage("No value specified for parameter 2");
        pstmt.executeUpdate();

        pstmt.close();
        stmt.close();
        conn.close();
    }
}
