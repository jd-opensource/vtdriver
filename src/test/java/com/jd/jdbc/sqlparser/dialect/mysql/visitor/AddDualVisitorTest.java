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

package com.jd.jdbc.sqlparser.dialect.mysql.visitor;

import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import org.junit.Assert;
import org.junit.Test;

public class AddDualVisitorTest {
    @Test
    public void case01() {
        @AllArgsConstructor
        class TestCase {
            private final String originSql;

            private final String expect;
        }
        List<TestCase> testCaseList = new ArrayList<>();
        testCaseList.add(new TestCase("select 1", "select 1 from dual"));
        testCaseList.add(new TestCase("select 100", "select 100 from dual"));
        testCaseList.add(new TestCase("select 1+2 as result", "select 1 + 2 as result from dual"));
        testCaseList.add(new TestCase("(select 1 union all select 1)", "(select 1 from dual union all select 1 from dual)"));
        testCaseList.add(new TestCase("select 1 union all select 1", "select 1 from dual union all select 1 from dual"));
        testCaseList.add(new TestCase("(select 1 union select 1)", "(select 1 from dual union select 1 from dual)"));
        testCaseList.add(new TestCase("select 1 union select 1", "select 1 from dual union select 1 from dual"));
        testCaseList.add(new TestCase("select id from user union select 1", "select id from user union select 1 from dual"));
        testCaseList.add(new TestCase("select id from user union all select 1", "select id from user union all select 1 from dual"));
        testCaseList.add(new TestCase("SELECT column1, column2, (SELECT 1) AS constant FROM table1 WHERE column3 IN (SELECT column3 FROM table2 WHERE column4 = 'some value')",
            "select column1, column2, ( select 1 from DUAL ) as constant from table1 where column3 in ( select column3 from table2 where column4 = 'some value' )"));
        testCaseList.add(new TestCase("SELECT id, name FROM (SELECT 0 as id , 'DEFAULT' as name) t", "SELECT id, name FROM ( SELECT 0 as id, 'DEFAULT' as name from dual ) t"));
        testCaseList.add(new TestCase("select id, name, age from t_users where id = ? and name is not ? and age in (select 1 )",
            "select id, name, age from t_users where id = ? and name is not ? and age in ( select 1 from dual )"));
        for (TestCase testCase : testCaseList) {
            SQLStatement statement = SQLUtils.parseSingleMysqlStatement(testCase.originSql);
            AddDualVisitor visitor = new AddDualVisitor();
            statement.accept(visitor);
            String newSql = SQLUtils.toMySqlString(statement, SQLUtils.NOT_FORMAT_OPTION);
            Assert.assertTrue(testCase.originSql + " FAIL,rewrite sql: " + newSql, testCase.expect.equalsIgnoreCase(newSql));
        }
    }
}