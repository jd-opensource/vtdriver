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

import com.jd.BaseTest;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import lombok.AllArgsConstructor;
import org.junit.Assert;
import org.junit.Test;

public class EarlyRewriterTest extends BaseTest {
    @Test
    public void testOrderByGroupByLiteral() {
        FakeSI schemaInfo = new FakeSI(new HashMap<>(), null);
        String cDB = "db";

        @AllArgsConstructor
        class TmpCase {
            String sql;

            String expSQL;

            String expErr;
        }
        List<TmpCase> tmpCases = new ArrayList<>();
        tmpCases.add(new TmpCase("select 1 as id from t1 order by 1 desc", "select 1 as id from t1 order by id desc", null));
        tmpCases.add(new TmpCase("select 1 as id from t1 order by 1", "select 1 as id from t1 order by id", null));
        tmpCases.add(new TmpCase("select 1 as id from t1 order by 1 asc", "select 1 as id from t1 order by id asc", null));
        tmpCases.add(new TmpCase("select t1.col from t1 order by 1 desc", "select t1.col from t1 order by t1.col desc", null));
        tmpCases.add(new TmpCase("select t1.col from t1 order by 1", "select t1.col from t1 order by t1.col", null));
        tmpCases.add(new TmpCase("select t1.col from t1 order by 1 asc", "select t1.col from t1 order by t1.col asc", null));
        tmpCases.add(new TmpCase("select t1.col from t1 group by 1", "select t1.col from t1 group by t1.col", null));
        tmpCases.add(new TmpCase("select t1.col as xyz from t1 group by 1", "select t1.col as xyz from t1 group by xyz", null));
        tmpCases.add(new TmpCase("select t1.col as xyz, count(*) from t1 group by 1 order by 2", "select t1.col as xyz, count(*) from t1 group by xyz order by count(*)", null));
        tmpCases.add(new TmpCase("select t1.col as xyz, count(*) from t1 group by 1 order by 2 asc", "select t1.col as xyz, count(*) from t1 group by xyz order by count(*) asc", null));
        tmpCases.add(new TmpCase("select t1.col as xyz, count(*) from t1 group by 1 order by 2 desc", "select t1.col as xyz, count(*) from t1 group by xyz order by count(*) desc", null));
        tmpCases.add(new TmpCase("select id from t1 group by 2", null, "Unknown column '2' in 'group statement'"));
        tmpCases.add(new TmpCase("select id from t1 order by 2", null, "Unknown column '2' in 'order clause'"));
        tmpCases.add(new TmpCase("select *, id from t1 order by 2", null, "cannot use column offsets in order clause when using `*`"));
        tmpCases.add(new TmpCase("select *, id from t1 group by 2", null, "cannot use column offsets in group statement when using `*`"));
        tmpCases.add(new TmpCase("select id from t1 order by 1 collate utf8_general_ci", "select id from t1 order by id collate utf8_general_ci", null));
        tmpCases.add(new TmpCase("select id from t1 order by 1 collate utf8_general_ci asc", "select id from t1 order by id collate utf8_general_ci asc", null));
        tmpCases.add(new TmpCase("select id from t1 order by 1 collate utf8_general_ci desc", "select id from t1 order by id collate utf8_general_ci desc", null));

        for (TmpCase tmpCase : tmpCases) {
            SQLStatement ast = SQLUtils.parseSingleMysqlStatement(tmpCase.sql);
            Assert.assertNotNull(ast);
            try {
                SemTable semTable = Analyzer.analyze((SQLSelectStatement) ast, cDB, schemaInfo);
                Assert.assertTrue(StringUtils.isEmpty(tmpCase.expErr));
                String trim = SQLUtils.toMySqlString(ast, SQLUtils.NOT_FORMAT_OPTION).trim();
                Assert.assertEquals(tmpCase.expSQL, trim);
                printOk("testOrderByGroupByLiteral is [OK],current sql = " + tmpCase.sql);
            } catch (SQLException e) {
                Assert.assertFalse(StringUtils.isEmpty(tmpCase.expErr));
                Assert.assertEquals(tmpCase.expErr, e.getMessage());
                printOk("expect error [OK],current sql = " + tmpCase.sql);
            }

        }
    }
}