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
import com.jd.jdbc.common.tuple.ImmutablePair;
import com.jd.jdbc.common.tuple.Pair;
import com.jd.jdbc.planbuilder.semantics.Analyzer;
import com.jd.jdbc.planbuilder.semantics.FakeSI;
import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class RewriterTest extends BaseTest {
    @Test
    public void testHavingRewrite() throws SQLException {
        class TmpCase {
            String input;

            String output;

            Map<String, String> sql;

            public TmpCase(String input, String output) {
                this.input = input;
                this.output = output;
            }

            public TmpCase(String input, String output, Map<String, String> sql) {
                this.input = input;
                this.output = output;
                this.sql = sql;
            }
        }

        List<TmpCase> tmpCases = new ArrayList<>();
        tmpCases.add(new TmpCase("select 1 from t1 having a = 1", "select 1 from t1 where a = 1"));
        tmpCases.add(new TmpCase("select 1 from t1 where x = 1 and y = 2 having a = 1", "select 1 from t1 where x = 1 and y = 2 and a = 1"));
        tmpCases.add(new TmpCase("select 1 from t1 where x = 1 or y = 2 having a = 1", "select 1 from t1 where (x = 1 or y = 2) and a = 1"));
        tmpCases.add(new TmpCase("select 1 from t1 where x = 1 having a = 1 and b = 2", "select 1 from t1 where x = 1 and a = 1 and b = 2"));
        tmpCases.add(new TmpCase("select 1 from t1 where x = 1 having a = 1 or b = 2", "select 1 from t1 where x = 1 and (a = 1 or b = 2)"));
        tmpCases.add(new TmpCase("select 1 from t1 where x = 1 and y = 2 having a = 1 and b = 2", "select 1 from t1 where x = 1 and y = 2 and a = 1 and b = 2"));
        tmpCases.add(new TmpCase("select 1 from t1 where x = 1 or y = 2 having a = 1 and b = 2", "select 1 from t1 where (x = 1 or y = 2) and a = 1 and b = 2"));
        tmpCases.add(new TmpCase("select 1 from t1 where x = 1 and y = 2 having a = 1 or b = 2", "select 1 from t1 where x = 1 and y = 2 and (a = 1 or b = 2)"));
        tmpCases.add(new TmpCase("select 1 from t1 where x = 1 or y = 2 having a = 1 or b = 2", "select 1 from t1 where (x = 1 or y = 2) and (a = 1 or b = 2)"));
        tmpCases.add(new TmpCase("select 1 from t1 where x = 1 or y = 2 having a = 1 and count(*) = 1", "select 1 from t1 where (x = 1 or y = 2) and a = 1 having count(*) = 1"));
        tmpCases.add(new TmpCase("select count(*) k from t1 where x = 1 or y = 2 having a = 1 and k = 1", "select count(*) as k from t1 where (x = 1 or y = 2) and a = 1 having k = 1"));
        tmpCases.add(new TmpCase("select count(*) k from t1 having k = 10", "select count(*) as k from t1 having k = 10"));
        tmpCases.add(new TmpCase("select 1 from t1 group by a having a = 1 and count(*) > 1", "select 1 from t1 where a = 1 group by a having count(*) > 1"));
        // new
        tmpCases.add(new TmpCase("select count(*) as k from t1 having k = 10", "select count(*) as k from t1 having k = 10"));
        tmpCases.add(new TmpCase("select col1 as k from t1 group by t having k = 10", "select col1 as k from t1 where col1 = 10 group by t"));
        tmpCases.add(new TmpCase("select t1.col1 as k from t1 group by t having k = 10", "select t1.col1 as k from t1 where t1.col1 = 10 group by t"));
        tmpCases.add(new TmpCase("select t1.col1 from t1 group by t having t1.col1 = 10", "select t1.col1 from t1 where t1.col1 = 10 group by t"));
        tmpCases.add(new TmpCase("select distinct col1 as k from t1 group by k having k = 10", "select distinct col1 as k from t1 where col1 = 10 group by k"));
        tmpCases.add(new TmpCase("select distinct t1.col1 as k from t1 group by k having k = 10", "select distinct t1.col1 as k from t1 where t1.col1 = 10 group by k"));
        tmpCases.add(new TmpCase("select distinct t1.col1 from t1 group by k having t1.col1 = 10", "select distinct t1.col1 from t1 where t1.col1 = 10 group by k"));
        tmpCases.add(new TmpCase("select col1 as k,count(col2) from t1 group by t having k = 10", "select col1 as k, count(col2) from t1 where col1 = 10 group by t"));
        tmpCases.add(new TmpCase("select col1 as k,count(col2) as c from t1 group by t having k = 10 and c > 1", "select col1 as k, count(col2) as c from t1 where col1 = 10 group by t having c > 1"));
        tmpCases.add(
            new TmpCase("select col1 as k,count(t1.col2) as c from t1 group by t having k = 10 and c > 1", "select col1 as k, count(t1.col2) as c from t1 where col1 = 10 group by t having c > 1"));
        tmpCases.add(new TmpCase("select distinct col1 as k,count(t1.col2) as c from t1 group by t having k = 10 and c > 1",
            "select distinct col1 as k, count(t1.col2) as c from t1 where col1 = 10 group by t having c > 1"));
        tmpCases.add(
            new TmpCase("select col1 as k,count(t1.col2) as c from t1 group by t having k = 10 and c > 1", "select col1 as k, count(t1.col2) as c from t1 where col1 = 10 group by t having c > 1"));

        for (TmpCase tmpCase : tmpCases) {
            Pair<SemTable, SQLSelectStatement> pair = prepTest(tmpCase.input);
            SQLSelectStatement sel = pair.getRight();
            Rewriter.queryRewrite(pair.getLeft(), null, sel);
            Assert.assertEquals("", tmpCase.output, SQLUtils.toMySqlString(sel, SQLUtils.NOT_FORMAT_OPTION).trim());
            printOk("testHavingRewrite is [OK],current sql = " + tmpCase.input);
        }
    }

    private Pair<SemTable, SQLSelectStatement> prepTest(String sql) throws SQLException {
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        SemTable semTable = Analyzer.analyze((SQLSelectStatement) stmt, "", new FakeSI(new HashMap<>(), null));
        return new ImmutablePair(semTable, stmt);
    }
}
