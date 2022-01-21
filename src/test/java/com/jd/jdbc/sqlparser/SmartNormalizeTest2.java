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
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRestoreVisitor;
import com.jd.jdbc.srvtopo.BindVariable;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.junit.Assert;

import static com.jd.jdbc.sqlparser.SqlParser.prepareAst;

public class SmartNormalizeTest2 {
    private List<TestCase> testCaseList;

    private static String getStringByLength(int length) {
        StringBuilder stringBuilder = new StringBuilder("'");
        for (int i = 0; i < length; i++) {
            stringBuilder.append("A");
        }
        stringBuilder.append("'");
        String str = stringBuilder.toString();
        return str;
    }

    public void test() throws SQLException {
        for (TestCase testCase : testCaseList) {
            SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(testCase.innerSql);
            SqlParser.PrepareAstResult prepareAstResult = prepareAst(stmt, testCase.inBindVariableMap, null);

            Assert.assertEquals(testCase.outerSql, SQLUtils.toMySqlString(prepareAstResult.getAst(), SQLUtils.NOT_FORMAT_OPTION));
            if (testCase.outBindVariableMap != null) {
                Assert.assertEquals(testCase.outBindVariableMap, prepareAstResult.getBindVariableMap());
            }

            StringBuilder output = new StringBuilder();
            VtRestoreVisitor vtRestoreVisitor = new VtRestoreVisitor(output, prepareAstResult.getBindVariableMap(), null);
            prepareAstResult.getAst().accept(vtRestoreVisitor);
            Assert.assertEquals(testCase.querySql, output.toString());
        }
    }

    public void testMix() throws SQLException {
        for (TestCase testCase : testCaseList) {
            SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(testCase.innerSql);
            SqlParser.PrepareAstResult prepareAstResult = prepareAst(stmt, testCase.inBindVariableMap, null);

            StringBuilder output = new StringBuilder();
            VtRestoreVisitor vtRestoreVisitor = new VtRestoreVisitor(output, prepareAstResult.getBindVariableMap(), null);
            prepareAstResult.getAst().accept(vtRestoreVisitor);
            Assert.assertEquals(testCase.querySql, output.toString());
        }
    }

    private TestCase noPreparedStringParameters() throws Exception {
        String sqlFormat = "select * from t_string where f_varchar = %s and f_char = %s and f_binary = %s and f_varbinary = %s and f_tinyblob = %s " +
            "and f_blob = %s and f_mediumblob = %s and f_longblob = %s and f_tinytext = %s and f_text = %s and f_mediumtext = %s and f_longtext = %s " +
            "and f_enum = %s and f_set = %s order by f_varchar;";
        String[] expectedBindVars = new String[] {getStringByLength(10),
            getStringByLength(1),
            getStringByLength(3),
            getStringByLength(6),
            getStringByLength(16),
            getStringByLength(16),
            "'中文特test'",
            getStringByLength(6),
            getStringByLength(6),
            "'\uD83D\uDC95\uD83D\uDC95\uD83D\uDC95\uD83D\uDC95'",
            getStringByLength(6),
            "'Miss.龙\uD83D\uDC95'",
            getStringByLength(1),
            "'" + getStringByLength(11).getBytes() + "'"};
        String[] questionStringArray = new String[expectedBindVars.length];
        for (int i = 0; i < expectedBindVars.length; i++) {
            questionStringArray[i] = "?";
        }
        String sqlQuestion = String.format(sqlFormat, questionStringArray);
        String sql = String.format(sqlFormat, expectedBindVars);

        return new TestCase(sql, sqlQuestion, sql, new LinkedHashMap<>(), null);
    }

    @AllArgsConstructor
    private static class TestCase {
        private final String innerSql;

        private final String outerSql;

        private final String querySql;

        private final Map<String, BindVariable> inBindVariableMap;

        private final Map<String, BindVariable> outBindVariableMap;
    }
}
