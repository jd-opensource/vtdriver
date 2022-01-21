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
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.junit.Assert;
import org.junit.Test;

import static com.jd.jdbc.sqlparser.SqlParser.prepareAst;

public class SmartNormalizeTest1 {

    public void test(TestCase testCase) throws SQLException {
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

    @Test
    public void selectCount() throws SQLException {
        test(new TestCase(
            "select count(1) from customer where customer_id in (1, 2, 3, 4000, 5000)",
            "select count(?) from customer where customer_id in (?, ?, ?, ?, ?)",
            "select count(1) from customer where customer_id in (1, 2, 3, 4000, 5000)",
            Collections.emptyMap(),
            new LinkedHashMap<String, BindVariable>() {{
                put("0", new BindVariable(String.valueOf(1).getBytes(), Query.Type.INT32));
                put("1", new BindVariable(String.valueOf(1).getBytes(), Query.Type.INT32));
                put("2", new BindVariable(String.valueOf(2).getBytes(), Query.Type.INT32));
                put("3", new BindVariable(String.valueOf(3).getBytes(), Query.Type.INT32));
                put("4", new BindVariable(String.valueOf(4000).getBytes(), Query.Type.INT32));
                put("5", new BindVariable(String.valueOf(5000).getBytes(), Query.Type.INT32));
            }}));
    }

    @Test
    public void noPreparedParameters1() throws SQLException {
        test(new TestCase(
            "select * from t where id = 1",
            "select * from t where id = ?",
            "select * from t where id = 1",
            Collections.emptyMap(),
            new LinkedHashMap<String, BindVariable>() {{
                put("0", new BindVariable(String.valueOf(1).getBytes(), Query.Type.INT32));
            }}));
    }

    @Test
    public void noPreparedParameters2() throws SQLException {
        test(new TestCase(
            "select * from t where id = 1 and name in (2, 3, 4) and age > 5",
            "select * from t where id = ? and name in (?, ?, ?) and age > ?",
            "select * from t where id = 1 and name in (2, 3, 4) and age > 5",
            Collections.emptyMap(),
            new LinkedHashMap<String, BindVariable>() {{
                put("0", new BindVariable(String.valueOf(1).getBytes(), Query.Type.INT32));
                put("1", new BindVariable(String.valueOf(2).getBytes(), Query.Type.INT32));
                put("2", new BindVariable(String.valueOf(3).getBytes(), Query.Type.INT32));
                put("3", new BindVariable(String.valueOf(4).getBytes(), Query.Type.INT32));
                put("4", new BindVariable(String.valueOf(5).getBytes(), Query.Type.INT32));
            }}));
    }

    @Test
    public void verifyLiteralTypesInt() throws SQLException {
        //https://www.mysqltutorial.org/mysql-int/

        String sqlFormat = "select * from t where f_tiny = %s or f_tiny = %s or f_utiny = %s or f_utiny = %s" +
            " or f_small = %s or f_small = %s or f_small = %s or f_small = %s" +
            " or f_mid = %s or f_mid = %s or f_mid = %s or f_mid = %s" +
            " or f_int = %s or f_int = %s or f_int = %s or f_int = %s" +
            " or f_big = %s or f_big = %s or f_big = %s or f_big = %s";

        String[] expectedBindVars = new String[] {
            "-128", "127", "0", "255",
            "-32768", "32767", "0", "65535",
            "-8388608", "8388607", "0", "16777215",
            "-2147483648", "2147483647", "0", "4294967295",
            "-9223372036854775808", "9223372036854775807", "0", "18446744073709551615"
        };

        Query.Type[] expectedBindVarsTypes = new Query.Type[] {
            Query.Type.INT32, Query.Type.INT32, Query.Type.INT32, Query.Type.INT32,
            Query.Type.INT32, Query.Type.INT32, Query.Type.INT32, Query.Type.INT32,
            Query.Type.INT32, Query.Type.INT32, Query.Type.INT32, Query.Type.INT32,
            Query.Type.INT32, Query.Type.INT32, Query.Type.INT32, Query.Type.INT64,
            Query.Type.INT64, Query.Type.INT64, Query.Type.INT32, Query.Type.UINT64
        };

        LinkedHashMap<String, BindVariable> expectedBindVtVars = new LinkedHashMap<>();
        Set bindVars = new HashSet();
        int indexKey = 0;
        for (int i = 0; i < 20; i++) {
            BindVariable bindVariable = new BindVariable(expectedBindVars[i].getBytes(), expectedBindVarsTypes[i]);
            expectedBindVtVars.put(String.valueOf(indexKey), bindVariable);
            bindVars.add(expectedBindVars[i]);
            indexKey++;
        }

        String normalizedSql = String.format(sqlFormat, "?", "?", "?", "?",
            "?", "?", "?", "?",
            "?", "?", "?", "?",
            "?", "?", "?", "?",
            "?", "?", "?", "?");

        String sql = String.format(sqlFormat, expectedBindVars);

        test(new TestCase(
            sql,
            normalizedSql,
            sql,
            Collections.emptyMap(),
            expectedBindVtVars
        ));
    }


    @Test
    public void verifyLiteralTypesMisc() throws SQLException {
        //https://www.mysqltutorial.org/mysql-data-types.aspx/
        //https://www.mysqltutorial.org/mysql-bit/
        //https://www.mysqltutorial.org/mysql-decimal/

        return;
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