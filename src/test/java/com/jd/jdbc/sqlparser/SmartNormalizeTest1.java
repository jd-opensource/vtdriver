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

import com.google.protobuf.ByteString;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRestoreVisitor;
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
            new LinkedHashMap<String, Query.BindVariable>() {{
                put("0", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(1).getBytes())).build());
                put("1", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(1).getBytes())).build());
                put("2", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(2).getBytes())).build());
                put("3", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(3).getBytes())).build());
                put("4", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(4000).getBytes())).build());
                put("5", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(5000).getBytes())).build());
            }}));
    }

    @Test
    public void noPreparedParameters1() throws SQLException {
        test(new TestCase(
            "select * from t where id = 1",
            "select * from t where id = ?",
            "select * from t where id = 1",
            Collections.emptyMap(),
            new LinkedHashMap<String, Query.BindVariable>() {{
                put("0", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(1).getBytes())).build());
            }}));
    }

    @Test
    public void noPreparedParameters2() throws SQLException {
        test(new TestCase(
            "select * from t where id = 1 and name in (2, 3, 4) and age > 5",
            "select * from t where id = ? and name in (?, ?, ?) and age > ?",
            "select * from t where id = 1 and name in (2, 3, 4) and age > 5",
            Collections.emptyMap(),
            new LinkedHashMap<String, Query.BindVariable>() {{
                put("0", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(1).getBytes())).build());
                put("1", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(2).getBytes())).build());
                put("2", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(3).getBytes())).build());
                put("3", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(4).getBytes())).build());
                put("4", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(5).getBytes())).build());
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

        LinkedHashMap<String, Query.BindVariable> expectedBindVtVars = new LinkedHashMap<>();
        Set bindVars = new HashSet();
        int indexKey = 0;
        for (int i = 0; i < 20; i++) {
            expectedBindVtVars.put(String.valueOf(indexKey), Query.BindVariable.newBuilder()
                .setType(expectedBindVarsTypes[i])
                .setValue(ByteString.copyFrom(expectedBindVars[i].getBytes()))
                .build());
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

        private final Map<String, Query.BindVariable> inBindVariableMap;

        private final Map<String, Query.BindVariable> outBindVariableMap;
    }
}