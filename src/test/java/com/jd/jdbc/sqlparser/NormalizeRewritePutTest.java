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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import testsuite.TestSuite;

public class NormalizeRewritePutTest extends TestSuite {
    private List<TestCase> testCaseList = null;

    @Before
    public void init() throws Exception {
        this.initTestCase();
    }

    @Test
    public void testNormalizeRewritePut() throws SQLException {
        for (TestCase testCase : testCaseList) {
            SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(testCase.innerSql);

            printNormal("Inner SQL: " + testCase.innerSql);
            printInfo("Outer SQL: " + testCase.outerSql);
            printInfo("Query SQL: " + testCase.querySql);

            SqlParser.PrepareAstResult prepareAstResult = SqlParser.prepareAst(stmt, testCase.inBindVariableMap, null);
            SQLStatement resultStmt = prepareAstResult.getAst();
            Map<String, Query.BindVariable> bindVariableMap = prepareAstResult.getBindVariableMap();

            Assert.assertEquals(printFail("Normalize SQL Error!"),
                testCase.outerSql, SQLUtils.toMySqlString(resultStmt, SQLUtils.NOT_FORMAT_OPTION));
            Assert.assertEquals(printFail("Normalize Bind Variables Error!"),
                testCase.outBindVariableMap, bindVariableMap);

            StringBuilder output = new StringBuilder();
            VtRestoreVisitor vtRestoreVisitor = new VtRestoreVisitor(output, bindVariableMap, null);
            resultStmt.accept(vtRestoreVisitor);

            Assert.assertEquals(printFail("Put Bind Variables Error!"),
                testCase.querySql, output.toString());

            printOk("[OK]");
            System.out.println();
        }
    }

    @Test
    public void testConcurrency() throws InterruptedException {
        int tSize = 100;
        CountDownLatch cdl = new CountDownLatch(tSize);
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        executorService.submit(() -> {
            try {
                String name = Thread.currentThread().getName();
                SQLStatement stmt = SQLUtils.parseSingleMysqlStatement("select * from test where f_tinyint = 1");
                SqlParser.PrepareAstResult prepareAstResult = SqlParser.prepareAst(stmt, Collections.emptyMap(), null);
                Assert.assertEquals(name + " rewrite sql error!", "select * from test where f_tinyint = ?0", SQLUtils.toMySqlString(prepareAstResult.getAst(), SQLUtils.NOT_FORMAT_OPTION));
                Assert.assertEquals(name + " bind variable error!", 1, prepareAstResult.getBindVariableMap().size());
            } catch (SQLException e) {
                e.printStackTrace();
                Assert.assertNull(e);
            } finally {
                cdl.countDown();
            }
        });
        System.out.println(cdl.await(10, TimeUnit.SECONDS));
        executorService.shutdown();
    }

    private void initTestCase() {
        testCaseList = new ArrayList<TestCase>() {{
            add(new TestCase(
                "select RES.* from ACT_RU_TIMER_JOB RES where LOCK_OWNER_ is null LIMIT 100 OFFSET 10",
                "select RES.* from ACT_RU_TIMER_JOB as RES where LOCK_OWNER_ is null limit ? offset ?",
                "select RES.* from ACT_RU_TIMER_JOB as RES where LOCK_OWNER_ is null limit 100 offset 10",
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("100".getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("10".getBytes())).build());
                }},
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("100".getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("10".getBytes())).build());
                }}));
            add(new TestCase(
                "select col03, col31 from test123 where id = 462 and col01 = 462",
                "select col03, col31 from test123 where id = ? and col01 = ?",
                "select col03, col31 from test123 where id = 462 and col01 = 462",
                Collections.emptyMap(),
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("462".getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("462".getBytes())).build());
                }}));
            add(new TestCase(
                "select col03, col31 from test_1_2_3 where id = 462 and col01 = 462",
                "select col03, col31 from test_1_2_3 where id = ? and col01 = ?",
                "select col03, col31 from test_1_2_3 where id = 462 and col01 = 462",
                Collections.emptyMap(),
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("462".getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("462".getBytes())).build());
                }}));
            add(new TestCase(
                "select col03, col31 from 123test where id = 462 and col01 = 462",
                "select col03, col31 from 123test where id = ? and col01 = ?",
                "select col03, col31 from 123test where id = 462 and col01 = 462",
                Collections.emptyMap(),
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("462".getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("462".getBytes())).build());
                }}));
            add(new TestCase(
                "select col03, col31 from 1_2_3_test where id = 462 and col01 = 462",
                "select col03, col31 from 1_2_3_test where id = ? and col01 = ?",
                "select col03, col31 from 1_2_3_test where id = 462 and col01 = 462",
                Collections.emptyMap(),
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("462".getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("462".getBytes())).build());
                }}));
            add(new TestCase(
                "select * from normalize_test where f_tinyint = -128 or f_tinyint = 127 or f_u_tinyint = 0 or f_u_tinyint = 255",
                "select * from normalize_test where f_tinyint = ? or f_tinyint = ? or f_u_tinyint = ? or f_u_tinyint = ?",
                "select * from normalize_test where f_tinyint = -128 or f_tinyint = 127 or f_u_tinyint = 0 or f_u_tinyint = 255",
                Collections.emptyMap(),
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("-128".getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("127".getBytes())).build());
                    put("2", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("0".getBytes())).build());
                    put("3", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("255".getBytes())).build());
                }}));
            add(new TestCase(
                "select * from normalize_test where f_smallint = -32768 or f_smallint = 32767 or f_u_smallint = 0 or f_u_smallint = 65535",
                "select * from normalize_test where f_smallint = ? or f_smallint = ? or f_u_smallint = ? or f_u_smallint = ?",
                "select * from normalize_test where f_smallint = -32768 or f_smallint = 32767 or f_u_smallint = 0 or f_u_smallint = 65535",
                Collections.emptyMap(),
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("-32768".getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("32767".getBytes())).build());
                    put("2", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("0".getBytes())).build());
                    put("3", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("65535".getBytes())).build());
                }}));
            add(new TestCase(
                "select * from normalize_test where f_mediumint = -8388608 or f_mediumint = 8388607 or f_u_mediumint = 0 or f_u_mediumint = 16777215",
                "select * from normalize_test where f_mediumint = ? or f_mediumint = ? or f_u_mediumint = ? or f_u_mediumint = ?",
                "select * from normalize_test where f_mediumint = -8388608 or f_mediumint = 8388607 or f_u_mediumint = 0 or f_u_mediumint = 16777215",
                Collections.emptyMap(),
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("-8388608".getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("8388607".getBytes())).build());
                    put("2", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("0".getBytes())).build());
                    put("3", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("16777215".getBytes())).build());
                }}));
            add(new TestCase(
                "select * from normalize_test where f_int = -2147483648 or f_int = 2147483647 or f_u_int = 0 or f_u_int = 4294967295",
                "select * from normalize_test where f_int = ? or f_int = ? or f_u_int = ? or f_u_int = ?",
                "select * from normalize_test where f_int = -2147483648 or f_int = 2147483647 or f_u_int = 0 or f_u_int = 4294967295",
                Collections.emptyMap(),
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("-2147483648".getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("2147483647".getBytes())).build());
                    put("2", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("0".getBytes())).build());
                    put("3", Query.BindVariable.newBuilder().setType(Query.Type.INT64).setValue(ByteString.copyFrom(Long.valueOf(4294967295L).toString().getBytes())).build());
                }}));
            add(new TestCase(
                "select * from normalize_test where f_bigint = -9223372036854775808 or f_bigint = 9223372036854775807 or f_u_bigint = 0 or f_u_bigint = 18446744073709551615",
                "select * from normalize_test where f_bigint = ? or f_bigint = ? or f_u_bigint = ? or f_u_bigint = ?",
                "select * from normalize_test where f_bigint = -9223372036854775808 or f_bigint = 9223372036854775807 or f_u_bigint = 0 or f_u_bigint = 18446744073709551615",
                Collections.emptyMap(),
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.INT64).setValue(ByteString.copyFrom(Long.valueOf(-9223372036854775808L).toString().getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.INT64).setValue(ByteString.copyFrom(Long.valueOf(9223372036854775807L).toString().getBytes())).build());
                    put("2", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom("0".getBytes())).build());
                    put("3", Query.BindVariable.newBuilder().setType(Query.Type.UINT64).setValue(ByteString.copyFrom(new BigInteger("18446744073709551615").toString(10).getBytes())).build());
                }}));
            add(new TestCase(
                "select * from normalize_test where f_float = 1.2 or f_float = -1.2 or f_float = -3.402823466E+38 or f_float = -1.175494351E-38 or f_u_float = 0.0 or f_u_float = -1.175494351E-38 or f_u_float = -3.402823466E+38",
                "select * from normalize_test where f_float = ? or f_float = ? or f_float = ? or f_float = ? or f_u_float = ? or f_u_float = ? or f_u_float = ?",
                "select * from normalize_test where f_float = 1.2 or f_float = -1.2 or f_float = -3.402823466E+38 or f_float = -1.175494351E-38 or f_u_float = 0.0 or f_u_float = -1.175494351E-38 or f_u_float = -3.402823466E+38",
                Collections.emptyMap(),
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.DECIMAL).setValue(ByteString.copyFrom(new BigDecimal("1.2").toEngineeringString().getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.DECIMAL).setValue(ByteString.copyFrom(new BigDecimal("-1.2").toEngineeringString().getBytes())).build());
                    put("2", Query.BindVariable.newBuilder().setType(Query.Type.DECIMAL).setValue(ByteString.copyFrom(new BigDecimal("-3.402823466E+38").toEngineeringString().getBytes())).build());
                    put("3", Query.BindVariable.newBuilder().setType(Query.Type.DECIMAL).setValue(ByteString.copyFrom(new BigDecimal("-1.175494351E-38").toEngineeringString().getBytes())).build());
                    put("4", Query.BindVariable.newBuilder().setType(Query.Type.DECIMAL).setValue(ByteString.copyFrom(String.valueOf(0.0).getBytes())).build());
                    put("5", Query.BindVariable.newBuilder().setType(Query.Type.DECIMAL).setValue(ByteString.copyFrom(new BigDecimal("-1.175494351E-38").toEngineeringString().getBytes())).build());
                    put("6", Query.BindVariable.newBuilder().setType(Query.Type.DECIMAL).setValue(ByteString.copyFrom(new BigDecimal("-3.402823466E+38").toEngineeringString().getBytes())).build());
                }}));
            add(new TestCase(
                "select * from normalize_test where f_double = 1.2 or f_double = -1.2 or f_double = -1.7976931348623157E+308 or f_double = -2.2250738585072014E-308 or f_u_double = 0.0 or f_u_double = -2.2250738585072014E-308 or f_u_double = -1.7976931348623157E+308",
                "select * from normalize_test where f_double = ? or f_double = ? or f_double = ? or f_double = ? or f_u_double = ? or f_u_double = ? or f_u_double = ?",
                "select * from normalize_test where f_double = 1.2 or f_double = -1.2 or f_double = -1.7976931348623157E+308 or f_double = -2.2250738585072014E-308 or f_u_double = 0.0 or f_u_double = -2.2250738585072014E-308 or f_u_double = -1.7976931348623157E+308",
                Collections.emptyMap(),
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.DECIMAL).setValue(ByteString.copyFrom(new BigDecimal("1.2").toEngineeringString().getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.DECIMAL).setValue(ByteString.copyFrom(new BigDecimal("-1.2").toEngineeringString().getBytes())).build());
                    put("2",
                        Query.BindVariable.newBuilder().setType(Query.Type.DECIMAL).setValue(ByteString.copyFrom(new BigDecimal("-1.7976931348623157E+308").toEngineeringString().getBytes())).build());
                    put("3",
                        Query.BindVariable.newBuilder().setType(Query.Type.DECIMAL).setValue(ByteString.copyFrom(new BigDecimal("-2.2250738585072014E-308").toEngineeringString().getBytes())).build());
                    put("4", Query.BindVariable.newBuilder().setType(Query.Type.DECIMAL).setValue(ByteString.copyFrom(String.valueOf(0.0).getBytes())).build());
                    put("5",
                        Query.BindVariable.newBuilder().setType(Query.Type.DECIMAL).setValue(ByteString.copyFrom(new BigDecimal("-2.2250738585072014E-308").toEngineeringString().getBytes())).build());
                    put("6",
                        Query.BindVariable.newBuilder().setType(Query.Type.DECIMAL).setValue(ByteString.copyFrom(new BigDecimal("-1.7976931348623157E+308").toEngineeringString().getBytes())).build());
                }}));
            add(new TestCase(
                "select * from normalize_test where f_bit = b'110' or f_bit = B'110' or f_bit = 0b110",
                "select * from normalize_test where f_bit = b'110' or f_bit = b'110' or f_bit = b'110'",
                "select * from normalize_test where f_bit = b'110' or f_bit = b'110' or f_bit = b'110'",
                Collections.emptyMap(),
                Collections.emptyMap()));
            add(new TestCase(
                "select * from normalize_test where f_int = 0x1234 or f_int = 0x1234 or f_int = 0x1234",
                "select * from normalize_test where f_int = 0x1234 or f_int = 0x1234 or f_int = 0x1234",
                "select * from normalize_test where f_int = 0x1234 or f_int = 0x1234 or f_int = 0x1234",
                Collections.emptyMap(),
                Collections.emptyMap()));
            add(new TestCase(
                "select * from normalize_test where f_boolean = true or f_boolean = false",
                "select * from normalize_test where f_boolean = ? or f_boolean = ?",
                "select * from normalize_test where f_boolean = true or f_boolean = false",
                Collections.emptyMap(),
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.BIT).setValue(ByteString.copyFrom(new byte[] {1})).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.BIT).setValue(ByteString.copyFrom(new byte[] {0})).build());
                }}));
            add(new TestCase(
                "select * from normalize_test where f_varchar = 'aa'",
                "select * from normalize_test where f_varchar = ?",
                "select * from normalize_test where f_varchar = 'aa'",
                Collections.emptyMap(),
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("aa".getBytes())).build());
                }}));
            add(new TestCase(
                "select * from normalize_test where f_varchar = ?",
                "select * from normalize_test where f_varchar = ?",
                "select * from normalize_test where f_varchar = 'aa'",
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("aa".getBytes())).build());
                }},
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("aa".getBytes())).build());
                }}));
            add(new TestCase(
                "select * from normalize_test where f_varchar = ? or f_varchar = 'bb' or f_varchar = ?",
                "select * from normalize_test where f_varchar = ? or f_varchar = ? or f_varchar = ?",
                "select * from normalize_test where f_varchar = 'aa' or f_varchar = 'bb' or f_varchar = 'cc'",
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("aa".getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("cc".getBytes())).build());
                }},
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("aa".getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("bb".getBytes())).build());
                    put("2", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("cc".getBytes())).build());
                }}));
            add(new TestCase(
                "select * from normalize_test where f_varchar = ? or f_varchar = ? or f_varchar = 'cc' or f_varchar = 'dd' or f_varchar = ? or f_varchar = ?",
                "select * from normalize_test where f_varchar = ? or f_varchar = ? or f_varchar = ? or f_varchar = ? or f_varchar = ? or f_varchar = ?",
                "select * from normalize_test where f_varchar = 'aa' or f_varchar = 'bb' or f_varchar = 'cc' or f_varchar = 'dd' or f_varchar = 'ee' or f_varchar = 'ff'",
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("aa".getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("bb".getBytes())).build());
                    put("2", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("ee".getBytes())).build());
                    put("3", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("ff".getBytes())).build());
                }},
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("aa".getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("bb".getBytes())).build());
                    put("2", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("cc".getBytes())).build());
                    put("3", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("dd".getBytes())).build());
                    put("4", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("ee".getBytes())).build());
                    put("5", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("ff".getBytes())).build());
                }}));
            add(new TestCase(
                "select * from normalize_test where f_varchar = 'aa' or f_varchar = ? or f_varchar = ? or f_varchar = ? or f_varchar = 'ee' or f_varchar = ?",
                "select * from normalize_test where f_varchar = ? or f_varchar = ? or f_varchar = ? or f_varchar = ? or f_varchar = ? or f_varchar = ?",
                "select * from normalize_test where f_varchar = 'aa' or f_varchar = 'bb' or f_varchar = 'cc' or f_varchar = 'dd' or f_varchar = 'ee' or f_varchar = 'ff'",
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("bb".getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("cc".getBytes())).build());
                    put("2", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("dd".getBytes())).build());
                    put("3", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("ff".getBytes())).build());
                }},
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("aa".getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("bb".getBytes())).build());
                    put("2", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("cc".getBytes())).build());
                    put("3", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("dd".getBytes())).build());
                    put("4", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("ee".getBytes())).build());
                    put("5", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("ff".getBytes())).build());
                }}));
            add(new TestCase(
                "select 'aa', 'bb', 'cc' from normalize_test",
                "select ?, ?, ? from normalize_test",
                "select 'aa', 'bb', 'cc' from normalize_test",
                Collections.emptyMap(),
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("aa".getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("bb".getBytes())).build());
                    put("2", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("cc".getBytes())).build());
                }}));
            add(new TestCase(
                "select ?, 'bb', ? from normalize_test",
                "select ?, ?, ? from normalize_test",
                "select 'aa', 'bb', 'cc' from normalize_test",
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("aa".getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("cc".getBytes())).build());
                }},
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("aa".getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("bb".getBytes())).build());
                    put("2", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("cc".getBytes())).build());
                }}));
            add(new TestCase(
                "select ?, 'bb', ? from normalize_test where f_varchar = 'dd' or f_varchar = ? or f_varchar = ? or f_varchar = ? or f_varchar = 'hh' or f_varchar = ?",
                "select ?, ?, ? from normalize_test where f_varchar = ? or f_varchar = ? or f_varchar = ? or f_varchar = ? or f_varchar = ? or f_varchar = ?",
                "select 'aa', 'bb', 'cc' from normalize_test where f_varchar = 'dd' or f_varchar = 'ee' or f_varchar = 'ff' or f_varchar = 'gg' or f_varchar = 'hh' or f_varchar = 'ii'",
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("aa".getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("cc".getBytes())).build());
                    put("2", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("ee".getBytes())).build());
                    put("3", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("ff".getBytes())).build());
                    put("4", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("gg".getBytes())).build());
                    put("5", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("ii".getBytes())).build());
                }},
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("aa".getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("bb".getBytes())).build());
                    put("2", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("cc".getBytes())).build());
                    put("3", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("dd".getBytes())).build());
                    put("4", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("ee".getBytes())).build());
                    put("5", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("ff".getBytes())).build());
                    put("6", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("gg".getBytes())).build());
                    put("7", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("hh".getBytes())).build());
                    put("8", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("ii".getBytes())).build());
                }}));
            add(new TestCase(
                "select a, b from normalize_test order by 1 desc",
                "select a, b from normalize_test order by 1 desc",
                "select a, b from normalize_test order by 1 desc",
                Collections.emptyMap(),
                Collections.emptyMap()));
            add(new TestCase(
                "select a, b from normalize_test order by c desc",
                "select a, b from normalize_test order by c desc",
                "select a, b from normalize_test order by c desc",
                Collections.emptyMap(),
                Collections.emptyMap()));
            add(new TestCase(
                "select a, b from normalize_test group by 1",
                "select a, b from normalize_test group by 1",
                "select a, b from normalize_test group by 1",
                Collections.emptyMap(),
                Collections.emptyMap()));
            add(new TestCase(
                "select a, b from normalize_test group by c",
                "select a, b from normalize_test group by c",
                "select a, b from normalize_test group by c",
                Collections.emptyMap(),
                Collections.emptyMap()));
            add(new TestCase(
                "select * from normalize_test where f_tinyint = f_bigint",
                "select * from normalize_test where f_tinyint = f_bigint",
                "select * from normalize_test where f_tinyint = f_bigint",
                Collections.emptyMap(),
                Collections.emptyMap()));
            add(new TestCase(
                "select * from normalize_test where f_tinyint in (1, 2, 3) or f_tinyint not in (11, 22, 33)",
                "select * from normalize_test where f_tinyint in (?, ?, ?) or f_tinyint not in (?, ?, ?)",
                "select * from normalize_test where f_tinyint in (1, 2, 3) or f_tinyint not in (11, 22, 33)",
                Collections.emptyMap(),
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(1).getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(2).getBytes())).build());
                    put("2", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(3).getBytes())).build());
                    put("3", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(11).getBytes())).build());
                    put("4", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(22).getBytes())).build());
                    put("5", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(33).getBytes())).build());
                }}));
            add(new TestCase(
                "select * from normalize_test where f_tinyint in (1, a) or f_tinyint not in (11, aa)",
                "select * from normalize_test where f_tinyint in (?, a) or f_tinyint not in (?, aa)",
                "select * from normalize_test where f_tinyint in (1, a) or f_tinyint not in (11, aa)",
                Collections.emptyMap(),
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(1).getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(11).getBytes())).build());
                }}));
            add(new TestCase(
                "select * from normalize_test where f_tinyint in (1, a, null) or f_tinyint not in (11, aa, null)",
                "select * from normalize_test where f_tinyint in (?, a, null) or f_tinyint not in (?, aa, null)",
                "select * from normalize_test where f_tinyint in (1, a, null) or f_tinyint not in (11, aa, null)",
                Collections.emptyMap(),
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(1).getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(11).getBytes())).build());
                }}));
            add(new TestCase(
                "select * from normalize_test where f_tinyint in (?, ?, 3, ?, ?, a, ?, null, ?) or f_tinyint not in (?, ?, 33, ?, ?, aa, ?, null, ?)",
                "select * from normalize_test where f_tinyint in (?, ?, ?, ?, ?, a, ?, null, ?) or f_tinyint not in (?, ?, ?, ?, ?, aa, ?, null, ?)",
                "select * from normalize_test where f_tinyint in (1, 2, 3, 4, 5, a, 'b', null, 'c') or f_tinyint not in (11, 22, 33, 44, 55, aa, 'bb', null, 'cc')",
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(1).getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(2).getBytes())).build());
                    put("2", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(4).getBytes())).build());
                    put("3", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(5).getBytes())).build());
                    put("4", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("b".getBytes())).build());
                    put("5", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("c".getBytes())).build());
                    put("6", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(11).getBytes())).build());
                    put("7", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(22).getBytes())).build());
                    put("8", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(44).getBytes())).build());
                    put("9", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(55).getBytes())).build());
                    put("10", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("bb".getBytes())).build());
                    put("11", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("cc".getBytes())).build());
                }},
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(1).getBytes())).build());
                    put("1", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(2).getBytes())).build());
                    put("2", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(3).getBytes())).build());
                    put("3", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(4).getBytes())).build());
                    put("4", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(5).getBytes())).build());
                    put("5", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("b".getBytes())).build());
                    put("6", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("c".getBytes())).build());
                    put("7", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(11).getBytes())).build());
                    put("8", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(22).getBytes())).build());
                    put("9", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(33).getBytes())).build());
                    put("10", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(44).getBytes())).build());
                    put("11", Query.BindVariable.newBuilder().setType(Query.Type.INT32).setValue(ByteString.copyFrom(String.valueOf(55).getBytes())).build());
                    put("12", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("bb".getBytes())).build());
                    put("13", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("cc".getBytes())).build());
                }}));
            add(new TestCase(
                "select CAST('test' AS CHAR(60))",
                "select cast(? as CHAR(60))",
                "select cast('test' as CHAR(60))",
                Collections.emptyMap(),
                new LinkedHashMap<String, Query.BindVariable>() {{
                    put("0", Query.BindVariable.newBuilder().setType(Query.Type.VARBINARY).setValue(ByteString.copyFrom("test".getBytes())).build());
                }}));
        }};
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
