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

package com.jd.jdbc.evalengine;

import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtValue;
import io.vitess.proto.Query;
import java.math.BigInteger;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.junit.Assert;
import org.junit.Test;

public class ArithmeticTest {

    @Test
    public void testDecimalFormat() {
        DecimalFormat decimalFormat = new DecimalFormat("0.################");
        //decimalFormat.setGroupingUsed(false);
        decimalFormat.setDecimalSeparatorAlwaysShown(false);

        Double a = 2024.0d;
        String year = decimalFormat.format(a);
        System.out.println(year);
    }

    /**
     * 第七个测试用例与vitess本身有差别，vitess中，uint64和int64相加，会被强转成uint64+uint64，
     * -1会被转成uint64的最大值。
     * 因为java不存在UINT64，用biginteger可以控制范围，但是-1从long转成uint64，还是-1，所以结果不一样。
     *
     * @throws Exception
     */
    @Test
    public void testNullSafeAdd() throws Exception {
        List<NSATestCase> testCases = new ArrayList<NSATestCase>() {{
            // All nulls.
            add(new NSATestCase(VtResultValue.NULL, VtResultValue.NULL, VtResultValue.newVtResultValue(Query.Type.INT64, 0L), null));
            // First value null.
            add(new NSATestCase(VtResultValue.newVtResultValue(Query.Type.INT32, 1), VtResultValue.NULL, VtResultValue.newVtResultValue(Query.Type.INT64, 1L), null));
            // Second value null.
            add(new NSATestCase(VtResultValue.NULL, VtResultValue.newVtResultValue(Query.Type.INT32, 1), VtResultValue.newVtResultValue(Query.Type.INT64, 1L), null));
            // Normal case.
            add(new NSATestCase(VtResultValue.newVtResultValue(Query.Type.INT64, 1L), VtResultValue.newVtResultValue(Query.Type.INT64, 2L), VtResultValue.newVtResultValue(Query.Type.INT64, 3L),
                null));
            // Make sure underlying error is returned for LHS.
            //add(new NSATestCase(VtResultValue.newVtResultValue(Query.Type.INT64, "1.2".getBytes()), VtResultValue.newVtResultValue(Query.Type.INT64, Long.valueOf(2).toString().getBytes()), VtResultValue.NULL, null));
            // Make sure underlying error is returned for RHS.
            //add(new NSATestCase(VtResultValue.newVtResultValue(Query.Type.INT64, Long.valueOf(2).toString().getBytes()), VtResultValue.newVtResultValue(Query.Type.INT64, "1.2".getBytes()), VtResultValue.NULL, null));
            // Make sure underlying error is returned while adding.
            add(new NSATestCase(VtResultValue.newVtResultValue(Query.Type.INT64, -1L), VtResultValue.newVtResultValue(Query.Type.UINT64, BigInteger.valueOf(2)),
                VtResultValue.newVtResultValue(Query.Type.UINT64, BigInteger.valueOf(1)), null));
            // Make sure underlying error is returned while converting.
            add(new NSATestCase(VtResultValue.newVtResultValue(Query.Type.FLOAT64, 1d), VtResultValue.newVtResultValue(Query.Type.FLOAT64, 2d), VtResultValue.newVtResultValue(Query.Type.INT64, 3L),
                null));
        }};

        int i = 0;
        for (NSATestCase testCase : testCases) {
            i++;
            printInfo("NO." + i + " Test case: [" + testCase.toString() + "]");
            try {
                VtResultValue result = EvalEngine.nullSafeAdd(testCase.value1, testCase.value2, Query.Type.INT64);
                printInfo("NO." + i + " Test Result: [" + result.toString() + "]");
                if (VtResultValueCompare(result, testCase.out)) {
                    printOk("NO." + i + " Test case is [OK]");
                }
            } catch (Exception e) {
                System.out.println("NO." + i + " Test Error: [" + e.getMessage() + "]");
                if (testCase.errorMessage != null && testCase.errorMessage.equals(e.getMessage())) {
                    printOk("NO." + i + " Test case is [OK]");
                } else {
                    printFail("NO." + i + " Test case is [FAIL]");
                }
            }
        }
    }

    public void nullSafeCompare(List<NSCTestCase> testCases) {
        int i = 0;
        for (NSCTestCase testCase : testCases) {
            i++;
            printInfo("NO." + i + " Test case: [" + testCase.toString() + "]");
            try {
                int result = EvalEngine.nullSafeCompare(testCase.value1, testCase.value2);
                Assert.assertEquals(testCase.out.intValue(), result);
            } catch (Exception e) {
                Assert.assertNotNull("expected no error, but is: " + e.getMessage(), testCase.errorMessage);
                Assert.assertEquals(testCase.errorMessage, e.getMessage());
            }
            printOk("NO." + i + " Test case is [OK]");
        }
    }

    @Test
    public void nullSafeCompareTest() throws SQLException {
        List<NSCTestCase> testCases = new ArrayList<NSCTestCase>(){{
            // All nulls.
            add(new NSCTestCase(VtResultValue.NULL, VtResultValue.NULL, 0, null));
            // LHS null.
            add(new NSCTestCase(VtResultValue.NULL, VtResultValue.newVtResultValue(Query.Type.INT32, 1L), -1, null));
            // RHS null.
            add(new NSCTestCase(VtResultValue.newVtResultValue(Query.Type.INT32, 1L), VtResultValue.NULL, 1, null));
            // LHS Text
            add(new NSCTestCase(VtResultValue.newVtResultValue(Query.Type.VARCHAR, "abcd"), VtResultValue.newVtResultValue(Query.Type.VARCHAR, "abcd"), 0, null));
            // Make sure underlying error is returned for LHS.
            //add(new NSCTestCase(VtResultValue.newVtResultValue(Query.Type.INT64, "1.2".getBytes()), VtResultValue.newVtResultValue(Query.Type.INT64, Long.valueOf(2).toString().getBytes()), null, "For input string: \"1.2\""));
            // Make sure underlying error is returned for RHS.
            //add(new NSCTestCase(VtResultValue.newVtResultValue(Query.Type.INT64, Long.valueOf(2).toString().getBytes()), VtResultValue.newVtResultValue(Query.Type.INT64, "1.2".getBytes()), null, "For input string: \"1.2\""));
            // Numeric equal.
            add(new NSCTestCase(VtResultValue.newVtResultValue(Query.Type.INT64, 1L), VtResultValue.newVtResultValue(Query.Type.UINT64, BigInteger.valueOf(1)), 0, ""));
            // Numeric unequal.
            add(new NSCTestCase(VtResultValue.newVtResultValue(Query.Type.INT64, 1L), VtResultValue.newVtResultValue(Query.Type.UINT64, BigInteger.valueOf(2)), -1, ""));
            // Non-numeric equal
            add(new NSCTestCase(VtResultValue.newVtResultValue(Query.Type.VARBINARY, "abcd"), VtResultValue.newVtResultValue(Query.Type.BINARY, "abcd"), 0, null));
            // Non-numeric unequal
            add(new NSCTestCase(VtResultValue.newVtResultValue(Query.Type.VARBINARY, "abcd"), VtResultValue.newVtResultValue(Query.Type.BINARY, "bcde"), -1, null));
            // Date/Time types
            add(new NSCTestCase(VtResultValue.newVtResultValue(Query.Type.DATETIME, "1000-01-01 00:00:00".getBytes()), VtResultValue.newVtResultValue(Query.Type.BINARY, "1000-01-01 00:00:00".getBytes()), 0, null));
            // Date/Time types
            add(new NSCTestCase(VtResultValue.newVtResultValue(Query.Type.DATETIME, "2000-01-01 00:00:00".getBytes()), VtResultValue.newVtResultValue(Query.Type.BINARY, "1000-01-01 00:00:00".getBytes()), 1, null));
            // Date/Time types
            add(new NSCTestCase(VtResultValue.newVtResultValue(Query.Type.DATETIME, "1000-01-01 00:00:00".getBytes()), VtResultValue.newVtResultValue(Query.Type.BINARY, "2000-01-01 00:00:00".getBytes()), -1, null));
        }};

        nullSafeCompare(testCases);
    }

    /**
     * 使用VtResultValue后 NO.15 转为FLOAT64后，再转为VARBINARY 会变成64.0 与64不同
     *
     * @throws SQLException
     */
    @Test
    public void testCast() throws SQLException {
        List<CastTestCase> testCases = new ArrayList<CastTestCase>() {{
            add(new CastTestCase(Query.Type.VARCHAR, VtResultValue.NULL, VtResultValue.NULL, null));
            add(new CastTestCase(Query.Type.VARCHAR, VtResultValue.newVtResultValue(Query.Type.VARCHAR, "exact types"), VtResultValue.newVtResultValue(Query.Type.VARCHAR, "exact types"), null));
            add(new CastTestCase(Query.Type.INT64, VtResultValue.newVtResultValue(Query.Type.INT32, "32"), VtResultValue.newVtResultValue(Query.Type.INT64, "32"), null));
            add(new CastTestCase(Query.Type.INT24, VtResultValue.newVtResultValue(Query.Type.UINT64, "64"), VtResultValue.newVtResultValue(Query.Type.INT24, "64"), null));
            add(new CastTestCase(Query.Type.INT24, VtResultValue.newVtResultValue(Query.Type.VARCHAR, "bad int"), null, "For input string: \"bad int\""));
            add(new CastTestCase(Query.Type.UINT64, VtResultValue.newVtResultValue(Query.Type.UINT32, "32"), VtResultValue.newVtResultValue(Query.Type.UINT64, "32"), null));
            add(new CastTestCase(Query.Type.UINT24, VtResultValue.newVtResultValue(Query.Type.INT64, "64"), VtResultValue.newVtResultValue(Query.Type.INT24, "64"), null));
            add(new CastTestCase(Query.Type.UINT24, VtResultValue.newVtResultValue(Query.Type.INT64, "-1"), null, "wrong data type UINT24 for -1"));
            add(new CastTestCase(Query.Type.FLOAT64, VtResultValue.newVtResultValue(Query.Type.INT64, "64"), VtResultValue.newVtResultValue(Query.Type.FLOAT64, "64"), null));
            add(new CastTestCase(Query.Type.FLOAT32, VtResultValue.newVtResultValue(Query.Type.FLOAT64, "64"), VtResultValue.newVtResultValue(Query.Type.FLOAT32, "64"), null));
            add(new CastTestCase(Query.Type.FLOAT32, VtResultValue.newVtResultValue(Query.Type.DECIMAL, "1.24"), VtResultValue.newVtResultValue(Query.Type.FLOAT32, "1.24"), null));
            add(new CastTestCase(Query.Type.FLOAT64, VtResultValue.newVtResultValue(Query.Type.VARCHAR, "1.25"), VtResultValue.newVtResultValue(Query.Type.FLOAT64, "1.25"), null));
            add(new CastTestCase(Query.Type.FLOAT64, VtResultValue.newVtResultValue(Query.Type.VARCHAR, "bad float"), null, "For input string: \"bad float\""));
            add(new CastTestCase(Query.Type.VARCHAR, VtResultValue.newVtResultValue(Query.Type.INT64, "64"), VtResultValue.newVtResultValue(Query.Type.VARCHAR, "64"), null));
            add(new CastTestCase(Query.Type.VARBINARY, VtResultValue.newVtResultValue(Query.Type.FLOAT64, "64"), VtResultValue.newVtResultValue(Query.Type.VARBINARY, "64".getBytes()), null));
            add(new CastTestCase(Query.Type.VARBINARY, VtResultValue.newVtResultValue(Query.Type.DECIMAL, "1.24"), VtResultValue.newVtResultValue(Query.Type.VARBINARY, "1.24".getBytes()), null));
            add(new CastTestCase(Query.Type.VARBINARY, VtResultValue.newVtResultValue(Query.Type.VARCHAR, "1.25"), VtResultValue.newVtResultValue(Query.Type.VARBINARY, "1.25".getBytes()), null));
            add(new CastTestCase(Query.Type.VARCHAR, VtResultValue.newVtResultValue(Query.Type.VARBINARY, "valid string"), VtResultValue.newVtResultValue(Query.Type.VARCHAR, "valid string"), null));
            add(new CastTestCase(Query.Type.VARCHAR, VtResultValue.newVtResultValue(Query.Type.EXPRESSION, "bad string"), null, "EXPRESSION(bad string) cannot be cast to VARCHAR"));
        }};

        int i = 0;
        for (CastTestCase testCase : testCases) {
            i++;
            printInfo("NO." + i + " Test case: [" + testCase.toString() + "]");
            try {
                VtResultValue result = EvalEngine.cast(testCase.value1, testCase.getType());
                printInfo("NO." + i + " Test Result: [" + result.toString() + "]");
                if (VtResultValueCompare(result, testCase.out)) {
                    printOk("NO." + i + " Test case is [OK]");
                }
            } catch (Exception e) {
                System.out.println("NO." + i + " Test Error: [" + e.getMessage() + "]");
                if (testCase.errorMessage != null && testCase.errorMessage.equals(e.getMessage())) {
                    printOk("NO." + i + " Test case is [OK]");
                } else {
                    printFail("NO." + i + " Test case is [FAIL]");
                }
            }
        }
    }

    /**
     * 测试两数相加的方法，需要手动将方法从private改为public测试
     * 在测试用例中有两个测试用例和vitess不一样，分别是第8个和第10个
     * 其中，第8个是因为java没有unsigned这个值，所以int64转uint64不会从-1变为18446744073709551617
     * 第10个是因为Java的BigInteger理论上可以无限大，不会转换为FLOAT64，另外，虽然DOUBLE的值范围非常的大，
     * 但是他是指数表示法（会转变为科学计数法），限于精度的问题（小数点后17位），
     * 所以18446744073709551617会被表示为1.8446744073709552E19
     *
     * @throws SQLException
     */
    @Test
    public void testAddNumeric() throws SQLException {
        List<NumAddTestCase> testCases = new ArrayList<NumAddTestCase>() {{
            add(new NumAddTestCase(new EvalEngine.EvalResult(Long.valueOf(1), Query.Type.INT64),
                new EvalEngine.EvalResult(Long.valueOf(2), Query.Type.INT64),
                new EvalEngine.EvalResult(Long.valueOf(3), Query.Type.INT64)));

            add(new NumAddTestCase(new EvalEngine.EvalResult(Long.valueOf(1), Query.Type.INT64),
                new EvalEngine.EvalResult(BigInteger.valueOf(2), Query.Type.UINT64),
                new EvalEngine.EvalResult(BigInteger.valueOf(3), Query.Type.UINT64)));

            add(new NumAddTestCase(new EvalEngine.EvalResult(Long.valueOf(1), Query.Type.INT64),
                new EvalEngine.EvalResult(Double.valueOf(2), Query.Type.FLOAT64),
                new EvalEngine.EvalResult(Double.valueOf(3), Query.Type.FLOAT64)));

            add(new NumAddTestCase(new EvalEngine.EvalResult(BigInteger.valueOf(1), Query.Type.UINT64),
                new EvalEngine.EvalResult(BigInteger.valueOf(2), Query.Type.UINT64),
                new EvalEngine.EvalResult(BigInteger.valueOf(3), Query.Type.UINT64)));

            add(new NumAddTestCase(new EvalEngine.EvalResult(BigInteger.valueOf(1), Query.Type.UINT64),
                new EvalEngine.EvalResult(Double.valueOf(2), Query.Type.FLOAT64),
                new EvalEngine.EvalResult(Double.valueOf(3), Query.Type.FLOAT64)));

            add(new NumAddTestCase(new EvalEngine.EvalResult(Double.valueOf(1), Query.Type.FLOAT64),
                new EvalEngine.EvalResult(Double.valueOf(2), Query.Type.FLOAT64),
                new EvalEngine.EvalResult(Double.valueOf(3), Query.Type.FLOAT64)));
            // Int64 overflow
            add(new NumAddTestCase(new EvalEngine.EvalResult(9223372036854775807L, Query.Type.INT64),
                new EvalEngine.EvalResult(2L, Query.Type.INT64),
                new EvalEngine.EvalResult(9223372036854775809d, Query.Type.FLOAT64)));
            // Int64 underflow
            add(new NumAddTestCase(new EvalEngine.EvalResult(-9223372036854775807L, Query.Type.INT64),
                new EvalEngine.EvalResult(-2L, Query.Type.INT64),
                new EvalEngine.EvalResult(-9223372036854775809d, Query.Type.FLOAT64)));

            add(new NumAddTestCase(new EvalEngine.EvalResult(-1L, Query.Type.INT64),
                new EvalEngine.EvalResult(BigInteger.valueOf(2l), Query.Type.UINT64),
                new EvalEngine.EvalResult(BigInteger.valueOf(1l), Query.Type.UINT64)));
            // Uint64 overflow
            add(new NumAddTestCase(new EvalEngine.EvalResult(new BigInteger("18446744073709551615"), Query.Type.UINT64),
                new EvalEngine.EvalResult(BigInteger.valueOf(2l), Query.Type.UINT64),
                new EvalEngine.EvalResult(new BigInteger("18446744073709551617"), Query.Type.UINT64)));
        }};

        int i = 0;
        for (NumAddTestCase testCase : testCases) {
            i++;
            EvalEngine.EvalResult result = Arithmetic.addNumeric(testCase.value1, testCase.value2);
            printInfo("NO." + i + " Test case: [" + testCase + "]");
            printInfo("NO." + i + " Test Result: [" + result.toString() + "]");

            if (result.toString().equals(testCase.result.toString())) {
                printOk("NO." + i + " Test case is [OK]");
            } else {
                printFail("NO." + i + " Test case is [FAIL]");
            }
        }
    }

    /**
     * 注意，Java在转浮点数的科学表示法的时候，会转成1.2E而非1.2e, 并且浮点数一定会被转换程科学计数法
     *
     * @throws Exception
     */
    @Test
    public void testCastFromNumeric() throws Exception {
        List<CFNTestCase> testCases = new ArrayList<CFNTestCase>() {{
            add(new CFNTestCase(Query.Type.INT64, new EvalEngine.EvalResult(1l, Query.Type.INT64), VtValue.newVtValue(Query.Type.INT64, "1".getBytes())));
            add(new CFNTestCase(Query.Type.INT64, new EvalEngine.EvalResult(BigInteger.valueOf(1l), Query.Type.UINT64), VtValue.newVtValue(Query.Type.INT64, "1".getBytes())));
            add(new CFNTestCase(Query.Type.INT64, new EvalEngine.EvalResult(1.2e-16, Query.Type.FLOAT64), VtValue.newVtValue(Query.Type.INT64, "0".getBytes())));
            add(new CFNTestCase(Query.Type.UINT64, new EvalEngine.EvalResult(1l, Query.Type.INT64), VtValue.newVtValue(Query.Type.UINT64, BigInteger.valueOf(1l).toString().getBytes())));
            add(new CFNTestCase(Query.Type.UINT64, new EvalEngine.EvalResult(BigInteger.valueOf(1l), Query.Type.UINT64),
                VtValue.newVtValue(Query.Type.UINT64, BigInteger.valueOf(1l).toString().getBytes())));
            add(new CFNTestCase(Query.Type.UINT64, new EvalEngine.EvalResult(1.2e-16, Query.Type.FLOAT64), VtValue.newVtValue(Query.Type.UINT64, BigInteger.valueOf(0l).toString().getBytes())));
            add(new CFNTestCase(Query.Type.FLOAT64, new EvalEngine.EvalResult(1l, Query.Type.INT64), VtValue.newVtValue(Query.Type.FLOAT64, "1".getBytes())));
            add(new CFNTestCase(Query.Type.FLOAT64, new EvalEngine.EvalResult(BigInteger.valueOf(1l), Query.Type.UINT64), VtValue.newVtValue(Query.Type.FLOAT64, "1".getBytes())));
            add(new CFNTestCase(Query.Type.FLOAT64, new EvalEngine.EvalResult(1.2e-16, Query.Type.FLOAT64), VtValue.newVtValue(Query.Type.FLOAT64, "0.0000000000000001".getBytes())));
            add(new CFNTestCase(Query.Type.DECIMAL, new EvalEngine.EvalResult(1l, Query.Type.INT64), VtValue.newVtValue(Query.Type.DECIMAL, "1".getBytes())));
            add(new CFNTestCase(Query.Type.DECIMAL, new EvalEngine.EvalResult(BigInteger.valueOf(1l), Query.Type.UINT64), VtValue.newVtValue(Query.Type.DECIMAL, "1".getBytes())));
            // For float, we should not use scientific notation.
            add(new CFNTestCase(Query.Type.DECIMAL, new EvalEngine.EvalResult(0.00000000000000012d, Query.Type.FLOAT64), VtValue.newVtValue(Query.Type.DECIMAL, "0.0000000000000001".getBytes())));
        }};

        int i = 0;
        for (CFNTestCase testCase : testCases) {
            i++;
            VtValue result = Arithmetic.castFromNumeric(testCase.value, testCase.type);
            printInfo("NO." + i + " Test case: [" + testCase + "]");
            printInfo("NO." + i + " Test Result: [" + result.toString() + "]");

            if (result.toString().equals(testCase.out.toString())) {
                printOk("NO." + i + " Test case is [OK]");
            } else {
                printFail("NO." + i + " Test case is [FAIL]");
            }
        }
    }

    @Test
    public void testCompareNumeric() {
        List<CompNumTestCase> testCases = new ArrayList<CompNumTestCase>() {{
            add(new CompNumTestCase(new EvalEngine.EvalResult(1l, Query.Type.INT64), new EvalEngine.EvalResult(1l, Query.Type.INT64), 0));
            add(new CompNumTestCase(new EvalEngine.EvalResult(1l, Query.Type.INT64), new EvalEngine.EvalResult(2l, Query.Type.INT64), -1));
            add(new CompNumTestCase(new EvalEngine.EvalResult(2l, Query.Type.INT64), new EvalEngine.EvalResult(1l, Query.Type.INT64), 1));
            // Special case.
            add(new CompNumTestCase(new EvalEngine.EvalResult(-1l, Query.Type.INT64), new EvalEngine.EvalResult(BigInteger.valueOf(1l), Query.Type.UINT64), -1));
            add(new CompNumTestCase(new EvalEngine.EvalResult(1l, Query.Type.INT64), new EvalEngine.EvalResult(BigInteger.valueOf(1l), Query.Type.UINT64), 0));
            add(new CompNumTestCase(new EvalEngine.EvalResult(1l, Query.Type.INT64), new EvalEngine.EvalResult(BigInteger.valueOf(2l), Query.Type.UINT64), -1));
            add(new CompNumTestCase(new EvalEngine.EvalResult(2l, Query.Type.INT64), new EvalEngine.EvalResult(BigInteger.valueOf(1l), Query.Type.UINT64), 1));
            add(new CompNumTestCase(new EvalEngine.EvalResult(1l, Query.Type.INT64), new EvalEngine.EvalResult(1d, Query.Type.FLOAT64), 0));
            add(new CompNumTestCase(new EvalEngine.EvalResult(1l, Query.Type.INT64), new EvalEngine.EvalResult(2d, Query.Type.FLOAT64), -1));
            add(new CompNumTestCase(new EvalEngine.EvalResult(2l, Query.Type.INT64), new EvalEngine.EvalResult(1d, Query.Type.FLOAT64), 1));
            // Special case.
            add(new CompNumTestCase(new EvalEngine.EvalResult(BigInteger.valueOf(1l), Query.Type.UINT64), new EvalEngine.EvalResult(-1l, Query.Type.INT64), 1));
            add(new CompNumTestCase(new EvalEngine.EvalResult(BigInteger.valueOf(1l), Query.Type.UINT64), new EvalEngine.EvalResult(1l, Query.Type.INT64), 0));
            add(new CompNumTestCase(new EvalEngine.EvalResult(BigInteger.valueOf(1l), Query.Type.UINT64), new EvalEngine.EvalResult(2l, Query.Type.INT64), -1));
            add(new CompNumTestCase(new EvalEngine.EvalResult(BigInteger.valueOf(2l), Query.Type.UINT64), new EvalEngine.EvalResult(1l, Query.Type.INT64), 1));
            add(new CompNumTestCase(new EvalEngine.EvalResult(BigInteger.valueOf(1l), Query.Type.UINT64), new EvalEngine.EvalResult(BigInteger.valueOf(1l), Query.Type.UINT64), 0));
            add(new CompNumTestCase(new EvalEngine.EvalResult(BigInteger.valueOf(1l), Query.Type.UINT64), new EvalEngine.EvalResult(BigInteger.valueOf(2l), Query.Type.UINT64), -1));
            add(new CompNumTestCase(new EvalEngine.EvalResult(BigInteger.valueOf(2l), Query.Type.UINT64), new EvalEngine.EvalResult(BigInteger.valueOf(1l), Query.Type.UINT64), 1));
            add(new CompNumTestCase(new EvalEngine.EvalResult(BigInteger.valueOf(1l), Query.Type.UINT64), new EvalEngine.EvalResult(1d, Query.Type.FLOAT64), 0));
            add(new CompNumTestCase(new EvalEngine.EvalResult(BigInteger.valueOf(1l), Query.Type.UINT64), new EvalEngine.EvalResult(2d, Query.Type.FLOAT64), -1));
            add(new CompNumTestCase(new EvalEngine.EvalResult(BigInteger.valueOf(2l), Query.Type.UINT64), new EvalEngine.EvalResult(1d, Query.Type.FLOAT64), 1));
            add(new CompNumTestCase(new EvalEngine.EvalResult(1d, Query.Type.FLOAT64), new EvalEngine.EvalResult(1l, Query.Type.INT64), 0));
            add(new CompNumTestCase(new EvalEngine.EvalResult(1d, Query.Type.FLOAT64), new EvalEngine.EvalResult(2l, Query.Type.INT64), -1));
            add(new CompNumTestCase(new EvalEngine.EvalResult(2d, Query.Type.FLOAT64), new EvalEngine.EvalResult(1l, Query.Type.INT64), 1));
            add(new CompNumTestCase(new EvalEngine.EvalResult(1d, Query.Type.FLOAT64), new EvalEngine.EvalResult(BigInteger.valueOf(1l), Query.Type.UINT64), 0));
            add(new CompNumTestCase(new EvalEngine.EvalResult(1d, Query.Type.FLOAT64), new EvalEngine.EvalResult(BigInteger.valueOf(2l), Query.Type.UINT64), -1));
            add(new CompNumTestCase(new EvalEngine.EvalResult(2d, Query.Type.FLOAT64), new EvalEngine.EvalResult(BigInteger.valueOf(1l), Query.Type.UINT64), 1));
            add(new CompNumTestCase(new EvalEngine.EvalResult(1d, Query.Type.FLOAT64), new EvalEngine.EvalResult(1d, Query.Type.FLOAT64), 0));
            add(new CompNumTestCase(new EvalEngine.EvalResult(1d, Query.Type.FLOAT64), new EvalEngine.EvalResult(2d, Query.Type.FLOAT64), -1));
            add(new CompNumTestCase(new EvalEngine.EvalResult(2d, Query.Type.FLOAT64), new EvalEngine.EvalResult(1d, Query.Type.FLOAT64), 1));
        }};

        int i = 0;
        for (CompNumTestCase testCase : testCases) {
            i++;
            int result = Arithmetic.compareNumeric(testCase.value1, testCase.value2);
            printInfo("NO." + i + " Test case: [" + testCase + "]");
            printInfo("NO." + i + " Test Result: [" + result + "]");

            if (result == testCase.out) {
                printOk("NO." + i + " Test case is [OK]");
            } else {
                printFail("NO." + i + " Test case is [FAIL]");
            }
        }
    }

    private boolean VtResultValueCompare(VtResultValue v1, VtResultValue v2) {
        if (v1.isNull() && v2.isNull()) {
            return true;
        }
        if (v1.toString().equals(v2.toString())) {
            return v1.getVtType() == v2.getVtType();
        }
        return false;
    }

    private static void printOk(String message) {
        System.out.println("\033[1;32m" + message + "\033[0m");
    }

    private static void printFail(String message) {
        System.out.println("\033[1;31m" + message + "\033[0m");
    }

    private static void printInfo(String message) {
        System.out.println("\033[1;34m" + message + "\033[0m");
    }

    @AllArgsConstructor
    @Getter
    private class NSATestCase {
        VtResultValue value1;

        VtResultValue value2;

        VtResultValue out;

        String errorMessage;

        @Override
        public String toString() {
            String message = "Value1: [" + value1.toString() + "]; Value2: [" + value2.toString();
            if (out != null) {
                message += "]; Expected result: [" + out + "]";
            } else {
                message += "]; Expected error: [" + errorMessage + "]";
            }
            return message;
        }
    }

    // null safe compare test case
    @AllArgsConstructor
    @Getter
    private class NSCTestCase {
        VtResultValue value1;

        VtResultValue value2;

        Integer out;

        String errorMessage;

        @Override
        public String toString() {
            String message = "Value1: [" + value1.toString() + "]; Value2: [" + value2.toString();
            if (out != null) {
                message += "]; Expected result: [" + out + "]";
            } else {
                message += "]; Expected error: [" + errorMessage + "]";
            }
            return message;
        }
    }

    @AllArgsConstructor
    @Getter
    private class CastTestCase {
        Query.Type type;

        VtResultValue value1;

        VtResultValue out;

        String errorMessage;

        @Override
        public String toString() {
            String message = "Type: [" + type.toString() + "]; Value2: [" + value1.toString();
            if (out != null) {
                message += "]; Expected result: [" + out + "]";
            } else {
                message += "]; Expected error: [" + errorMessage + "]";
            }
            return message;
        }
    }

    @AllArgsConstructor
    @Getter
    private class NumAddTestCase {
        EvalEngine.EvalResult value1;

        EvalEngine.EvalResult value2;

        EvalEngine.EvalResult result;

        @Override
        public String toString() {
            return "Value1: [" + value1.toString() + "]; Value2: [" + value2.toString() + "]; Expected Result: [" + result.toString() + "]";
        }
    }

    // CastFromNumeric test case data struct
    @AllArgsConstructor
    @Getter
    private class CFNTestCase {
        Query.Type type;

        EvalEngine.EvalResult value;

        VtValue out;

        @Override
        public String toString() {
            return "Value: [" + value.toString() + "], Type: [" + type.toString() + "], Expected Value: [" + out.toString() + "]";
        }
    }

    // compareNumeric test case data struct
    @AllArgsConstructor
    @Getter
    private class CompNumTestCase {
        EvalEngine.EvalResult value1;

        EvalEngine.EvalResult value2;

        Integer out;

        @Override
        public String toString() {
            return "Value1: [" + value1.toString() + "], Value2: [" + value2.toString() + "], Expected Value: [" + out.toString() + "]";
        }
    }

}
