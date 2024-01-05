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

package com.jd.jdbc.engine.gen4;

import com.google.common.collect.Lists;
import com.jd.BaseTest;
import com.jd.jdbc.sqltypes.VtResultValue;
import io.vitess.proto.Query;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.ToString;
import org.junit.Assert;
import org.junit.Test;

public class VitessCompareTest extends BaseTest {
    @Test
    public void testComparer() throws SQLException {
        @AllArgsConstructor
        @ToString
        class TestCase {
            VitessCompare compare;

            List<VtResultValue> row1;

            List<VtResultValue> row2;

            int output;
        }

        List<TestCase> testCases = new ArrayList<>();
        VitessCompare compare1 = new VitessCompare(0, -1, true, 0);
        VtResultValue r11 = new VtResultValue(BigInteger.valueOf(23), Query.Type.INT64);
        VtResultValue r12 = new VtResultValue(BigInteger.valueOf(34), Query.Type.INT64);
        TestCase testCase1 = new TestCase(compare1, Lists.newArrayList(r11), Lists.newArrayList(r12), 1);
        testCases.add(testCase1);

        VitessCompare compare2 = new VitessCompare(0, -1, false, 0);
        VtResultValue r21 = new VtResultValue(BigInteger.valueOf(23), Query.Type.INT64);
        VtResultValue r22 = new VtResultValue(BigInteger.valueOf(23), Query.Type.INT64);
        TestCase testCase2 = new TestCase(compare2, Lists.newArrayList(r21), Lists.newArrayList(r22), 0);
        testCases.add(testCase2);

        VitessCompare compare3 = new VitessCompare(0, -1, false, 0);
        VtResultValue r31 = new VtResultValue(BigInteger.valueOf(23), Query.Type.INT64);
        VtResultValue r32 = new VtResultValue(BigInteger.valueOf(12), Query.Type.INT64);
        TestCase testCase3 = new TestCase(compare3, Lists.newArrayList(r31), Lists.newArrayList(r32), 1);
        testCases.add(testCase3);

        VitessCompare compare4 = new VitessCompare(1, 0, false, 0);
        VtResultValue r411 = new VtResultValue(BigInteger.valueOf(23), Query.Type.INT64);
        VtResultValue r412 = new VtResultValue("b", Query.Type.VARCHAR);
        VtResultValue r421 = new VtResultValue(BigInteger.valueOf(34), Query.Type.INT64);
        VtResultValue r422 = new VtResultValue("a", Query.Type.VARCHAR);
        TestCase testCase4 = new TestCase(compare4, Lists.newArrayList(r411, r412), Lists.newArrayList(r421, r422), -1);
        testCases.add(testCase4);

        VitessCompare compare5 = new VitessCompare(1, 0, true, 0);
        VtResultValue r511 = new VtResultValue(BigInteger.valueOf(23), Query.Type.INT64);
        VtResultValue r512 = new VtResultValue("A", Query.Type.VARCHAR);
        VtResultValue r521 = new VtResultValue(BigInteger.valueOf(23), Query.Type.INT64);
        VtResultValue r522 = new VtResultValue("a", Query.Type.VARCHAR);
        TestCase testCase5 = new TestCase(compare5, Lists.newArrayList(r511, r512), Lists.newArrayList(r521, r522), 0);
        testCases.add(testCase5);

        for (TestCase testCase : testCases) {
            int got = testCase.compare.compare(testCase.row1, testCase.row2);
            Assert.assertEquals(testCase.output, got);
            printOk(testCase + " test is [OK] ");
        }
    }
}