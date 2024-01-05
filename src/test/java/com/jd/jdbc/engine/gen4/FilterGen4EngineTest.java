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

import com.jd.jdbc.IExecute;
import com.jd.jdbc.engine.FakePrimitive;
import com.jd.jdbc.engine.util.TestResult;
import com.jd.jdbc.evalengine.Comparisons;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import io.vitess.proto.Query;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class FilterGen4EngineTest {

    @Test
    public void testFilterPass() throws SQLException {
        @Getter
        @Setter
        @AllArgsConstructor
        class TestCase {
            String name;

            VtResultSet res;

            List<List<VtResultValue>> expectResult;
        }

        List<TestCase> testCases = new ArrayList<>(2);
        testCases.add(
            new TestCase("int32", TestResult.makeTestResult(TestResult.makeTestFields("a|b", "int32|int32"), "0|1", "1|0", "2|3"),
                Collections.singletonList(Arrays.asList(new VtResultValue(1, Query.Type.INT32), new VtResultValue(0, Query.Type.INT32)))));
        testCases.add(
            new TestCase("uint64_int64", TestResult.makeTestResult(TestResult.makeTestFields("a|b", "uint64|int64"), "0|1", "1|0", "2|3"),
                Collections.singletonList(Arrays.asList(new VtResultValue(new BigInteger("1"), Query.Type.UINT64), new VtResultValue(0L, Query.Type.INT64)))));

        Comparisons.ComparisonExpr predicate = new Comparisons.ComparisonExpr(new Comparisons.CompareGT(), new EvalEngine.Column(0), new EvalEngine.Column(1));
        for (TestCase tc : testCases) {
            FilterGen4Engine filterGen4Engine = new FilterGen4Engine();
            filterGen4Engine.setPredicate(predicate);
            filterGen4Engine.setInput(new FakePrimitive(Collections.singletonList(tc.getRes())));
            VtResultSet rowList = (VtResultSet) filterGen4Engine.execute(null, null, null, false).getVtRowList();

            Assert.assertEquals(tc.getExpectResult(), rowList.getRows());
        }
    }

    @Test
    @Ignore
    public void testFilterMixedFail() throws SQLException {
        @Getter
        @Setter
        @AllArgsConstructor
        class TestCase {
            String name;

            VtResultSet res;

            String expErr;
        }

        Comparisons.ComparisonExpr predicate = new Comparisons.ComparisonExpr(new Comparisons.CompareGT(), new EvalEngine.Column(0), new EvalEngine.Column(1));

        List<TestCase> testCases = new ArrayList<>();
        testCases.add(
            new TestCase("uint64_int32", TestResult.makeTestResult(TestResult.makeTestFields("a|b", "uint64|int32"), "0|1", "1|0", "2|3"), "unsupported: cannot compare UINT64 and INT32"));

        for (TestCase tc : testCases) {
            FilterGen4Engine filterGen4Engine = new FilterGen4Engine();
            filterGen4Engine.setPredicate(predicate);
            filterGen4Engine.setInput(new FakePrimitive(Collections.singletonList(tc.getRes())));

            try {
                IExecute.ExecuteMultiShardResponse response = filterGen4Engine.execute(null, null, null, false);
            } catch (SQLException e) {
                Assert.assertEquals(tc.expErr, e.getMessage());
            }
        }
    }
}
