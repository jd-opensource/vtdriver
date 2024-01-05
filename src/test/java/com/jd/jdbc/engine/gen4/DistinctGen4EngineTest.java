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

import com.jd.BaseTest;
import com.jd.jdbc.context.VtContext;
import com.jd.jdbc.engine.FakePrimitive;
import com.jd.jdbc.engine.util.TestResult;
import com.jd.jdbc.engine.vcursor.NoopVCursor;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class DistinctGen4EngineTest extends BaseTest {

    @Test
    public void testDistinct() {
        List<TestCase> testCases = Arrays.asList(
            new TestCase("empty", r("id1|col11|col12", "int64|varbinary|varbinary"), r("id1|col11|col12", "int64|varbinary|varbinary")),
            new TestCase("int64 numbers", r("myid", "int64", "0", "1", "1", "null", "null"), r("myid", "int64", "0", "1", "null")),
            new TestCase("int64 numbers, two columns", r("a|b", "int64|int64", "0|0", "1|1", "1|1", "null|null", "null|null", "1|2"), r("a|b", "int64|int64", "0|0", "1|1", "null|null", "1|2")),
            new TestCase("int64 numbers, two columns", r("a|b", "int64|int64", "3|3", "3|3", "3|4", "5|1", "5|1"), r("a|b", "int64|int64", "3|3", "3|4", "5|1")),
            new TestCase("float64 columns designed to produce the same hashcode but not be equal", r("a|b", "float64|float64", "0.1|0.2", "0.1|0.3", "0.1|0.4", "0.1|0.5"), r("a|b", "float64|float64", "0.1|0.2", "0.1|0.3", "0.1|0.4", "0.1|0.5")),
            new TestCase("varchar columns without collations", r("myid", "varchar", "monkey", "horse"), "text type with an unknown/unsupported collation cannot be hashed")
//            new TestCase("varchar columns with collations", null, r("myid", "varchar", "monkey", "horse", "Horse", "Monkey", "horses", "MONKEY"), r("myid", "varchar", "monkey", "horse", "horses")),
//            new TestCase("mixed columns", null, r("myid|id", "varchar|int64", "monkey|1", "horse|1", "Horse|1", "Monkey|1", "horses|1", "MONKEY|2"), r("myid|id", "varchar|int64", "monkey|1", "horse|1", "horses|1", "MONKEY|2"))
            );

        for (TestCase tc : testCases) {
            List<CheckCol> checkCols = new ArrayList<>();
            if (tc.inputs.getRows().size() > 0) {
                for (int i = 0; i < tc.inputs.getRows().get(0).size(); i++) {
                    checkCols.add(new CheckCol(i, -1));
                }
            }

            // Execute
            try {
                FakePrimitive source = new FakePrimitive(Collections.singletonList(tc.inputs));
                DistinctGen4Engine distinct = new DistinctGen4Engine(source, checkCols, false);
                VtResultSet qr = (VtResultSet) distinct.execute(VtContext.background(), new NoopVCursor(), null, true).getVtRowList();
                if (tc.expectedError == null) {
                    List<List<VtResultValue>> got = qr.getRows();
                    List<List<VtResultValue>> expected = tc.expectedResult.getRows();
                    Assert.assertEquals("result not what correct", expected, got);
                } else {
                    Assert.fail();
                }
            } catch (SQLException exception) {
                Assert.assertEquals(tc.expectedError, exception.getMessage());
            }
            printOk("testDistinct is [OK],case name = " + tc.testName);

            // StreamExecute
        }
    }

    private static class TestCase {
        private String testName;

        private VtResultSet inputs;

        private List<Object> collations;

        private VtResultSet expectedResult;

        private String expectedError;

        TestCase(String testName, VtResultSet inputs, VtResultSet expectedResult) {
            this.testName = testName;
            this.inputs = inputs;
            this.expectedResult = expectedResult;
        }

        TestCase(String testName, VtResultSet inputs, String expectedError) {
            this.testName = testName;
            this.inputs = inputs;
            this.expectedError = expectedError;
        }

        TestCase(String testName, List<Object> collations, VtResultSet inputs, VtResultSet expectedResult) {
            this.testName = testName;
            this.collations = collations;
            this.inputs = inputs;
            this.expectedResult = expectedResult;
        }
    }

    private VtResultSet r(String names, String types, String... rows) {
        return TestResult.makeTestResult(TestResult.makeTestFields(names, types), rows);
    }
}