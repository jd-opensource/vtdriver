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
import com.jd.jdbc.context.VtContext;
import com.jd.jdbc.engine.Engine.AggregateOpcodeG4;
import com.jd.jdbc.engine.FakePrimitive;
import com.jd.jdbc.engine.util.TestResult;
import com.jd.jdbc.engine.vcursor.NoopVCursor;
import com.jd.jdbc.sqltypes.VtResultSet;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import org.junit.Assert;
import org.junit.Test;

public class ScalarAggregateGen4EngineTest extends BaseTest {

    @Test
    public void testEmptyRows() throws SQLException {
        @AllArgsConstructor
        class TestCase {
            AggregateOpcodeG4 opcode;

            AggregateOpcodeG4 origOpcode;

            String expectedVal;

            String expectedTyp;

        }
        List<TestCase> testCases = new ArrayList<>();
        testCases.add(new TestCase(AggregateOpcodeG4.AggregateCountDistinct, null, "0", "INT64"));
        testCases.add(new TestCase(AggregateOpcodeG4.AggregateCount, null, "0", "INT64"));
        testCases.add(new TestCase(AggregateOpcodeG4.AggregateSumDistinct, null, "null", "DECIMAL"));
        testCases.add(new TestCase(AggregateOpcodeG4.AggregateSum, null, "null", "INT64"));
        testCases.add(new TestCase(AggregateOpcodeG4.AggregateSum, AggregateOpcodeG4.AggregateCount, "0", "INT64"));
        testCases.add(new TestCase(AggregateOpcodeG4.AggregateMax, null, "null", "INT64"));
        testCases.add(new TestCase(AggregateOpcodeG4.AggregateMin, null, "null", "INT64"));
        for (TestCase test : testCases) {
            Query.Field[] field = TestResult.makeTestFields(AbstractAggregateGen4.printOpcode(test.opcode), "int64");
            VtResultSet resultSet = TestResult.makeTestResult(field);
            FakePrimitive fp = new FakePrimitive(Lists.newArrayList(resultSet));
            AbstractAggregateGen4.AggregateParams aggr = new AbstractAggregateGen4.AggregateParams(test.opcode, 0, AbstractAggregateGen4.printOpcode(test.opcode));
            aggr.setOrigOpcode(test.origOpcode);
            ScalarAggregateGen4Engine oa = new ScalarAggregateGen4Engine(true, Lists.newArrayList(aggr), fp);
            VtResultSet outResult = (VtResultSet) oa.execute(VtContext.background(), new NoopVCursor(), null, false).getVtRowList();
            Query.Field[] wantField = TestResult.makeTestFields(AbstractAggregateGen4.printOpcode(test.opcode), test.expectedTyp);
            VtResultSet wantResult = TestResult.makeTestResult(wantField, test.expectedVal);
            Assert.assertEquals(printFail(AbstractAggregateGen4.printOpcode(test.opcode) + " test is FAIL"), wantResult, outResult);
            printOk(AbstractAggregateGen4.printOpcode(test.opcode) + " test is [OK] ");
        }
    }
}
