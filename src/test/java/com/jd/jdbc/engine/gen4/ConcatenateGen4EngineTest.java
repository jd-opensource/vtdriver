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
import com.jd.jdbc.IExecute;
import com.jd.jdbc.context.VtContext;
import com.jd.jdbc.engine.FakePrimitive;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.util.TestResult;
import com.jd.jdbc.engine.vcursor.NoopVCursor;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.util.threadpool.impl.VtQueryExecutorService;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ConcatenateGen4EngineTest extends BaseTest {

    public VtResultSet r(String names, String types, String... rows) {
        return TestResult.makeTestResult(TestResult.makeTestFields(names, types), rows);
    }

    @Before
    public void init() {
        VtQueryExecutorService.initialize(null, null, null, null);
    }

    @Test
    public void testConcatenateNoErrors() throws InterruptedException {
        VtResultSet r1 = r("id|col1|col2", "int64|varbinary|varbinary", "1|a1|b1", "2|a2|b2");
        VtResultSet r2 = r("id|col1|col2", "int32|varbinary|varbinary", "1|a1|b1", "2|a2|b2");
        VtResultSet combinedResult = r1;
        r1.getRows().addAll(r2.getRows());

        List<TestCase> testCases = new ArrayList<>();

        // 测试用例1
        List<VtResultSet> inputs1 = new ArrayList<>();
        inputs1.add(r("id1|col11|col12", "int64|varbinary|varbinary"));
        inputs1.add(r("id2|col21|col22", "int64|varbinary|varbinary"));
        inputs1.add(r("id3|col31|col32", "int64|varbinary|varbinary"));
        testCases.add(new TestCase("empty results", inputs1, r("id1|col11|col12", "int64|varbinary|varbinary")));

        // 测试用例2
        List<VtResultSet> inputs2 = new ArrayList<>();
        inputs2.add(r("myid|mycol1|mycol2", "int64|varchar|varbinary", "11|m1|n1", "22|m2|n2"));
        inputs2.add(r("id|col1|col2", "int64|varchar|varbinary", "1|a1|b1", "2|a2|b2"));
        inputs2.add(r("id2|col2|col3", "int64|varchar|varbinary", "3|a3|b3"));
        inputs2.add(r("id2|col2|col3", "int64|varchar|varbinary", "4|a4|b4"));
        testCases.add(new TestCase("2 non empty result", inputs2, r("myid|mycol1|mycol2", "int64|varchar|varbinary", "11|m1|n1", "22|m2|n2", "1|a1|b1", "2|a2|b2", "3|a3|b3", "4|a4|b4")));

//         测试用例3
        List<VtResultSet> inputs3 = new ArrayList<>();
        inputs3.add(r("id|col1|col2", "int64|varbinary|varbinary", "1|a1|b1", "2|a2|b2"));
        inputs3.add(r("id|col1|col2", "int64|varbinary|varbinary", "1|a1|b1", "2|a2|b2"));
        inputs3.add(r("id|col3|col4", "int64|varchar|varbinary", "1|a1|b1", "2|a2|b2"));
        testCases.add(new TestCase("column field type does not match for name", inputs3, "merging field of different types is not supported"));

        // 测试用例4
        List<VtResultSet> inputs4 = new ArrayList<>();
        inputs4.add(r("id|col1|col2", "int64|varbinary|varbinary", "1|a1|b1", "2|a2|b2"));
        inputs4.add(r("id|col1|col2", "int32|varbinary|varbinary", "1|a1|b1", "2|a2|b2"));
        testCases.add(new TestCase("ignored field types - ignored", inputs4, combinedResult, null, Lists.newArrayList(0)));

        // 测试用例5
        List<VtResultSet> inputs5 = new ArrayList<>();
        inputs5.add(r("id|col1|col2", "int64|varchar|varchar", "1|a1|b1", "2|a2|b2"));
        inputs5.add(r("id|col1|col2", "int64|varchar|varchar", "1|a1|b1", "2|a2|b2"));
        inputs5.add(r("id|col3|col4|col5", "int64|varchar|varchar|int32", "1|a1|b1|5", "2|a2|b2|6"));
        testCases.add(new TestCase("input source has different column count", inputs5, "The used SELECT statements have a different number of columns"));

        // 测试用例6
        List<VtResultSet> inputs6 = new ArrayList<>();
        inputs6.add(r("myid|mycol1|mycol2", "int64|varchar|varbinary"));
        inputs6.add(r("id|col1|col2", "int64|varchar|varbinary", "1|a1|b1", "2|a2|b2"));
        testCases.add(new TestCase("1 empty result and 1 non empty result", inputs6, r("myid|mycol1|mycol2", "int64|varchar|varbinary", "1|a1|b1", "2|a2|b2")));
        CountDownLatch latch = new CountDownLatch(testCases.size());
        ExecutorService executorService = new ThreadPoolExecutor(
            10, 10, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>());
        AtomicBoolean errorFlag = new AtomicBoolean(true);
        for (TestCase tc : testCases) {
            List<PrimitiveEngine> sources = new ArrayList<>();
            for (VtResultSet input : tc.inputs) {
                // input is added twice, since the first one is used by execute and the next by stream execute
                sources.add(new FakePrimitive(Lists.newArrayList(input, input)));
            }
            ConcatenateGen4Engine concatenate = new ConcatenateGen4Engine(sources, tc.ignoreTypes);
            executorService.execute(() -> {
                try {
                    IExecute.ExecuteMultiShardResponse qr = concatenate.execute(VtContext.background(), new NoopVCursor(), null, true);
                    try {
                        Assert.assertEquals(printFail(tc.testName + "  is [FAIL]"), tc.expectedResult, qr.getVtRowList());
                        printOk(tc.testName + " is[OK]");
                    } catch (AssertionError e) {
                        e.printStackTrace();
                        errorFlag.set(false);
                    }

                } catch (Exception e) {
                    if (tc.expectedError != null && !e.getMessage().contains(tc.expectedError) || tc.expectedError == null) {
                        errorFlag.set(false);
                        System.out.println(printFail(tc.testName + "  is [FAIL]" + e.getMessage()));
                        e.printStackTrace();
                    } else {
                        printOk(tc.testName + " is[OK]");
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        if (!latch.await(10, TimeUnit.SECONDS)) {
            Assert.fail("testConcatenateNoErrors is [FAIL] timeout");
        }
        Assert.assertTrue(printFail("testConcatenateNoErrors is [FAIL]"), errorFlag.get());

    }

    @Test
    public void testConcatenateWithErrors() {
        SQLException strFailed = new SQLException("failed");
        VtResultSet fake = r("id|col1|col2", "int64|varchar|varbinary", "1|a1|b1", "2|a2|b2");
        FakePrimitive fake1 = new FakePrimitive(
            Lists.newArrayList(fake, fake)
        );
        FakePrimitive fake2 = new FakePrimitive(null, strFailed);
        FakePrimitive fake3 = new FakePrimitive(
            Lists.newArrayList(fake, fake)
        );
        List<PrimitiveEngine> primitives = Lists.newArrayList(fake1, fake2, fake3);
        ConcatenateGen4Engine concatenate = new ConcatenateGen4Engine(primitives, new ArrayList<>());
        try {
            concatenate.execute(VtContext.background(), new NoopVCursor(), null, true);
        } catch (SQLException e) {
            Assert.assertEquals(strFailed, e);
        }

        concatenate = new ConcatenateGen4Engine(primitives, new ArrayList<>());
        try {
            concatenate.execute(VtContext.background(), new NoopVCursor(), null, true);
        } catch (SQLException e) {
            Assert.assertEquals(strFailed, e);
        }
    }

    @AllArgsConstructor
    @Data
    class TestCase {
        private String testName;

        private List<VtResultSet> inputs;

        private VtResultSet expectedResult;

        private String expectedError;

        private List<Integer> ignoreTypes;

        TestCase(String testName, List<VtResultSet> inputs, VtResultSet expectedResult) {
            this.testName = testName;
            this.inputs = inputs;
            this.expectedResult = expectedResult;

        }

        TestCase(String testName, List<VtResultSet> inputs, String expectedError) {
            this.testName = testName;
            this.inputs = inputs;
            this.expectedError = expectedError;

        }

        TestCase(String testName, List<VtResultSet> inputs, VtResultSet expectedResult, List<Integer> ignoreTypes) {
            this.testName = testName;
            this.inputs = inputs;
            this.expectedResult = expectedResult;
            this.ignoreTypes = ignoreTypes;
        }
    }
}
