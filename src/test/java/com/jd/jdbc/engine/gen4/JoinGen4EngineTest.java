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
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.FakePrimitive;
import com.jd.jdbc.engine.util.TestResult;
import com.jd.jdbc.engine.vcursor.FakeVcursorUtil;
import com.jd.jdbc.engine.vcursor.NoopVCursor;
import com.jd.jdbc.sqltypes.SqlTypes;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.srvtopo.BindVariable;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.junit.Assert;
import org.junit.Test;

public class JoinGen4EngineTest extends BaseTest {

    @Test
    public void testJoinExecute() throws SQLException {
        Query.Field[] leftField = TestResult.makeTestFields("col1|col2|col3", "int64|varchar|varchar");
        VtResultSet leftResultSet = TestResult.makeTestResult(leftField, "1|a|aa", "2|b|bb", "3|c|cc");
        FakePrimitive leftPrim = new FakePrimitive(Lists.newArrayList(leftResultSet));
        Query.Field[] rightField = TestResult.makeTestFields("col4|col5|col6", "int64|varchar|varchar");
        List<VtResultSet> rightResultSet = new ArrayList<>();
        rightResultSet.add(TestResult.makeTestResult(rightField, "4|d|dd"));
        rightResultSet.add(TestResult.makeTestResult(rightField));
        rightResultSet.add(TestResult.makeTestResult(rightField, "5|e|ee", "6|f|ff", "7|g|gg"));
        FakePrimitive rightPrim = new FakePrimitive(rightResultSet);
        Map<String, BindVariable> bv = new HashMap<>();
        bv.put("a", SqlTypes.int64BindVariable(10L));
        // Normal join
        Map<String, Integer> vars = new HashMap<>();
        vars.put("bv", 1);
        JoinGen4Engine jn = new JoinGen4Engine(Engine.JoinOpcode.NormalJoin, vars);
        jn.setLeft(leftPrim);
        jn.setRight(rightPrim);
        jn.setCols(Lists.newArrayList(-1, -2, 1, 2));
        IExecute.ExecuteMultiShardResponse result = jn.execute(VtContext.background(), new NoopVCursor(), bv, true);
        leftPrim.expectLog(Lists.newArrayList("Execute a: type:INT64 value:\"10\" true"));
        rightPrim.expectLog(Lists.newArrayList("Execute a: type:INT64 value:\"10\" bv: type:VARCHAR value:\"a\" true",
            "Execute a: type:INT64 value:\"10\" bv: type:VARCHAR value:\"b\" false",
            "Execute a: type:INT64 value:\"10\" bv: type:VARCHAR value:\"c\" false"));

        Query.Field[] expectResultField = TestResult.makeTestFields("col1|col2|col4|col5", "int64|varchar|int64|varchar");
        VtResultSet expectResultSet = TestResult.makeTestResult(expectResultField, "1|a|4|d",
            "3|c|5|e",
            "3|c|6|f",
            "3|c|7|g");
        Assert.assertEquals(printFail(" testJoinExecute  Normaljoin is FAIL"), expectResultSet, result.getVtRowList());

        // Left Join
        leftPrim.rewind();
        rightPrim.rewind();
        jn.setOpcode(Engine.JoinOpcode.LeftJoin);
        result = jn.execute(VtContext.background(), new NoopVCursor(), bv, true);
        leftPrim.expectLog(Lists.newArrayList("Execute a: type:INT64 value:\"10\" true"));
        rightPrim.expectLog(Lists.newArrayList("Execute a: type:INT64 value:\"10\" bv: type:VARCHAR value:\"a\" true",
            "Execute a: type:INT64 value:\"10\" bv: type:VARCHAR value:\"b\" false",
            "Execute a: type:INT64 value:\"10\" bv: type:VARCHAR value:\"c\" false"));
        expectResultField = TestResult.makeTestFields("col1|col2|col4|col5", "int64|varchar|int64|varchar");
        expectResultSet = TestResult.makeTestResult(expectResultField, "1|a|4|d",
            "2|b|null|null",
            "3|c|5|e",
            "3|c|6|f",
            "3|c|7|g");
        Assert.assertEquals(printFail(" testJoinExecute  leftjoin is FAIL"), expectResultSet, result.getVtRowList());
    }


    @Test
    public void testJoinExecuteMaxMemoryRows() throws SQLException {
        int saveMax = FakeVcursorUtil.testMaxMemoryRows;

        boolean saveIgnore = FakeVcursorUtil.testIgnoreMaxMemoryRows;

        FakeVcursorUtil.testMaxMemoryRows = 3;
        @Getter
        @AllArgsConstructor
        class TestCase {
            boolean ignoreMaxMemoryRows;

            String err;
        }
        List<TestCase> testCases = new ArrayList<>();
        testCases.add(new TestCase(true, null));
        testCases.add(new TestCase(false, "in-memory row count exceeded allowed limit of 3"));

        for (TestCase tc : testCases) {
            Query.Field[] leftField = TestResult.makeTestFields("col1|col2|col3", "int64|varchar|varchar");
            VtResultSet leftResultSet = TestResult.makeTestResult(leftField, "1|a|aa", "2|b|bb", "3|c|cc");
            FakePrimitive leftPrim = new FakePrimitive(Lists.newArrayList(leftResultSet));
            Query.Field[] rightField = TestResult.makeTestFields("col4|col5|col6", "int64|varchar|varchar");
            List<VtResultSet> rightResultSet = new ArrayList<>();
            rightResultSet.add(TestResult.makeTestResult(rightField, "4|d|dd"));
            rightResultSet.add(TestResult.makeTestResult(rightField));
            rightResultSet.add(TestResult.makeTestResult(rightField, "5|e|ee", "6|f|ff", "7|g|gg"));
            FakePrimitive rightPrim = new FakePrimitive(rightResultSet);
            Map<String, BindVariable> bv = new HashMap<>();
            bv.put("a", SqlTypes.int64BindVariable(10L));
            // Normal join
            Map<String, Integer> vars = new HashMap<>();
            vars.put("bv", 1);
            JoinGen4Engine jn = new JoinGen4Engine(Engine.JoinOpcode.NormalJoin, vars);
            jn.setLeft(leftPrim);
            jn.setRight(rightPrim);
            jn.setCols(Lists.newArrayList(-1, -2, 1, 2));

            FakeVcursorUtil.testIgnoreMaxMemoryRows = tc.ignoreMaxMemoryRows;
            try {
                jn.execute(VtContext.background(), new NoopVCursor(), bv, true);
                if (!FakeVcursorUtil.testIgnoreMaxMemoryRows) {
                    Assert.fail(" testJoinExecuteMaxMemoryRows   is FAIL");
                }
            } catch (SQLException e) {
                if (!FakeVcursorUtil.testIgnoreMaxMemoryRows) {
                    Assert.assertEquals(printFail(" testJoinExecuteMaxMemoryRows   is FAIL"), tc.err, e.getMessage());
                } else {
                    throw e;
                }
            }
        }
        FakeVcursorUtil.testMaxMemoryRows = saveMax;
        FakeVcursorUtil.testIgnoreMaxMemoryRows = saveIgnore;

    }

    @Test
    public void testJoinExecuteNoResult() throws SQLException {
        Query.Field[] leftField = TestResult.makeTestFields("col1|col2|col3", "int64|varchar|varchar");
        VtResultSet leftResultSet = TestResult.makeTestResult(leftField);
        FakePrimitive leftPrim = new FakePrimitive(Lists.newArrayList(leftResultSet));
        Query.Field[] rightField = TestResult.makeTestFields("col4|col5|col6", "int64|varchar|varchar");
        VtResultSet rightResultSet = TestResult.makeTestResult(rightField);
        FakePrimitive rightPrim = new FakePrimitive(Lists.newArrayList(rightResultSet));
        Map<String, Integer> vars = new HashMap<>();
        vars.put("bv", 1);
        JoinGen4Engine jn = new JoinGen4Engine(Engine.JoinOpcode.NormalJoin, vars);
        jn.setLeft(leftPrim);
        jn.setRight(rightPrim);
        jn.setCols(Lists.newArrayList(-1, -2, 1, 2));
        IExecute.ExecuteMultiShardResponse result = jn.execute(VtContext.background(), new NoopVCursor(), null, true);
        leftPrim.expectLog(Lists.newArrayList("Execute  true"));
        rightPrim.expectLog(Lists.newArrayList("GetFields bv: ",
            "Execute bv:  true"));

        Query.Field[] expectResultField = TestResult.makeTestFields("col1|col2|col4|col5", "int64|varchar|int64|varchar");
        VtResultSet expectResultSet = TestResult.makeTestResult(expectResultField);
        Assert.assertEquals(printFail(" testJoinExecuteNoResult   is FAIL"), expectResultSet, result.getVtRowList());
    }

    @Test
    public void testJoinExecuteErrors() {
        // Error on left query
        FakePrimitive leftPrim = new FakePrimitive(new SQLException("left err"));
        JoinGen4Engine jn = new JoinGen4Engine(Engine.JoinOpcode.NormalJoin, null);
        jn.setLeft(leftPrim);
        try {
            jn.execute(VtContext.background(), new NoopVCursor(), null, true);
            Assert.fail(" testJoinExecuteErrors   is FAIL");
        } catch (SQLException e) {
            Assert.assertEquals(printFail(" testJoinExecuteErrors   is FAIL"), "left err", e.getMessage());
        }

        // Error on right query
        Query.Field[] leftField = TestResult.makeTestFields("col1|col2|col3", "int64|varchar|varchar");
        VtResultSet leftResultSet = TestResult.makeTestResult(leftField, "1|a|aa",
            "2|b|bb",
            "3|c|cc");
        leftPrim = new FakePrimitive(Lists.newArrayList(leftResultSet));
        FakePrimitive rightPrim = new FakePrimitive(new SQLException("right err"));
        Map<String, Integer> vars = new HashMap<>();
        vars.put("bv", 1);
        jn = new JoinGen4Engine(Engine.JoinOpcode.NormalJoin, vars);
        jn.setLeft(leftPrim);
        jn.setRight(rightPrim);
        jn.setCols(Lists.newArrayList(-1, -2, 1, 2));
        try {
            jn.execute(VtContext.background(), new NoopVCursor(), null, true);
            Assert.fail(" testJoinExecuteErrors   is FAIL");
        } catch (SQLException e) {
            Assert.assertEquals(printFail(" testJoinExecuteErrors   is FAIL"), "right err", e.getMessage());
        }

        // Error on right getfields
        leftResultSet = TestResult.makeTestResult(leftField);
        leftPrim = new FakePrimitive(Lists.newArrayList(leftResultSet));
        jn = new JoinGen4Engine(Engine.JoinOpcode.NormalJoin, vars);
        jn.setLeft(leftPrim);
        jn.setRight(rightPrim);
        try {
            jn.execute(VtContext.background(), new NoopVCursor(), null, true);
            Assert.fail(" testJoinExecuteErrors   is FAIL");
        } catch (SQLException e) {
            Assert.assertEquals(printFail(" testJoinExecuteErrors   is FAIL"), "right err", e.getMessage());
        }
    }

  /*  @Test
    public void testJoinStreamExecute() {}*/

    @Test
    public void testGetFields() throws SQLException {
        Query.Field[] leftField = TestResult.makeTestFields("col1|col2|col3", "int64|varchar|varchar");
        VtResultSet leftResultSet = TestResult.makeTestResult(leftField);
        FakePrimitive leftPrim = new FakePrimitive(Lists.newArrayList(leftResultSet));
        Query.Field[] rightField = TestResult.makeTestFields("col4|col5|col6", "int64|varchar|varchar");
        VtResultSet rightResultSet = TestResult.makeTestResult(rightField);
        FakePrimitive rightPrim = new FakePrimitive(Lists.newArrayList(rightResultSet));
        Map<String, Integer> vars = new HashMap<>();
        vars.put("bv", 1);
        JoinGen4Engine jn = new JoinGen4Engine(Engine.JoinOpcode.NormalJoin, vars);
        jn.setLeft(leftPrim);
        jn.setRight(rightPrim);
        jn.setCols(Lists.newArrayList(-1, -2, 1, 2));
        VtResultSet fields = jn.getFields(null, null);
        leftPrim.expectLog(Lists.newArrayList("GetFields ", "Execute  true"));
        rightPrim.expectLog(Lists.newArrayList("GetFields bv: ",
            "Execute bv:  true"));
        Query.Field[] expectResultField = TestResult.makeTestFields("col1|col2|col4|col5", "int64|varchar|int64|varchar");
        VtResultSet expectResultSet = TestResult.makeTestResult(expectResultField);
        Assert.assertEquals(printFail(" testGetFields   is FAIL"), expectResultSet, fields);
    }

    @Test
    public void testGetFieldsErrors() {
        FakePrimitive leftPrim = new FakePrimitive(new SQLException("left err"));
        FakePrimitive rightPrim = new FakePrimitive(new SQLException("right err"));
        Map<String, Integer> vars = new HashMap<>();
        vars.put("bv", 1);
        JoinGen4Engine jn = new JoinGen4Engine(Engine.JoinOpcode.NormalJoin, vars);
        jn.setLeft(leftPrim);
        jn.setRight(rightPrim);
        jn.setCols(Lists.newArrayList(-1, -2, 1, 2));
        try {
            jn.execute(VtContext.background(), new NoopVCursor(), null, true);
            Assert.fail(" testGetFieldsErrors   is FAIL");
        } catch (SQLException e) {
            Assert.assertEquals(printFail(" testGetFieldsErrors   is FAIL"), "left err", e.getMessage());
        }

        Query.Field[] leftField = TestResult.makeTestFields("col1|col2|col3", "int64|varchar|varchar");
        VtResultSet leftResultSet = TestResult.makeTestResult(leftField);
        leftPrim = new FakePrimitive(Lists.newArrayList(leftResultSet));
        jn.setLeft(leftPrim);
        try {
            jn.execute(VtContext.background(), new NoopVCursor(), null, true);
            Assert.fail(" testGetFieldsErrors   is FAIL");
        } catch (SQLException e) {
            Assert.assertEquals(printFail(" testGetFieldsErrors   is FAIL"), "right err", e.getMessage());
        }
    }

}
