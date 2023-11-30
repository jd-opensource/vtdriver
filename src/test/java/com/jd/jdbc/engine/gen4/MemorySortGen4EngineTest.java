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
import com.jd.jdbc.engine.FakePrimitive;
import com.jd.jdbc.engine.util.TestResult;
import com.jd.jdbc.engine.vcursor.NoopVCursor;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefExpr;
import com.jd.jdbc.sqltypes.SqlTypes;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.srvtopo.BindVariable;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class MemorySortGen4EngineTest extends BaseTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testMemorySortExecute() throws SQLException {
        Query.Field[] fields = TestResult.makeTestFields("c1|c2", "varbinary|decimal");
        VtResultSet resultSet = TestResult.makeTestResult(fields, "a|1", "g|2", "a|1", "c|4", "c|3");
        FakePrimitive fp = new FakePrimitive(Lists.newArrayList(resultSet));

        OrderByParamsGen4 orderByParamsGen4 = new OrderByParamsGen4(1, false, -1, null);
        MemorySortGen4Engine ms = new MemorySortGen4Engine(Lists.newArrayList(orderByParamsGen4), fp);

        VtResultSet result = (VtResultSet) ms.execute(VtContext.background(), new NoopVCursor(), null, false).getVtRowList();

        VtResultSet wantResult = TestResult.makeTestResult(fields, "a|1", "a|1", "g|2", "c|3", "c|4");
        Assert.assertEquals(wantResult, result);

        fp.rewind();
        EvalEngine.BindVariable pv = new EvalEngine.BindVariable("__upper_limit");
        ms.setUpperLimit(pv);

        Map<String, BindVariable> bindVariableMap = new HashMap<>();
        bindVariableMap.put("__upper_limit", SqlTypes.int64BindVariable(3L));
        result = (VtResultSet) ms.execute(VtContext.background(), new NoopVCursor(), bindVariableMap, false).getVtRowList();
        wantResult = TestResult.makeTestResult(fields, "a|1", "a|1", "g|2");
        Assert.assertEquals(wantResult, result);
    }

    public void testMemorySortStreamExecuteWeightString() throws SQLException {

    }

    @Test
    public void testMemorySortExecuteWeightString() throws SQLException {
        Query.Field[] fields = TestResult.makeTestFields("c1|c2", "varchar|varbinary");
        VtResultSet resultSet = TestResult.makeTestResult(fields, "a|1", "g|2", "a|1", "c|4", "c|3");
        FakePrimitive fp = new FakePrimitive(Lists.newArrayList(resultSet));

        OrderByParamsGen4 orderByParamsGen4 = new OrderByParamsGen4(0, false, 1, null);
        MemorySortGen4Engine ms = new MemorySortGen4Engine(Lists.newArrayList(orderByParamsGen4), fp);

        VtResultSet result = (VtResultSet) ms.execute(VtContext.background(), new NoopVCursor(), null, false).getVtRowList();
        VtResultSet wantResult = TestResult.makeTestResult(fields, "a|1", "a|1", "g|2", "c|3", "c|4");
        Assert.assertEquals(wantResult, result);

        fp.rewind();
        SQLVariantRefExpr upperLimitVar = new SQLVariantRefExpr();
        EvalEngine.BindVariable pv = new EvalEngine.BindVariable("__upper_limit");
        ms.setUpperLimit(pv);

        Map<String, BindVariable> bindVariableMap = new HashMap<>();
        bindVariableMap.put("__upper_limit", SqlTypes.int64BindVariable(3L));
        result = (VtResultSet) ms.execute(VtContext.background(), new NoopVCursor(), bindVariableMap, false).getVtRowList();
        wantResult = TestResult.makeTestResult(fields, "a|1", "a|1", "g|2");
        Assert.assertEquals(wantResult, result);
    }

    public void testMemorySortStreamExecuteCollation() throws SQLException {

    }

    public void testMemorySortExecuteCollation() throws SQLException {

    }

    public void testMemorySortStreamExecute() throws SQLException {

    }

    @Test
    public void testMemorySortGetFields() throws SQLException {
        Query.Field[] fields = TestResult.makeTestFields("col1|col2", "int64|varchar");
        VtResultSet resultSet = TestResult.makeTestResult(fields);
        FakePrimitive fp = new FakePrimitive(Lists.newArrayList(resultSet));

        MemorySortGen4Engine ms = new MemorySortGen4Engine(null, fp);
        VtResultSet got = ms.getFields(null, null);
        Assert.assertEquals(resultSet, got);
    }

    @Test
    public void testMemorySortExecuteTruncate() throws SQLException {
        Query.Field[] fields = TestResult.makeTestFields("c1|c2|c3", "varbinary|decimal|int64");
        VtResultSet resultSet = TestResult.makeTestResult(fields, "a|1|1", "g|2|1", "a|1|1", "c|4|1", "c|3|1");
        FakePrimitive fp = new FakePrimitive(Lists.newArrayList(resultSet));

        OrderByParamsGen4 orderByParamsGen4 = new OrderByParamsGen4(1, false, -1, null);
        MemorySortGen4Engine ms = new MemorySortGen4Engine(Lists.newArrayList(orderByParamsGen4), fp);
        ms.setTruncateColumnCount(2);

        VtResultSet result = (VtResultSet) ms.execute(VtContext.background(), new NoopVCursor(), null, false).getVtRowList();
        VtResultSet wantResult = TestResult.makeTestResult(new Query.Field[] {fields[0], fields[1]}, "a|1", "a|1", "g|2", "c|3", "c|4");
        Assert.assertEquals(wantResult, result);
    }

    public void testMemorySortStreamExecuteTruncate() throws SQLException {

    }

    @Test
    public void testMemorySortMultiColumn() throws SQLException {
        Query.Field[] fields = TestResult.makeTestFields("c1|c2", "varbinary|decimal");
        VtResultSet resultSet = TestResult.makeTestResult(fields, "a|1", "b|2", "b|1", "c|4", "c|3");
        FakePrimitive fp = new FakePrimitive(Lists.newArrayList(resultSet));

        OrderByParamsGen4 orderByParams1 = new OrderByParamsGen4(1, false, -1, null);
        OrderByParamsGen4 orderByParams2 = new OrderByParamsGen4(0, true, -1, null);
        MemorySortGen4Engine ms = new MemorySortGen4Engine(Lists.newArrayList(orderByParams1, orderByParams2), fp);

        VtResultSet result = (VtResultSet) ms.execute(VtContext.background(), new NoopVCursor(), null, false).getVtRowList();
        VtResultSet wantResult = TestResult.makeTestResult(fields, "b|1", "a|1", "b|2", "c|3", "c|4");
        Assert.assertEquals(wantResult, result);

        fp.rewind();
        EvalEngine.BindVariable pv = new EvalEngine.BindVariable("__upper_limit");
        ms.setUpperLimit(pv);
        Map<String, BindVariable> bindVariableMap = new HashMap<>();
        bindVariableMap.put("__upper_limit", SqlTypes.int64BindVariable(3L));
        result = (VtResultSet) ms.execute(VtContext.background(), new NoopVCursor(), bindVariableMap, false).getVtRowList();
        wantResult = TestResult.makeTestResult(fields, "b|1", "a|1", "b|2");
        Assert.assertEquals(wantResult, result);
    }

    public void testMemorySortMaxMemoryRows() throws SQLException {

    }

    @Test
    @Ignore
    public void TestMemorySortExecuteNoVarChar() throws SQLException {
        Query.Field[] fields = TestResult.makeTestFields("c1|c2", "varchar|decimal");
        VtResultSet resultSet = TestResult.makeTestResult(fields, "a|1", "b|2", "a|1", "c|4", "c|3");
        FakePrimitive fp = new FakePrimitive(Lists.newArrayList(resultSet));

        OrderByParamsGen4 orderByParams = new OrderByParamsGen4(1, false, -1, null);
        MemorySortGen4Engine ms = new MemorySortGen4Engine(Lists.newArrayList(orderByParams), fp);

        VtResultSet result = (VtResultSet) ms.execute(VtContext.background(), new NoopVCursor(), null, false).getVtRowList();
        String want = "cannot compare strings, collation is unknown or unsupported (collation ID: 0)";
        thrown.expect(SQLException.class);
        thrown.expectMessage(want);
    }
}