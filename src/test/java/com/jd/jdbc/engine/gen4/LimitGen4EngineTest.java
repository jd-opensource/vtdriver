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

import com.jd.jdbc.context.VtContext;
import com.jd.jdbc.engine.FakePrimitive;
import com.jd.jdbc.engine.util.TestResult;
import com.jd.jdbc.engine.vcursor.NoopVCursor;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.evalengine.EvalResult;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.srvtopo.BindVariable;
import io.vitess.proto.Query;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class LimitGen4EngineTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testLimitExecute() throws SQLException {
        Map<String, BindVariable> bindVariableMap = new LinkedHashMap<String, BindVariable>();
        Query.Field[] fields = TestResult.makeTestFields("col1|col2", "varchar|int64");
        VtResultSet inputResult = TestResult.makeTestResult(fields, "a|1", "b|2", "c|3");

        FakePrimitive fp = new FakePrimitive(Collections.singletonList(inputResult));
        LimitGen4Engine l = new LimitGen4Engine(fp, new EvalEngine.Literal(new EvalResult(BigInteger.valueOf(2), Query.Type.UINT64)), null);

        // Test with limit smaller than input.
        VtResultSet resultSet = (VtResultSet) l.execute(VtContext.background(), null, bindVariableMap, false).getVtRowList();
        List<List<VtResultValue>> expectResult = Arrays.asList(
            Arrays.asList(new VtResultValue("a", Query.Type.VARCHAR), new VtResultValue(1, Query.Type.INT64)),
            Arrays.asList(new VtResultValue("b", Query.Type.VARCHAR), new VtResultValue(2, Query.Type.INT64))
        );
        Assert.assertEquals(expectResult.toString(), resultSet.getRows().toString());

        // Test with limit equal to input.
        VtResultSet inputResult2 = TestResult.makeTestResult(fields, "a|1", "b|2", "c|3");
        FakePrimitive fp2 = new FakePrimitive(Collections.singletonList(inputResult2));
        LimitGen4Engine l2 = new LimitGen4Engine(fp2, new EvalEngine.Literal(new EvalResult(BigInteger.valueOf(3), Query.Type.UINT64)), null);
        List<List<VtResultValue>> expectResult2 = Arrays.asList(
            Arrays.asList(new VtResultValue("a", Query.Type.VARCHAR), new VtResultValue(1, Query.Type.INT64)),
            Arrays.asList(new VtResultValue("b", Query.Type.VARCHAR), new VtResultValue(2, Query.Type.INT64)),
            Arrays.asList(new VtResultValue("c", Query.Type.VARCHAR), new VtResultValue(3, Query.Type.INT64))
        );
        VtResultSet resultSet2 = (VtResultSet) l2.execute(VtContext.background(), null, bindVariableMap, false).getVtRowList();
        Assert.assertEquals(expectResult2.toString(), resultSet2.getRows().toString());

        // Test with limit higher than input.
        VtResultSet inputResult3 = TestResult.makeTestResult(fields, "a|1", "b|2", "c|3");
        FakePrimitive fp3 = new FakePrimitive(Collections.singletonList(inputResult3));
        LimitGen4Engine l3 = new LimitGen4Engine(fp3, new EvalEngine.Literal(new EvalResult(BigInteger.valueOf(4), Query.Type.UINT64)), null);
        List<List<VtResultValue>> expectResult3 = Arrays.asList(
            Arrays.asList(new VtResultValue("a", Query.Type.VARCHAR), new VtResultValue(1, Query.Type.INT64)),
            Arrays.asList(new VtResultValue("b", Query.Type.VARCHAR), new VtResultValue(2, Query.Type.INT64)),
            Arrays.asList(new VtResultValue("c", Query.Type.VARCHAR), new VtResultValue(3, Query.Type.INT64))
        );
        VtResultSet resultSet3 = (VtResultSet) l3.execute(VtContext.background(), null, bindVariableMap, false).getVtRowList();
        Assert.assertEquals(expectResult3.toString(), resultSet3.getRows().toString());
    }

    @Test
    public void testLimitOffsetExecute() throws SQLException {
        Query.Field[] fields = TestResult.makeTestFields("col1|col2", "varchar|int64");
        Map<String, BindVariable> bindVariableMap = new LinkedHashMap<String, BindVariable>();

        // Test with offset 0
        // limit 2,0
        VtResultSet inputResult1 = TestResult.makeTestResult(fields, "a|1", "b|2", "c|3", "c|4", "c|5", "c|6");
        LimitGen4Engine l1 = new LimitGen4Engine();
        l1.setInput(new FakePrimitive(Collections.singletonList(inputResult1)));
        l1.setOffset(new EvalEngine.Literal(new EvalResult(BigInteger.valueOf(2), Query.Type.UINT64)));
        l1.setCount(new EvalEngine.Literal(new EvalResult(BigInteger.valueOf(0), Query.Type.UINT64)));

        VtResultSet resultSet1 = (VtResultSet) l1.execute(VtContext.background(), null, bindVariableMap, false).getVtRowList();

        List<List<VtResultValue>> expectResult1 = Arrays.asList();
        Assert.assertEquals(expectResult1.toString(), resultSet1.getRows().toString());

        // Test with offset set
        // limit 1,2
        VtResultSet inputResult2 = TestResult.makeTestResult(fields, "a|1", "b|2", "c|3", "c|4", "c|5", "c|6");
        LimitGen4Engine l2 = new LimitGen4Engine();
        l2.setInput(new FakePrimitive(Collections.singletonList(inputResult2)));
        l2.setOffset(new EvalEngine.Literal(new EvalResult(BigInteger.valueOf(1), Query.Type.UINT64)));
        l2.setCount(new EvalEngine.Literal(new EvalResult(BigInteger.valueOf(2), Query.Type.UINT64)));

        VtResultSet resultSet2 = (VtResultSet) l2.execute(VtContext.background(), null, bindVariableMap, false).getVtRowList();

        List<List<VtResultValue>> expectResult2 = Arrays.asList(
            Arrays.asList(new VtResultValue("b", Query.Type.VARCHAR), new VtResultValue(2, Query.Type.INT64)),
            Arrays.asList(new VtResultValue("c", Query.Type.VARCHAR), new VtResultValue(3, Query.Type.INT64))
        );
        Assert.assertEquals(expectResult2.toString(), resultSet2.getRows().toString());

        // Works on boundary condition (elements == limit + offset)
        VtResultSet inputResult3 = TestResult.makeTestResult(fields, "a|1", "b|2", "c|3", "c|4", "c|5", "c|6");
        LimitGen4Engine l3 = new LimitGen4Engine();
        l3.setInput(new FakePrimitive(Collections.singletonList(inputResult3)));
        l3.setOffset(new EvalEngine.Literal(new EvalResult(BigInteger.valueOf(1), Query.Type.UINT64)));
        l3.setCount(new EvalEngine.Literal(new EvalResult(BigInteger.valueOf(2), Query.Type.UINT64)));

        VtResultSet resultSet3 = (VtResultSet) l3.execute(VtContext.background(), null, bindVariableMap, false).getVtRowList();

        List<List<VtResultValue>> expectResult3 = Arrays.asList(
            Arrays.asList(new VtResultValue("b", Query.Type.VARCHAR), new VtResultValue(2, Query.Type.INT64)),
            Arrays.asList(new VtResultValue("c", Query.Type.VARCHAR), new VtResultValue(3, Query.Type.INT64))
        );
        Assert.assertEquals(expectResult3.toString(), resultSet3.getRows().toString());

        // Works on boundary condition (elements == limit + offset)
        // limit 2,4
        VtResultSet inputResult4 = TestResult.makeTestResult(fields, "a|1", "b|2", "c|3", "c|4", "c|5", "c|6");
        LimitGen4Engine l4 = new LimitGen4Engine();
        l4.setInput(new FakePrimitive(Collections.singletonList(inputResult4)));
        l4.setOffset(new EvalEngine.Literal(new EvalResult(BigInteger.valueOf(2), Query.Type.UINT64)));
        l4.setCount(new EvalEngine.Literal(new EvalResult(BigInteger.valueOf(4), Query.Type.UINT64)));

        VtResultSet resultSet4 = (VtResultSet) l4.execute(VtContext.background(), null, bindVariableMap, false).getVtRowList();

        List<List<VtResultValue>> expectResult4 = Arrays.asList(
            Arrays.asList(new VtResultValue("c", Query.Type.VARCHAR), new VtResultValue(3, Query.Type.INT64)),
            Arrays.asList(new VtResultValue("c", Query.Type.VARCHAR), new VtResultValue(4, Query.Type.INT64)),
            Arrays.asList(new VtResultValue("c", Query.Type.VARCHAR), new VtResultValue(5, Query.Type.INT64)),
            Arrays.asList(new VtResultValue("c", Query.Type.VARCHAR), new VtResultValue(6, Query.Type.INT64))
        );
        Assert.assertEquals(expectResult4.toString(), resultSet4.getRows().toString());

        // test when limit is beyond the number of available elements
        // limit 5,2
        VtResultSet inputResult5 = TestResult.makeTestResult(fields, "a|1", "b|2", "c|3", "c|4", "c|5", "c|6");
        LimitGen4Engine l5 = new LimitGen4Engine();
        l5.setInput(new FakePrimitive(Collections.singletonList(inputResult5)));
        l5.setOffset(new EvalEngine.Literal(new EvalResult(BigInteger.valueOf(5), Query.Type.UINT64)));
        l5.setCount(new EvalEngine.Literal(new EvalResult(BigInteger.valueOf(2), Query.Type.UINT64)));

        VtResultSet resultSet5 = (VtResultSet) l5.execute(VtContext.background(), null, bindVariableMap, false).getVtRowList();

        List<List<VtResultValue>> expectResult5 = Arrays.asList(
            Arrays.asList(new VtResultValue("c", Query.Type.VARCHAR), new VtResultValue(6, Query.Type.INT64))
        );
        Assert.assertEquals(expectResult5.toString(), resultSet5.getRows().toString());

        // Works when offset is beyond the response
        // limit 7,2
        VtResultSet inputResult6 = TestResult.makeTestResult(fields, "a|1", "b|2", "c|3", "c|4", "c|5", "c|6");
        LimitGen4Engine l6 = new LimitGen4Engine();
        l6.setInput(new FakePrimitive(Collections.singletonList(inputResult6)));
        l6.setOffset(new EvalEngine.Literal(new EvalResult(BigInteger.valueOf(7), Query.Type.UINT64)));
        l6.setCount(new EvalEngine.Literal(new EvalResult(BigInteger.valueOf(2), Query.Type.UINT64)));

        VtResultSet resultSet6 = (VtResultSet) l6.execute(VtContext.background(), null, bindVariableMap, false).getVtRowList();

        Assert.assertFalse(resultSet6.hasNext());
    }

    @Test
    public void testLimitStreamExecute(){
        // todo
    }

    @Test
    public void testOffsetStreamExecute(){
        // todo
    }

    @Test
    public void testLimitGetFields() throws SQLException {
        Query.Field[] fields = TestResult.makeTestFields("col1|col2", "varchar|int64");
        VtResultSet inputResult = TestResult.makeTestResult(fields);
        LimitGen4Engine l1 = new LimitGen4Engine();
        l1.setInput(new FakePrimitive(Collections.singletonList(inputResult)));

        VtResultSet resultSet = l1.getFields(null, new HashMap<>());
        Assert.assertEquals(inputResult, resultSet);
    }

    @Test
    public void testLimitInputFail() throws SQLException {
        FakePrimitive fp = new FakePrimitive(new SQLException("input fail"));
        LimitGen4Engine l = new LimitGen4Engine(fp, new EvalEngine.Literal(new EvalResult(BigInteger.valueOf(2), Query.Type.UINT64)), null);

        String want = "input fail";
        thrown.expect(SQLException.class);
        thrown.expectMessage(want);

        l.execute(VtContext.background(), null, new LinkedHashMap<String, BindVariable>(), false);

//        fp.rewind();
//        l.streamExecute(VtContext.background(), null, new LinkedHashMap<String, BindVariable>() {{
//            put("0", new BindVariable("2".getBytes(), Query.Type.INT32));
//        }}, false);

        fp.rewind();
        l.getFields(null, null);
    }

    @Test
    public void testLimitInvalidCount() throws SQLException {
        Query.Field[] fields = TestResult.makeTestFields("col1|col2", "varchar|int64");
        FakePrimitive fp = new FakePrimitive(Collections.singletonList(TestResult.makeTestResult(fields)));

        String want = "could not parse value: '1.2'";
        thrown.expect(SQLException.class);
        thrown.expectMessage(want);

        LimitGen4Engine l = new LimitGen4Engine(fp, new EvalEngine.Literal(new EvalResult(1.2, Query.Type.FLOAT64)), null);
        l.execute(VtContext.background(), null, new LinkedHashMap<String, BindVariable>(), false);
    }

    @Test
    public void testLimitOutOfRange() throws SQLException {
        Query.Field[] fields = TestResult.makeTestFields("col1|col2", "varchar|int64");
        FakePrimitive fp = new FakePrimitive(Collections.singletonList(TestResult.makeTestResult(fields)));

        String want = "requested limit is out of range: 18446744073709551615";
        thrown.expect(SQLException.class);
        thrown.expectMessage(want);

        LimitGen4Engine l = new LimitGen4Engine(fp, new EvalEngine.Literal(new EvalResult(new BigInteger("18446744073709551615"), Query.Type.UINT64)), null);
        l.getCount(new NoopVCursor(), null);
    }
}
