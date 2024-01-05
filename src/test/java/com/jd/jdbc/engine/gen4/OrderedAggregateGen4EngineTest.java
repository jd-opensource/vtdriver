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
import com.jd.jdbc.common.tuple.Pair;
import com.jd.jdbc.context.VtContext;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.FakePrimitive;
import com.jd.jdbc.engine.util.TestResult;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import io.vitess.proto.Query;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class OrderedAggregateGen4EngineTest extends BaseTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testOrderedAggregateExecute() throws SQLException {
        Query.Field[] fields = TestResult.makeTestFields("col|count(*)", "varbinary|decimal");
        VtResultSet resultSet = TestResult.makeTestResult(fields, "a|1", "a|1", "b|2", "c|3", "c|4");
        FakePrimitive fp = new FakePrimitive(Lists.newArrayList(resultSet));

        AbstractAggregateGen4.AggregateParams aggr = new AbstractAggregateGen4.AggregateParams(Engine.AggregateOpcodeG4.AggregateSum, 1);
        GroupByParams groupByParams = new GroupByParams();
        groupByParams.setKeyCol(0);
        OrderedAggregateGen4Engine oa = new OrderedAggregateGen4Engine(false, Lists.newArrayList(aggr), false, 0, Lists.newArrayList(groupByParams), null, fp);

        VtResultSet result = (VtResultSet) oa.execute(VtContext.background(), null, null, false).getVtRowList();

        VtResultSet wantResult = TestResult.makeTestResult(fields, "a|2", "b|2", "c|7");
        Assert.assertEquals(wantResult, result);
    }

    @Test
    public void testOrderedAggregateExecuteTruncate() throws SQLException {
        Query.Field[] fields = TestResult.makeTestFields("col|count(*)|weight_string(col)", "varchar|decimal|varbinary");
        VtResultSet resultSet = TestResult.makeTestResult(fields, "a|1|A", "A|1|A", "b|2|B", "C|3|C", "c|4|C");
        FakePrimitive fp = new FakePrimitive(Lists.newArrayList(resultSet));

        AbstractAggregateGen4.AggregateParams aggr = new AbstractAggregateGen4.AggregateParams(Engine.AggregateOpcodeG4.AggregateSum, 1);
        GroupByParams groupByParams = new GroupByParams();
        groupByParams.setKeyCol(2);
        OrderedAggregateGen4Engine oa = new OrderedAggregateGen4Engine(false, Lists.newArrayList(aggr), false, 2, Lists.newArrayList(groupByParams), null, fp);

        VtResultSet result = (VtResultSet) oa.execute(VtContext.background(), null, null, false).getVtRowList();

        Query.Field[] wantFields = TestResult.makeTestFields("col|count(*)", "varchar|decimal");
        VtResultSet wantResult = TestResult.makeTestResult(wantFields, "a|2", "b|2", "C|7");
        Assert.assertEquals(wantResult, result);
    }

    public void testOrderedAggregateStreamExecute() {
        // todo
    }

    public void testOrderedAggregateStreamExecuteTruncate() {
        // todo
    }

    @Test
    public void testOrderedAggregateGetFields() throws SQLException {
        VtResultSet input = TestResult.makeTestResult(TestResult.makeTestFields("col|count(*)", "varbinary|decimal"));
        FakePrimitive fp = new FakePrimitive(Lists.newArrayList(input));

        OrderedAggregateGen4Engine oa = new OrderedAggregateGen4Engine(false, null, false, 0, null, null, fp);
        VtResultSet got = oa.getFields(null, null);

        Assert.assertEquals(got, input);
    }

    @Test
    public void testOrderedAggregateGetFieldsTruncate() throws SQLException {
        VtResultSet input = TestResult.makeTestResult(TestResult.makeTestFields("col|count(*)|weight_string(col)", "varchar|decimal|varbinary"));
        FakePrimitive fp = new FakePrimitive(Lists.newArrayList(input));

        OrderedAggregateGen4Engine oa = new OrderedAggregateGen4Engine(false, null, false, 2, null, null, fp);
        VtResultSet got = oa.getFields(null, null);
        VtResultSet wantResult = TestResult.makeTestResult(TestResult.makeTestFields("col|count(*)", "varchar|decimal"));
        Assert.assertEquals(got, wantResult);
    }

    @Test
    public void testOrderedAggregateInputFail() throws SQLException {
        FakePrimitive fp = new FakePrimitive(new SQLException("input fail"));
        OrderedAggregateGen4Engine oa = new OrderedAggregateGen4Engine(false, null, false, 0, null, null, fp);

        String want = "input fail";
        thrown.expect(SQLException.class);
        thrown.expectMessage(want);
        oa.execute(VtContext.background(), null, null, false);

        fp.rewind();
        thrown.expect(SQLException.class);
        thrown.expectMessage(want);
        oa.getFields(null, null);
    }

    @Test
    public void testOrderedAggregateExecuteCountDistinct() throws SQLException {
        VtResultSet input = TestResult.makeTestResult(TestResult.makeTestFields("col1|col2|count(*)", "varbinary|decimal|int64"),
            // Two identical values
            "a|1|1",
            "a|1|2",
            // Single value
            "b|1|1",
            // Two different values
            "c|3|1",
            "c|4|1",
            // Single null
            "d|null|1",
            // Start with null
            "e|null|1",
            "e|1|1",
            // Null comes after first
            "f|1|1",
            "f|null|1",
            // Identical to non-identical transition
            "g|1|1",
            "g|1|1",
            "g|2|1",
            "g|3|1",
            // Non-identical to identical transition
            "h|1|1",
            "h|2|1",
            "h|2|1",
            "h|3|1",
            // Key transition, should still count 3
            "i|3|1",
            "i|4|1");
        FakePrimitive fp = new FakePrimitive(Lists.newArrayList(input));

        AbstractAggregateGen4.AggregateParams aggr1 = new AbstractAggregateGen4.AggregateParams(Engine.AggregateOpcodeG4.AggregateCountDistinct, 1, "count(distinct col2)");
        AbstractAggregateGen4.AggregateParams aggr2 = new AbstractAggregateGen4.AggregateParams(Engine.AggregateOpcodeG4.AggregateSum, 2);
        GroupByParams groupByParams = new GroupByParams();
        groupByParams.setKeyCol(0);
        OrderedAggregateGen4Engine oa = new OrderedAggregateGen4Engine(true, Lists.newArrayList(aggr1, aggr2), false, 0, Lists.newArrayList(groupByParams), null, fp);
        VtResultSet result = (VtResultSet) oa.execute(VtContext.background(), null, null, false).getVtRowList();

        VtResultSet wantResult = TestResult.makeTestResult(TestResult.makeTestFields("col1|count(distinct col2)|count(*)", "varbinary|int64|int64"), "a|1|3",
            "b|1|1",
            "c|2|2",
            "d|0|1",
            "e|1|2",
            "f|1|2",
            "g|3|4",
            "h|3|4",
            "i|2|2");

        Assert.assertEquals(wantResult, result);
    }

    public void testOrderedAggregateStreamCountDistinct() {
        // todo
    }

    @Test
    public void testOrderedAggregateSumDistinctGood() throws SQLException {
        VtResultSet input = TestResult.makeTestResult(TestResult.makeTestFields("col1|col2|sum(col3)", "varbinary|int64|decimal"),
            // Two identical values
            "a|1|1",
            "a|1|2",
            // Single value
            "b|1|1",
            // Two different values
            "c|3|1",
            "c|4|1",
            // Single null
            "d|null|1",
            "d|1|1",
            // Start with null
            "e|null|1",
            "e|1|1",
            // Null comes after first
            "f|1|1",
            "f|null|1",
            // Identical to non-identical transition
            "g|1|1",
            "g|1|1",
            "g|2|1",
            "g|3|1",
            // Non-identical to identical transition
            "h|1|1",
            "h|2|1",
            "h|2|1",
            "h|3|1",
            // Key transition, should still count 3
            "i|3|1",
            "i|4|1");
        FakePrimitive fp = new FakePrimitive(Lists.newArrayList(input));

        AbstractAggregateGen4.AggregateParams aggr1 = new AbstractAggregateGen4.AggregateParams(Engine.AggregateOpcodeG4.AggregateSumDistinct, 1, "sum(distinct col2)");
        AbstractAggregateGen4.AggregateParams aggr2 = new AbstractAggregateGen4.AggregateParams(Engine.AggregateOpcodeG4.AggregateSum, 2);
        GroupByParams groupByParams = new GroupByParams();
        groupByParams.setKeyCol(0);
        OrderedAggregateGen4Engine oa = new OrderedAggregateGen4Engine(true, Lists.newArrayList(aggr1, aggr2), false, 0, Lists.newArrayList(groupByParams), null, fp);
        VtResultSet result = (VtResultSet) oa.execute(VtContext.background(), null, null, false).getVtRowList();

        VtResultSet wantResult = TestResult.makeTestResult(TestResult.makeTestFields("col1|sum(distinct col2)|sum(col3)", "varbinary|decimal|decimal"), "a|1|3",
            "b|1|1",
            "c|7|2",
            "d|1|2",
            "e|1|2",
            "f|1|2",
            "g|6|4",
            "h|6|4",
            "i|7|2");
        Assert.assertEquals(wantResult, result);
    }

    @Test
    public void testOrderedAggregateSumDistinctTolerateError() throws SQLException {
        VtResultSet input = TestResult.makeTestResult(TestResult.makeTestFields("col1|col2", "varbinary|varbinary"),
            "a|aaa",
            "a|0",
            "a|1");
        FakePrimitive fp = new FakePrimitive(Lists.newArrayList(input));

        AbstractAggregateGen4.AggregateParams aggr1 = new AbstractAggregateGen4.AggregateParams(Engine.AggregateOpcodeG4.AggregateSumDistinct, 1, "sum(distinct col2)");
        GroupByParams groupByParams = new GroupByParams();
        groupByParams.setKeyCol(0);
        OrderedAggregateGen4Engine oa = new OrderedAggregateGen4Engine(true, Lists.newArrayList(aggr1), false, 0, Lists.newArrayList(groupByParams), null, fp);
        VtResultSet result = (VtResultSet) oa.execute(VtContext.background(), null, null, false).getVtRowList();

        VtResultSet wantResult = TestResult.makeTestResult(TestResult.makeTestFields("col1|sum(distinct col2)", "varbinary|decimal"), "a|1");
        Assert.assertEquals(wantResult, result);
    }

    @Test
    @Ignore
    public void testOrderedAggregateKeysFail() throws SQLException {
        VtResultSet input = TestResult.makeTestResult(TestResult.makeTestFields("col|count(*)", "varchar|decimal"), "a|1", "a|1");
        FakePrimitive fp = new FakePrimitive(Lists.newArrayList(input));

        AbstractAggregateGen4.AggregateParams aggr1 = new AbstractAggregateGen4.AggregateParams(Engine.AggregateOpcodeG4.AggregateSum, 1);
        GroupByParams groupByParams = new GroupByParams();
        groupByParams.setKeyCol(0);
        OrderedAggregateGen4Engine oa = new OrderedAggregateGen4Engine(false, Lists.newArrayList(aggr1), false, 0, Lists.newArrayList(groupByParams), null, fp);

        String want = "cannot compare strings, collation is unknown or unsupported (collation ID: 0)";
        thrown.expect(SQLException.class);
        thrown.expectMessage(want);

        oa.execute(VtContext.background(), null, null, false);

        fp.rewind();
    }

    @Test
    public void testOrderedAggregateMergeFail() throws SQLException {
        VtResultSet input = TestResult.makeTestResult(TestResult.makeTestFields("col|count(*)", "varbinary|decimal"), "a|1", "a|0");
        FakePrimitive fp = new FakePrimitive(Lists.newArrayList(input));

        AbstractAggregateGen4.AggregateParams aggr1 = new AbstractAggregateGen4.AggregateParams(Engine.AggregateOpcodeG4.AggregateSum, 1);
        GroupByParams groupByParams = new GroupByParams();
        groupByParams.setKeyCol(0);
        OrderedAggregateGen4Engine oa = new OrderedAggregateGen4Engine(false, Lists.newArrayList(aggr1), false, 0, Lists.newArrayList(groupByParams), null, fp);

        Query.Field field1 = Query.Field.newBuilder().setName("col").setType(Query.Type.VARBINARY).build();
        Query.Field field2 = Query.Field.newBuilder().setName("count(*)").setType(Query.Type.DECIMAL).build();

        VtResultValue value1 = new VtResultValue("a", Query.Type.VARBINARY);
        VtResultValue value2 = new VtResultValue(BigDecimal.valueOf(1), Query.Type.DECIMAL);
        List<VtResultValue> vtResultValues = Lists.newArrayList(value1, value2);
        List<List<VtResultValue>> rows = new ArrayList<>();
        rows.add(vtResultValues);
        VtResultSet result = new VtResultSet(new Query.Field[] {field1, field2}, rows);

        VtResultSet res = (VtResultSet) oa.execute(VtContext.background(), null, null, false).getVtRowList();

        Assert.assertEquals(result, res);

        fp.rewind();
    }

    @Test
    public void testMerge() throws SQLException {
        AbstractAggregateGen4.AggregateParams aggr1 = new AbstractAggregateGen4.AggregateParams(Engine.AggregateOpcodeG4.AggregateSum, 1);
        AbstractAggregateGen4.AggregateParams aggr2 = new AbstractAggregateGen4.AggregateParams(Engine.AggregateOpcodeG4.AggregateSum, 2);
        AbstractAggregateGen4.AggregateParams aggr3 = new AbstractAggregateGen4.AggregateParams(Engine.AggregateOpcodeG4.AggregateMin, 3);
        AbstractAggregateGen4.AggregateParams aggr4 = new AbstractAggregateGen4.AggregateParams(Engine.AggregateOpcodeG4.AggregateMax, 4);
        OrderedAggregateGen4Engine oa = new OrderedAggregateGen4Engine(false, Lists.newArrayList(aggr1, aggr2, aggr3, aggr4), false, 0, null, null, null);

        Query.Field[] fields = TestResult.makeTestFields("a|b|c|d|e", "int64|int64|decimal|int32|varbinary");
        VtResultSet r = TestResult.makeTestResult(fields, "1|2|3.2|3|ab", "1|3|2.8|2|bc");

        Pair<List<VtResultValue>, List<VtResultValue>> pair = oa.merge(fields, r.getRows().get(0), r.getRows().get(1), null, null, oa.getAggregates());
        List<VtResultValue> merged = pair.getLeft();
        List<VtResultValue> want = TestResult.makeTestResult(fields, "1|5|6.0|2|bc").getRows().get(0);
        Assert.assertEquals(want, merged);

        // swap and retry
        pair = oa.merge(fields, r.getRows().get(1), r.getRows().get(0), null, null, oa.getAggregates());
        merged = pair.getLeft();
        Assert.assertEquals(want, merged);
    }

    public void testOrderedAggregateExecuteGtid() {
        // todo
    }

    @Test
    public void testCountDistinctOnVarchar() throws SQLException {
        Query.Field[] fields = TestResult.makeTestFields("c1|c2|weight_string(c2)", "int64|varchar|varbinary");
        VtResultSet r = TestResult.makeTestResult(fields, "10|a|0x41", "10|a|0x41", "10|b|0x42", "20|b|0x42");
        FakePrimitive fp = new FakePrimitive(Lists.newArrayList(r));

        AbstractAggregateGen4.AggregateParams aggr1 = new AbstractAggregateGen4.AggregateParams(Engine.AggregateOpcodeG4.AggregateCountDistinct, 1, "count(distinct c2)");
        aggr1.setWCol(2);
        aggr1.setWAssigned(true);
        GroupByParams groupByParams = new GroupByParams();
        groupByParams.setKeyCol(0);
        OrderedAggregateGen4Engine oa = new OrderedAggregateGen4Engine(true, Lists.newArrayList(aggr1), false, 2, Lists.newArrayList(groupByParams), null, fp);

        VtResultSet want = TestResult.makeTestResult(TestResult.makeTestFields("c1|count(distinct c2)", "int64|int64"), "10|2", "20|1");

        VtResultSet qr = (VtResultSet) oa.execute(VtContext.background(), null, null, false).getVtRowList();
        Assert.assertEquals(want, qr);
    }

    @Test
    public void testCountDistinctOnVarcharWithNulls() throws SQLException {
        Query.Field[] fields = TestResult.makeTestFields("c1|c2|weight_string(c2)", "int64|varchar|varbinary");
        VtResultSet r = TestResult.makeTestResult(fields, "null|null|null",
            "null|a|0x41",
            "null|b|0x42",
            "10|null|null",
            "10|null|null",
            "10|a|0x41",
            "10|a|0x41",
            "10|b|0x42",
            "20|null|null",
            "20|b|0x42",
            "30|null|null",
            "30|null|null",
            "30|null|null",
            "30|null|null");
        FakePrimitive fp = new FakePrimitive(Lists.newArrayList(r));

        AbstractAggregateGen4.AggregateParams aggr1 = new AbstractAggregateGen4.AggregateParams(Engine.AggregateOpcodeG4.AggregateCountDistinct, 1, "count(distinct c2)");
        aggr1.setWCol(2);
        aggr1.setWAssigned(true);
        GroupByParams groupByParams = new GroupByParams();
        groupByParams.setKeyCol(0);
        OrderedAggregateGen4Engine oa = new OrderedAggregateGen4Engine(true, Lists.newArrayList(aggr1), false, 2, Lists.newArrayList(groupByParams), null, fp);

        VtResultSet want = TestResult.makeTestResult(TestResult.makeTestFields("c1|count(distinct c2)", "int64|int64"), "null|2", "10|2", "20|1", "30|0");

        VtResultSet qr = (VtResultSet) oa.execute(VtContext.background(), null, null, false).getVtRowList();
        Assert.assertEquals(want, qr);
    }

    /**
     * 涉及到对字符串与null调用com.jd.jdbc.evalengine.EvalEngine#nullSafeAdd方法，暂不支持
     *
     * @throws SQLException
     */
    @Test
    @Ignore
    public void testSumDistinctOnVarcharWithNulls() throws SQLException {
        Query.Field[] fields = TestResult.makeTestFields("c1|c2|weight_string(c2)", "int64|varchar|varbinary");
        VtResultSet r = TestResult.makeTestResult(fields, "null|null|null",
            "null|a|0x41",
            "null|b|0x42",
            "10|null|null",
            "10|null|null",
            "10|a|0x41",
            "10|a|0x41",
            "10|b|0x42",
            "20|null|null",
            "20|b|0x42",
            "30|null|null",
            "30|null|null",
            "30|null|null",
            "30|null|null");
        FakePrimitive fp = new FakePrimitive(Lists.newArrayList(r));

        AbstractAggregateGen4.AggregateParams aggr1 = new AbstractAggregateGen4.AggregateParams(Engine.AggregateOpcodeG4.AggregateSumDistinct, 1, "sum(distinct c2)");
        aggr1.setWCol(2);
        aggr1.setWAssigned(true);
        GroupByParams groupByParams = new GroupByParams();
        groupByParams.setKeyCol(0);
        OrderedAggregateGen4Engine oa = new OrderedAggregateGen4Engine(true, Lists.newArrayList(aggr1), false, 2, Lists.newArrayList(groupByParams), null, fp);

        VtResultSet want = TestResult.makeTestResult(TestResult.makeTestFields("c1|sum(distinct c2)", "int64|decimal"), "null|0", "10|0", "20|0", "30|null");

        VtResultSet qr = (VtResultSet) oa.execute(VtContext.background(), null, null, false).getVtRowList();
        Assert.assertEquals(want, qr);
    }

    @Test
    public void testMultiDistinct() throws SQLException {
        Query.Field[] fields = TestResult.makeTestFields("c1|c2|c3", "int64|int64|int64");
        VtResultSet r = TestResult.makeTestResult(fields, "null|null|null",
            "null|1|2",
            "null|2|2",
            "10|null|null",
            "10|2|null",
            "10|2|1",
            "10|2|3",
            "10|3|3",
            "20|null|null",
            "20|null|null",
            "30|1|1",
            "30|1|2",
            "30|1|3",
            "40|1|1",
            "40|2|1",
            "40|3|1");
        FakePrimitive fp = new FakePrimitive(Lists.newArrayList(r));

        AbstractAggregateGen4.AggregateParams aggr1 = new AbstractAggregateGen4.AggregateParams(Engine.AggregateOpcodeG4.AggregateCountDistinct, 1, "count(distinct c2)");
        AbstractAggregateGen4.AggregateParams aggr2 = new AbstractAggregateGen4.AggregateParams(Engine.AggregateOpcodeG4.AggregateSumDistinct, 2, "sum(distinct c3)");
        GroupByParams groupByParams = new GroupByParams();
        groupByParams.setKeyCol(0);
        OrderedAggregateGen4Engine oa = new OrderedAggregateGen4Engine(true, Lists.newArrayList(aggr1, aggr2), false, 0, Lists.newArrayList(groupByParams), null, fp);

        VtResultSet want = TestResult.makeTestResult(TestResult.makeTestFields("c1|count(distinct c2)|sum(distinct c3)", "int64|int64|decimal"), "null|2|2", "10|2|4", "20|0|null", "30|1|6", "40|3|1");
        VtResultSet qr = (VtResultSet) oa.execute(VtContext.background(), null, null, false).getVtRowList();
        Assert.assertEquals(want, qr);
    }

    public void testOrderedAggregateCollate() {

    }

    public void testOrderedAggregateCollateAS() {

    }

    public void testOrderedAggregateCollateKS() {

    }
}
