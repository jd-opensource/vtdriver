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
import com.jd.jdbc.engine.util.TestResult;
import com.jd.jdbc.engine.vcursor.LoggingVCursor;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.evalengine.EvalResult;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtRowList;
import com.jd.jdbc.vindexes.VKeyspace;
import com.jd.jdbc.vindexes.hash.Binary;
import com.jd.jdbc.vindexes.hash.BinaryHash;
import io.vitess.proto.Query;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import vschema.Vschema;

public class RouteGen4EngineTest extends BaseTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private final VtResultSet defaultSelectResult = TestResult.makeTestResult(TestResult.makeTestFields("id", "int64"), "1");

    @Test
    public void testSelectUnsharded() throws SQLException {
        RouteGen4Engine sel = new RouteGen4Engine(Engine.RouteOpcode.SelectUnsharded, new VKeyspace("ks", false), "select", "select_field", new MySqlSelectQueryBlock());
        LoggingVCursor vc = new LoggingVCursor(Lists.newArrayList("0"), Lists.newArrayList(defaultSelectResult));
        IExecute.ExecuteMultiShardResponse result = sel.execute(VtContext.background(), vc, new HashMap<>(), false);
        List<String> wants = Lists.newArrayList("ResolveDestinations ks [] Destinations:DestinationAllShard()", "ExecuteMultiShard ks.0: select {} false false");
        vc.expectLog(wants);
        Assert.assertEquals(printFail("testSelectUnsharded is FAIL"), result.getVtRowList(), defaultSelectResult);

//        vc.Rewind()
//        result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
//        require.NoError(t, err)
//        vc.ExpectLog(t, []string{
//      `ResolveDestinations ks [] Destinations:DestinationAllShards()`,
//      `StreamExecuteMulti dummy_select ks.0: {} `,
//        })
//        expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
    }

    @Ignore
    @Test
    public void testInformationSchemaWithTableAndSchemaWithRoutedTables() throws SQLException {
        @AllArgsConstructor
        class TestCase {
            String testName;

            List<String> tableSchema;

            Map<String, EvalEngine.Expr> tableName;

            boolean routed;

            List<String> expectedLog;
        }

        List<TestCase> testCases = new ArrayList<>();
        testCases.add(new TestCase("both schema and table predicates - routed table", Lists.newArrayList("schema"), newTableName("table_name", "table"), true,
            Lists.newArrayList("FindTable(`schema`.`table`)", "ResolveDestinations routedKeyspace [] Destinations:DestinationAnyShard()",
                "ExecuteMultiShard routedKeyspace.1: select {__replacevtschemaname: type:INT64 value:\"1\" table_name: type:VARCHAR value:\"routedTable\"} false false")));
        for (TestCase tc : testCases) {
            RouteGen4Engine sel = new RouteGen4Engine(Engine.RouteOpcode.SelectDBA, new VKeyspace("ks", false), "select", "select_field", new MySqlSelectQueryBlock());
            sel.getRoutingParameters().setSystableTableSchema(stringListToExprList(tc.tableSchema));
            sel.getRoutingParameters().setSystableTableName(tc.tableName);
            LoggingVCursor vc = new LoggingVCursor(Lists.newArrayList("1"), Lists.newArrayList(defaultSelectResult));
            if (tc.routed) {
                Vschema.Table tbl = Vschema.Table.newBuilder().build();
                vc.setTableRoutes(tbl);
            }
            sel.execute(VtContext.background(), vc, null, false);
            vc.expectLog(tc.expectedLog);
        }
    }

    private List<EvalEngine.Expr> stringListToExprList(List<String> in) {
        List<EvalEngine.Expr> schema = new ArrayList<>();
        for (String s : in) {
            schema.add(newLiteralString(s));
        }
        return schema;
    }

    public Map<String, EvalEngine.Expr> newTableName(String table, String val) {
        Map<String, EvalEngine.Expr> res = new HashMap<>();
        res.put(table, newLiteralString(val));
        return res;
    }

    public EvalEngine.Literal newLiteralString(String t) {
        byte[] val = t.getBytes(StandardCharsets.UTF_8);
        return new EvalEngine.Literal(new EvalResult(val, Query.Type.VARBINARY));
    }

    @Test
    public void testSelectScatter() throws SQLException {
        RouteGen4Engine sel = new RouteGen4Engine(Engine.RouteOpcode.SelectScatter, new VKeyspace("ks", true), "select", "select_field", new MySqlSelectQueryBlock());
        LoggingVCursor vc = new LoggingVCursor(Lists.newArrayList("-20", "20-"), Lists.newArrayList(defaultSelectResult));
        IExecute.ExecuteMultiShardResponse result = sel.execute(VtContext.background(), vc, null, false);
        List<String> wants = Lists.newArrayList("ResolveDestinations ks [] Destinations:DestinationAllShard()", "ExecuteMultiShard ks.-20: select {} ks.20-: select {} false false");
        vc.expectLog(wants);
        Assert.assertEquals(printFail(" testSelectScatter is FAIL"), result.getVtRowList(), defaultSelectResult);

        //流式查询
//        vc.Rewind()
//        result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
//        require.NoError(t, err)
//        vc.ExpectLog(t, []string{
//      `ResolveDestinations ks [] Destinations:DestinationAllShards()`,
//      `StreamExecuteMulti dummy_select ks.-20: {} ks.20-: {} `,
//        })
//        expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
    }

    @Test
    public void testSelectEqualUnique() throws SQLException {
        RouteGen4Engine sel = new RouteGen4Engine(Engine.RouteOpcode.SelectEqualUnique, new VKeyspace("ks", true), "select", "select_field", new MySqlSelectQueryBlock());
        sel.getRoutingParameters().setVindex(new BinaryHash());
        sel.getRoutingParameters().setValues(Lists.newArrayList(EvalEngine.newLiteralInt(1L)));

        LoggingVCursor vc = new LoggingVCursor(Lists.newArrayList("-20", "20-"), Lists.newArrayList(defaultSelectResult));
        IExecute.ExecuteMultiShardResponse result = sel.execute(VtContext.background(), vc, null, false);
        List<String> wants =
            Lists.newArrayList("ResolveDestinations ks [type:INT64 value:\"1\"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)", "ExecuteMultiShard ks.-20: select {} false false");
        vc.expectLog(wants);
        Assert.assertEquals(printFail(" testSelectEqualUniqueScatter is FAIL"), defaultSelectResult, result.getVtRowList());
        //流式查询
        /*
            vc.Rewind()
            result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
            require.NoError(t, err)
            vc.ExpectLog(t, []string{
                `ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)`,
                `StreamExecuteMulti dummy_select ks.-20: {} `,
            })
            expectResult(t, "sel.StreamExecute", result, defaultSelectResult)

        */
    }

    @Test
    public void testSelectNone() throws SQLException {
        RouteGen4Engine sel = new RouteGen4Engine(Engine.RouteOpcode.SelectNone, new VKeyspace("ks", true), "select", "select_field", new MySqlSelectQueryBlock());
        sel.getRoutingParameters().setVindex(new Binary());
        LoggingVCursor vc = new LoggingVCursor(Lists.newArrayList("-20", "20-"), new ArrayList<>());
        IExecute.ExecuteMultiShardResponse result = sel.execute(VtContext.background(), vc, null, false);
        Assert.assertEquals(printFail(" testSelectNone is FAIL"), result.getVtRowList(), new VtResultSet());

        vc.rewind();
        // test with special no-routes handling
        sel.setNoRoutesSpecialHandling(true);
        result = sel.execute(VtContext.background(), vc, null, false);
        vc.expectLog(
            Lists.newArrayList("ResolveDestinations ks [] Destinations:DestinationAnyShard()", "ExecuteMultiShard ks.-20: select {} false false")
        );
        Assert.assertEquals(printFail(" testSelectNone is FAIL"), result.getVtRowList(), new VtResultSet());

//        vc.Rewind()
//        result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
//        require.NoError(t, err)
//        vc.ExpectLog(t, []string{
//       `ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
//       `StreamExecuteMulti dummy_select ks.-20: {} `,
//        })
//        expectResult(t, "sel.StreamExecute", result, &sqltypes.Result{})
    }

    @Test
    public void testSelectEqualUniqueScatter() throws SQLException {
        RouteGen4Engine sel = new RouteGen4Engine(Engine.RouteOpcode.SelectEqualUnique, new VKeyspace("ks", true), "select", "select_field", new MySqlSelectQueryBlock());
        sel.getRoutingParameters().setVindex(new BinaryHash());
        sel.getRoutingParameters().setValues(Lists.newArrayList(EvalEngine.newLiteralInt(1L)));
        LoggingVCursor vc = new LoggingVCursor(Lists.newArrayList("-20", "20-"), Lists.newArrayList(defaultSelectResult));
        vc.setShardForKsid(Lists.newArrayList("-20", "20-"));
        IExecute.ExecuteMultiShardResponse result = sel.execute(VtContext.background(), vc, null, false);
        vc.expectLog(
            Lists.newArrayList("ResolveDestinations ks [type:INT64 value:\"1\"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)", "ExecuteMultiShard ks.-20: select {} false false")
        );
        Assert.assertEquals(printFail(" testSelectEqualUniqueScatter is FAIL"), result.getVtRowList(), defaultSelectResult);

//        vc.Rewind()
//        result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
//        require.NoError(t, err)
//        vc.ExpectLog(t, []string{
//      `ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyRange(-)`,
//      `StreamExecuteMulti dummy_select ks.-20: {} ks.20-: {} `,
//        })
//        expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
    }

    @Test
    public void testSelectEqual() throws SQLException {
        RouteGen4Engine sel = new RouteGen4Engine(Engine.RouteOpcode.SelectEqual, new VKeyspace("ks", true), "select", "select_field", new MySqlSelectQueryBlock());
        sel.getRoutingParameters().setVindex(new BinaryHash());
        sel.getRoutingParameters().setValues(Lists.newArrayList(EvalEngine.newLiteralInt(1L)));
        LoggingVCursor vc = new LoggingVCursor(Lists.newArrayList("-20", "20-"), Lists.newArrayList(defaultSelectResult));
        IExecute.ExecuteMultiShardResponse result = sel.execute(VtContext.background(), vc, null, false);
        vc.expectLog(
            Lists.newArrayList("ResolveDestinations ks [type:INT64 value:\"1\"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)", "ExecuteMultiShard ks.-20: select {} false false")
        );
        Assert.assertEquals(printFail(" TestSelectEqual is FAIL"), result.getVtRowList(), defaultSelectResult);

//        vc.Rewind()
//        result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
//        require.NoError(t, err)
//        vc.ExpectLog(t, []string{
//      `ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyRange(-)`,
//      `StreamExecuteMulti dummy_select ks.-20: {} ks.20-: {} `,
//        })
//        expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
    }

    @Test
    public void testSelectEqualNoRoute() throws SQLException {
        RouteGen4Engine sel = new RouteGen4Engine(Engine.RouteOpcode.SelectEqual, new VKeyspace("ks", true), "select", "select_field", new MySqlSelectQueryBlock());
        sel.getRoutingParameters().setVindex(new BinaryHash());
        sel.getRoutingParameters().setValues(Lists.newArrayList(EvalEngine.newLiteralInt(1L)));
        LoggingVCursor vc = new LoggingVCursor(Lists.newArrayList("-20", "20-"), null);
        IExecute.ExecuteMultiShardResponse result = sel.execute(VtContext.background(), vc, null, false);
        vc.expectLog(
            Lists.newArrayList("ResolveDestinations ks [type:INT64 value:\"1\"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)", "ExecuteMultiShard ks.-20: select {} false false")
        );
        Assert.assertEquals(printFail(" testSelectEqualNoRoute is FAIL"), result.getVtRowList(), new VtResultSet());
    }

    @Test
    public void testINUnique() throws SQLException {
        RouteGen4Engine sel = new RouteGen4Engine(Engine.RouteOpcode.SelectIN, new VKeyspace("ks", true), "select", "select_field", new MySqlSelectQueryBlock());
        sel.getRoutingParameters().setVindex(new BinaryHash());
        sel.getRoutingParameters()
            .setValues(Lists.newArrayList(new EvalEngine.TupleExpr(Lists.newArrayList(EvalEngine.newLiteralInt(1L), EvalEngine.newLiteralInt(2L), EvalEngine.newLiteralInt(4L)))));
        LoggingVCursor vc = new LoggingVCursor(Lists.newArrayList("-20", "20-"), Lists.newArrayList(defaultSelectResult));
        vc.setShardForKsid(Lists.newArrayList("-20", "-20", "20-"));
        IExecute.ExecuteMultiShardResponse result = sel.execute(VtContext.background(), vc, new HashMap<>(), false);
        vc.expectLog(Lists.newArrayList(
            "ResolveDestinations ks [type:INT64 value:\"1\" type:INT64 value:\"2\" type:INT64 value:\"4\"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(d2fd8867d50d2dfe)",
            "ExecuteMultiShard ks.-20: select {__vals: type:TUPLE, values:[type:INT64 value:\"1\" type:INT64 value:\"2\"]} ks.20-: select {__vals: type:TUPLE, values:[type:INT64 value:\"4\"]} false false"));
        Assert.assertEquals(printFail(" testINUnique is FAIL"), result.getVtRowList(), defaultSelectResult);

//        vc.Rewind()
//        result, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
//        require.NoError(t, err)
//        vc.ExpectLog(t, []string{
//      `ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyRange(-)`,
//      `StreamExecuteMulti dummy_select ks.-20: {} ks.20-: {} `,
//        })
//        expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
    }

    @Test
    public void testINNonUnique() throws SQLException {
        RouteGen4Engine sel = new RouteGen4Engine(Engine.RouteOpcode.SelectIN, new VKeyspace("ks", true), "select", "select_field", new MySqlSelectQueryBlock());
        sel.getRoutingParameters().setVindex(new BinaryHash());
        sel.getRoutingParameters()
            .setValues(Lists.newArrayList(new EvalEngine.TupleExpr(Lists.newArrayList(EvalEngine.newLiteralInt(1L), EvalEngine.newLiteralInt(2L), EvalEngine.newLiteralInt(4L)))));
        Query.Field[] fields = TestResult.makeTestFields("fromc|toc", "int64|varbinary");
        VtResultSet vtResultSet = TestResult.makeTestResult(fields, "1|\\x00", "1|\\x80", "2|\\x00", "4|\\x80");
        LoggingVCursor vc = new LoggingVCursor(Lists.newArrayList("-20", "20-"), Lists.newArrayList(vtResultSet, defaultSelectResult));
        IExecute.ExecuteMultiShardResponse result = sel.execute(VtContext.background(), vc, new HashMap<>(), false);
        vc.expectLog(Lists.newArrayList(
            "ResolveDestinations ks [type:INT64 value:\"1\" type:INT64 value:\"2\" type:INT64 value:\"4\"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6),DestinationKeyspaceID(06e7ea22ce92708f),DestinationKeyspaceID(d2fd8867d50d2dfe)",
            "ExecuteMultiShard ks.-20: select {__vals: type:TUPLE, values:[type:INT64 value:\"1\" type:INT64 value:\"2\" type:INT64 value:\"4\"]} false false"));
        Assert.assertEquals(printFail(" testINNonUnique is FAIL"), result.getVtRowList(), vtResultSet);
    }

    public void testMultiEqual() throws SQLException {

    }

    @Test
    public void testSelectDBA() throws SQLException {
        RouteGen4Engine sel = new RouteGen4Engine(Engine.RouteOpcode.SelectDBA, new VKeyspace("ks", true), "select", "select_field", new MySqlSelectQueryBlock());
        LoggingVCursor vc = new LoggingVCursor(Lists.newArrayList("-20", "20-"), Lists.newArrayList(defaultSelectResult));
        IExecute.ExecuteMultiShardResponse result = sel.execute(VtContext.background(), vc, new HashMap<>(), false);
        vc.expectLog(Lists.newArrayList("ResolveDestinations ks [] Destinations:DestinationAnyShard()", "ExecuteMultiShard ks.-20: select {} false false"));
        Assert.assertEquals(printFail(" testSelectDBA is FAIL"), result.getVtRowList(), defaultSelectResult);

//        vc.Rewind()
//        result, _ = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
//        vc.ExpectLog(t, []string{
//       `ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
//       `StreamExecuteMulti dummy_select ks.-20: {} `,
//        })
//        expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
    }

    @Test
    public void testSelectReference() throws SQLException {
        RouteGen4Engine sel = new RouteGen4Engine(Engine.RouteOpcode.SelectReference, new VKeyspace("ks", true), "select", "select_field", new MySqlSelectQueryBlock());
        LoggingVCursor vc = new LoggingVCursor(Lists.newArrayList("-20", "20-"), Lists.newArrayList(defaultSelectResult));
        IExecute.ExecuteMultiShardResponse result = sel.execute(VtContext.background(), vc, new HashMap<>(), false);
        vc.expectLog(Lists.newArrayList("ResolveDestinations ks [] Destinations:DestinationAnyShard()", "ExecuteMultiShard ks.-20: select {} false false"));
        Assert.assertEquals(printFail(" testSelectReference is FAIL"), result.getVtRowList(), defaultSelectResult);

//        vc.Rewind()
//        result, _ = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
//        vc.ExpectLog(t, []string{
//        `ResolveDestinations ks [] Destinations:DestinationAnyShard()`,
//        `StreamExecuteMulti dummy_select ks.-20: {} `,
//        })
//        expectResult(t, "sel.StreamExecute", result, defaultSelectResult)
    }

    @Test
    public void testRouteGetFields() throws SQLException {
        RouteGen4Engine sel = new RouteGen4Engine(Engine.RouteOpcode.SelectEqual, new VKeyspace("ks", true), "select", "select_field", new MySqlSelectQueryBlock());
        sel.getRoutingParameters().setVindex(new BinaryHash());
        sel.getRoutingParameters().setValues(Lists.newArrayList(EvalEngine.newLiteralInt(1L)));
        LoggingVCursor vc = new LoggingVCursor(Lists.newArrayList("-20", "20-"), null);

        VtRowList result = sel.execute(VtContext.background(), vc, new HashMap<>(), false).getVtRowList();
        List<String> wants =
            Lists.newArrayList("ResolveDestinations ks [type:INT64 value:\"1\"] Destinations:DestinationKeyspaceID(166b40b44aba4bd6)", "ExecuteMultiShard ks.-20: select {} false false");
        vc.expectLog(wants);
        Assert.assertEquals(printFail("testRouteGetFields is FAIL"), new VtResultSet(), result);
    }

    @Test
    public void testRouteSort() throws SQLException {
        RouteGen4Engine sel = new RouteGen4Engine(Engine.RouteOpcode.SelectUnsharded, new VKeyspace("ks", false), "select", "select_field", new MySqlSelectQueryBlock());
        OrderByParamsGen4 order = new OrderByParamsGen4(0, false, -1, null);
        sel.getOrderBy().add(order);
        LoggingVCursor vc = new LoggingVCursor(Lists.newArrayList("0"), Lists.newArrayList(TestResult.makeTestResult(TestResult.makeTestFields("id", "int64"), "1", "1", "3", "2")));
        VtRowList result = sel.execute(VtContext.background(), vc, new HashMap<>(), false).getVtRowList();
        List<String> wants = Lists.newArrayList("ResolveDestinations ks [] Destinations:DestinationAllShard()", "ExecuteMultiShard ks.0: select {} false false");
        vc.expectLog(wants);
        VtResultSet wantResult = TestResult.makeTestResult(TestResult.makeTestFields("id", "int64"), "1", "1", "2", "3");
        Assert.assertEquals(printFail("testRouteSort is FAIL"), wantResult, result);

        sel.getOrderBy().get(0).setDesc(true);
        vc.rewind();
        result = sel.execute(VtContext.background(), vc, new HashMap<>(), false).getVtRowList();
        wantResult = TestResult.makeTestResult(TestResult.makeTestFields("id", "int64"), "3", "2", "1", "1");
        Assert.assertEquals(printFail("testRouteSort is FAIL"), wantResult, result);

        vc = new LoggingVCursor(Lists.newArrayList("0"), Lists.newArrayList(TestResult.makeTestResult(TestResult.makeTestFields("id", "varchar"), "1", "2", "3")));
        sel.execute(VtContext.background(), vc, new HashMap<>(), false);
//        require.EqualError(t, err, `cannot compare strings, collation is unknown or unsupported (collation ID: 0)`)
    }

    @Test
    public void testRouteSortWeightStrings() throws SQLException {
        RouteGen4Engine sel = new RouteGen4Engine(Engine.RouteOpcode.SelectUnsharded, new VKeyspace("ks", false), "select", "select_field", new MySqlSelectQueryBlock());
        OrderByParamsGen4 order = new OrderByParamsGen4(1, false, 0, null);
        sel.getOrderBy().add(order);
        LoggingVCursor vc = new LoggingVCursor(Lists.newArrayList("0"),
            Lists.newArrayList(TestResult.makeTestResult(TestResult.makeTestFields("weightString|normal", "varbinary|varchar"), "v|x", "g|d", "a|a", "c|t", "f|p")));

        // 后续调整为并发测试
        VtRowList result = sel.execute(VtContext.background(), vc, new HashMap<>(), false).getVtRowList();
        List<String> wants = Lists.newArrayList("ResolveDestinations ks [] Destinations:DestinationAllShard()", "ExecuteMultiShard ks.0: select {} false false");
        vc.expectLog(wants);
        VtResultSet wantResult = TestResult.makeTestResult(TestResult.makeTestFields("weightString|normal", "varbinary|varchar"), "a|a", "c|t", "f|p", "g|d", "v|x");
        Assert.assertEquals(printFail("Sort using Weight Strings is FAIL"), wantResult, result);

        sel.getOrderBy().get(0).setDesc(true);
        vc.rewind();
        result = sel.execute(VtContext.background(), vc, new HashMap<>(), false).getVtRowList();
        wantResult = TestResult.makeTestResult(TestResult.makeTestFields("weightString|normal", "varbinary|varchar"), "v|x", "g|d", "f|p", "c|t", "a|a");
        Assert.assertEquals(printFail("Descending ordering using weighted strings is FAIL"), wantResult, result);

        sel.getOrderBy().clear();
        order = new OrderByParamsGen4(1, false, -1, null);
        sel.getOrderBy().add(order);
        vc = new LoggingVCursor(Lists.newArrayList("0"),
            Lists.newArrayList(TestResult.makeTestResult(TestResult.makeTestFields("weightString|normal", "varbinary|varchar"), "v|x", "g|d", "a|a", "c|t", "f|p")));
        sel.execute(VtContext.background(), vc, new HashMap<>(), false);
//        require.EqualError(t, err, `cannot compare strings, collation is unknown or unsupported (collation ID: 0)`)
    }

    public void testRouteSortCollation() {

    }

    @Test
    public void testRouteSortTruncate() throws SQLException {
        RouteGen4Engine sel = new RouteGen4Engine(Engine.RouteOpcode.SelectUnsharded, new VKeyspace("ks", false), "select", "select_field", new MySqlSelectQueryBlock());
        OrderByParamsGen4 order = new OrderByParamsGen4(0, false, -1, null);
        sel.getOrderBy().add(order);
        sel.setTruncateColumnCount(1);
        LoggingVCursor vc = new LoggingVCursor(Lists.newArrayList("0"),
            Lists.newArrayList(TestResult.makeTestResult(TestResult.makeTestFields("id|col", "int64|int64"), "1|1", "1|1", "3|1", "2|1")));

        VtRowList result = sel.execute(VtContext.background(), vc, new HashMap<>(), false).getVtRowList();
        List<String> wants = Lists.newArrayList("ResolveDestinations ks [] Destinations:DestinationAllShard()", "ExecuteMultiShard ks.0: select {} false false");
        vc.expectLog(wants);
        VtResultSet wantResult = TestResult.makeTestResult(TestResult.makeTestFields("id", "int64"), "1", "1", "2", "3");
        Assert.assertEquals(printFail("testRouteSortTruncate"), wantResult, result);
    }

    @Test
    public void testRouteStreamTruncate() throws SQLException {
        RouteGen4Engine sel = new RouteGen4Engine(Engine.RouteOpcode.SelectUnsharded, new VKeyspace("ks", false), "select", "select_field", new MySqlSelectQueryBlock());
        sel.setTruncateColumnCount(1);
        LoggingVCursor vc = new LoggingVCursor(Lists.newArrayList("0"),
            Lists.newArrayList(TestResult.makeTestResult(TestResult.makeTestFields("id|col", "int64|int64"), "1|1", "2|1")));

        VtRowList result = sel.execute(VtContext.background(), vc, new HashMap<>(), false).getVtRowList();
        List<String> wants = Lists.newArrayList("ResolveDestinations ks [] Destinations:DestinationAllShard()", "ExecuteMultiShard ks.0: select {} false false");
        vc.expectLog(wants);
        VtResultSet wantResult = TestResult.makeTestResult(TestResult.makeTestFields("id", "int64"), "1", "2");
        Assert.assertEquals(printFail("testRouteSortTruncate"), wantResult, result);
    }

    public void testRouteStreamSortTruncate() {

    }

    @Test
    public void testParamsFail() throws SQLException {
        RouteGen4Engine sel = new RouteGen4Engine(Engine.RouteOpcode.SelectUnsharded, new VKeyspace("ks", false), "select", "select_field", new MySqlSelectQueryBlock());
        LoggingVCursor vc = new LoggingVCursor(new SQLException("shard error"));

        thrown.expect(SQLException.class);
        thrown.expectMessage("shard error");
        sel.execute(VtContext.background(), vc, new HashMap<>(), false);

//        vc.Rewind()
//        _, err = wrapStreamExecute(sel, vc, map[string]*querypb.BindVariable{}, false)
//        require.EqualError(t, err, `shard error`)
    }

    @Test
    public void testExecFail() throws SQLException {
        // normal route with no scatter errors as warnings
        RouteGen4Engine sel = new RouteGen4Engine(Engine.RouteOpcode.SelectScatter, new VKeyspace("ks", true), "select", "select_field", new MySqlSelectQueryBlock());
        LoggingVCursor vc = new LoggingVCursor(Lists.newArrayList("-20", "20-"), Lists.newArrayList(defaultSelectResult));
        List<SQLException> sqlExceptions = new ArrayList<>();
        sqlExceptions.add(new SQLException("result error -20"));
        vc.setMultiShardErrs(sqlExceptions);

        thrown.expect(SQLException.class);
        thrown.expectMessage("result error -20");
        sel.execute(VtContext.background(), vc, new HashMap<>(), false);
    }
}
