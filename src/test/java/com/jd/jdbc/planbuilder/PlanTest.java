/*
Copyright 2021 JD Project Authors. Licensed under Apache-2.0.

Copyright 2019 The Vitess Authors.

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

package com.jd.jdbc.planbuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.jd.jdbc.VSchemaManager;
import com.jd.jdbc.common.Hex;
import com.jd.jdbc.common.util.CollectionUtils;
import com.jd.jdbc.engine.ConcatenateEngine;
import com.jd.jdbc.engine.DeleteEngine;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.InsertEngine;
import com.jd.jdbc.engine.JoinEngine;
import com.jd.jdbc.engine.LimitEngine;
import com.jd.jdbc.engine.MemorySortEngine;
import com.jd.jdbc.engine.OrderByParams;
import com.jd.jdbc.engine.OrderedAggregateEngine;
import com.jd.jdbc.engine.Plan;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.ProjectionEngine;
import com.jd.jdbc.engine.PulloutSubqueryEngine;
import com.jd.jdbc.engine.RouteEngine;
import com.jd.jdbc.engine.SendEngine;
import com.jd.jdbc.engine.SingleRowEngine;
import com.jd.jdbc.engine.SubQueryEngine;
import com.jd.jdbc.engine.UpdateEngine;
import com.jd.jdbc.engine.gen4.AbstractAggregateGen4;
import com.jd.jdbc.engine.gen4.ConcatenateGen4Engine;
import com.jd.jdbc.engine.gen4.DistinctGen4Engine;
import com.jd.jdbc.engine.gen4.FilterGen4Engine;
import com.jd.jdbc.engine.gen4.GroupByParams;
import com.jd.jdbc.engine.gen4.JoinGen4Engine;
import com.jd.jdbc.engine.gen4.LimitGen4Engine;
import com.jd.jdbc.engine.gen4.MemorySortGen4Engine;
import com.jd.jdbc.engine.gen4.OrderByParamsGen4;
import com.jd.jdbc.engine.gen4.OrderedAggregateGen4Engine;
import com.jd.jdbc.engine.gen4.RouteGen4Engine;
import com.jd.jdbc.engine.gen4.ScalarAggregateGen4Engine;
import com.jd.jdbc.engine.gen4.SimpleProjectionGen4Engine;
import com.jd.jdbc.engine.vcursor.NoopVCursor;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.evalengine.EvalResult;
import com.jd.jdbc.key.DestinationShard;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqltypes.VtPlanValue;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.util.JsonUtil;
import com.jd.jdbc.vindexes.VKeyspace;
import io.netty.util.internal.StringUtil;
import io.vitess.proto.Query;
import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

public class PlanTest extends AbstractPlanTest {

    private final String samePlanMarker = "Gen4 plan same as above";

    private final String gen4ErrorPrefix = "Gen4 error: ";

    private final String gen3Skip = "Gen3 skip";

    protected VSchemaManager vm;

    @Before
    public void init() throws IOException {
        vm = loadSchema("src/test/resources/plan/plan_schema.json");
    }

    @Test
    public void testPlan() throws IOException {
        testFile("src/test/resources/plan/aggr_cases.txt", vm, 0);
        testFile("src/test/resources/plan/filter_cases.txt", vm, 0);
        testFile("src/test/resources/plan/from_cases.txt", vm, 0);
        testFile("src/test/resources/plan/memory_sort_cases.txt", vm, 0);
        testFile("src/test/resources/plan/postprocess_cases.txt", vm, 0);
        testFile("src/test/resources/plan/select_cases.txt", vm, 0);
        testFile("src/test/resources/plan/union_cases.txt", vm, 0);
        testFile("src/test/resources/plan/dml_insert_cases.txt", vm, 0);
        testFile("src/test/resources/plan/dml_delete_cases.txt", vm, 0);
        testFile("src/test/resources/plan/dml_update_cases.txt", vm, 0);
    }

    @Test
    public void testDestination() throws IOException {
        testFile("src/test/resources/plan/destination_case.txt", vm, 0);
    }

    protected void testFile(String filename, VSchemaManager vm, Integer startPos) throws IOException {
        List<TestCase> testCaseList = iterateExecFile(filename);
        for (TestCase testCase : testCaseList) {

            if (testCase.lineno <= startPos || testCase.output.equals("")) {
                continue;
            }

            printComment("Gen3 Test Case: " + testCase.comments);
            printNormal("Input SQL: " + testCase.input);

            TestPlan fromFile;
            try {
                fromFile = new ObjectMapper().readValue(testCase.output, TestPlan.class);
                printInfo("From File: " + fromFile);
            } catch (Exception e) {
                fromFile = new TestPlan();
                fromFile.setErrorMessage(testCase.output.replaceAll("\"", ""));
                printInfo("From File: " + fromFile.errorMessage);
            }

            try {
                Plan plan = build(testCase.input, vm, true);
                TestPlan fromCode = this.format(plan, testCase.input);
                printInfo("From Code: " + fromCode);
                assertEquals(printFail("File: " + filename + ", Line: " + testCase.lineno + " is [FAIL]"), fromFile, fromCode);
            } catch (Exception e) {
                String error = e.getMessage().replaceAll("\"", "");
                printInfo("From Code: " + error);
                assertEquals(printFail("File: " + filename + ", Line: " + testCase.lineno + " is [FAIL]"), fromFile.errorMessage.toLowerCase(), error.toLowerCase());
            }
            printOk("File: " + filename + ", Line: " + testCase.lineno + " is [OK]");
            System.out.println();
        }
    }

    protected void g4TestFile(String filename, VSchemaManager vm, Integer startPos) throws IOException {
        List<TestCase> testCaseList = iterateExecFile(filename);
        for (TestCase testCase : testCaseList) {

            if (testCase.lineno <= startPos) {
                continue;
            }
            if (testCase.output2ndPlanner.equals("")) {
                continue;

            }
            // our expectation for the new planner on this query is one of three
            //  - it produces the same plan as V3 - this is shown using empty brackets: {\n}
            //  - it produces a different but accepted plan - this is shown using the accepted plan
            //  - or it produces a different plan that has not yet been accepted, or it fails to produce a plan
            //       this is shown by not having any info at all after the result for the V3 planner
            //       with this last expectation, it is an error if the Gen4 planner
            //       produces the same plan as the V3 planner does
            TestPlan g4fromFile;
            try {
                g4fromFile = new ObjectMapper().readValue(testCase.output2ndPlanner, TestPlan.class);
            } catch (Exception e) {
                g4fromFile = new TestPlan();
                g4fromFile.setErrorMessage(testCase.output2ndPlanner.replaceAll("\"", ""));
            }

            try {
                Plan plan = build(testCase.input, vm, false);
                if (plan == null) {
                    continue;
                }
                TestPlan fromCode = this.format(plan, testCase.input);
                if (!g4fromFile.equals(fromCode)) {
                    printComment("Gen4 Test Case: " + testCase.comments);
                    printNormal("Input SQL: " + testCase.input);
                    printInfo("From File: " + g4fromFile);
                    printInfo("From Code: " + fromCode);
                    System.out.println(printFail("File: " + filename + ", Line: " + testCase.lineno + " is [FAIL]"));
                    System.out.println();
                }
            } catch (Exception e) {
                if (e instanceof NullPointerException) {
                    printComment("Gen4 Test Case: " + testCase.comments);
                    printNormal("Input SQL: " + testCase.input);
                    System.out.println("空指针");
                    System.out.println(printFail("File: " + filename + ", Line: " + testCase.lineno + " is [FAIL]"));
                    continue;
                }
                String error;
                if (e.getMessage() != null) {
                    error = e.getMessage().replaceAll("\"", "");
                } else {
                    error = e.toString();
                }
                if (g4fromFile.errorMessage == null || !g4fromFile.errorMessage.toLowerCase().equals(error.toLowerCase())) {
                    printComment("Gen4 Test Case: " + testCase.comments);
                    printNormal("Input SQL: " + testCase.input);
                    error = error.replaceAll("\"", "");
                    if (g4fromFile.errorMessage != null) {
                        printInfo("From File: " + g4fromFile.errorMessage);
                    } else {
                        printInfo("From File: " + g4fromFile);
                    }
                    printInfo("From Code: " + error);
                    System.out.println(printFail("File: " + filename + ", Line: " + testCase.lineno + " is [FAIL],errorMessage " + e.getMessage()));
                    System.out.println();
                }
            }
        }
    }

    protected void g4AssertTestFile(String filename, VSchemaManager vm, Integer startPos) throws IOException {
        List<TestCase> testCaseList = iterateExecFile(filename);
        for (TestCase testCase : testCaseList) {

            if (testCase.lineno <= startPos) {
                continue;
            }
            if (testCase.output2ndPlanner.equals("")) {
                continue;
            }
            // our expectation for the new planner on this query is one of three
            //  - it produces the same plan as V3 - this is shown using empty brackets: {\n}
            //  - it produces a different but accepted plan - this is shown using the accepted plan
            //  - or it produces a different plan that has not yet been accepted, or it fails to produce a plan
            //       this is shown by not having any info at all after the result for the V3 planner
            //       with this last expectation, it is an error if the Gen4 planner
            //       produces the same plan as the V3 planner does
            printComment("Gen4 Test Case: " + testCase.comments);
            printNormal("Input SQL: " + testCase.input);
            TestPlan g4fromFile;
            try {
                g4fromFile = new ObjectMapper().readValue(testCase.output2ndPlanner, TestPlan.class);
            } catch (Exception e) {
                g4fromFile = new TestPlan();
                g4fromFile.setErrorMessage(testCase.output2ndPlanner.replaceAll("\"", ""));
            }
            try {
                Plan plan = build(testCase.input, vm, false);
                if (plan == null) {
                    printInfo("skip test");
                    continue;
                }
                TestPlan fromCode = this.format(plan, testCase.input);
                printInfo("From File: " + g4fromFile);
                printInfo("From Code: " + fromCode);
                assertEquals(printFail("File: " + filename + ", Line: " + testCase.lineno + " is [FAIL]"), g4fromFile, fromCode);
            } catch (Exception e) {
                String error = e.getMessage().replaceAll("\"", "");
                if (g4fromFile.errorMessage != null) {
                    printInfo("From File: " + g4fromFile.errorMessage);
                } else {
                    printInfo("From File: " + g4fromFile);
                }
                printInfo("From Code: " + error);
                assertEquals(printFail("File: " + filename + ", Line: " + testCase.lineno + " is [FAIL]"), g4fromFile.errorMessage.toLowerCase(), error.toLowerCase());
            }
            printOk("File: " + filename + ", Line: " + testCase.lineno + " is [OK]");
            System.out.println();
        }
    }

    private List<TestCase> iterateExecFile(String filename) throws IOException {
        List<TestCase> testCaseList = new ArrayList<>();
        BufferedReader br = Files.newBufferedReader(Paths.get(filename), StandardCharsets.UTF_8);
        String line;
        int lineno = 0;
        StringBuilder comments = new StringBuilder();
        while ((line = br.readLine()) != null) {
            lineno++;
            if (StringUtil.isNullOrEmpty(line) || StringUtil.NEWLINE.equalsIgnoreCase(line)) {
                continue;
            }
            if (line.startsWith("#")) {
                comments.append(line);
                continue;
            }
            line = line.substring(1).substring(0, line.length() - 2);
            String l;
            StringBuilder output = new StringBuilder();
            while (true) {
                l = br.readLine();
                if (l.equals(gen3Skip)) {
                    output = new StringBuilder();
                    break;
                }
                lineno++;
                output.append(l);
                if (l.startsWith("}") || l.startsWith("\"")) {
                    break;
                }
            }
            StringBuilder output2Planner = new StringBuilder();
            l = br.readLine();
            lineno++;
            if (l != null && l.equals(samePlanMarker)) {
                output2Planner = output;
            } else if (l != null && l.startsWith(gen4ErrorPrefix)) {
                output2Planner = new StringBuilder(l.substring(gen4ErrorPrefix.length()));
            } else if (l != null && l.startsWith("{")) {
                output2Planner.append(l);
                while ((l = br.readLine()) != null) {
                    lineno++;
                    output2Planner.append(l);
                    if (l.startsWith("}") || l.startsWith("\"")) {
                        break;
                    }
                }
            } else if (l != null) {
                output2Planner.append(l);
            }
            testCaseList.add(new TestCase(filename, lineno, line, output.toString(), output2Planner.toString(), comments.toString()));
            comments = new StringBuilder();
        }
        return testCaseList;
    }

    private TestPlan format(Plan plan, String query) {
        Instructions instructions = null;

        PrimitiveEngine primitive = plan.getPrimitive();
        if (primitive instanceof RouteEngine) {
            instructions = this.formatRouteEngine((RouteEngine) primitive);
        } else if (primitive instanceof OrderedAggregateEngine) {
            instructions = this.formatOrderedAggregateEngine((OrderedAggregateEngine) primitive);
        } else if (primitive instanceof MemorySortEngine) {
            instructions = this.formatMemorySortEngine((MemorySortEngine) primitive);
        } else if (primitive instanceof LimitEngine) {
            instructions = this.formatLimitEngine((LimitEngine) primitive);
        } else if (primitive instanceof InsertEngine) {
            instructions = this.formatInsertEngine((InsertEngine) primitive);
        } else if (primitive instanceof JoinEngine) {
            instructions = this.formatJoinEngine((JoinEngine) primitive);
        } else if (primitive instanceof SubQueryEngine) {
            instructions = this.formatSubqueryEngine((SubQueryEngine) primitive);
        } else if (primitive instanceof PulloutSubqueryEngine) {
            instructions = this.formatPulloutSubqueryEngine((PulloutSubqueryEngine) primitive);
        } else if (primitive instanceof ProjectionEngine) {
            instructions = this.formatProjectionEngine((ProjectionEngine) primitive);
        } else if (primitive instanceof ConcatenateEngine) {
            instructions = this.formatConcatenateEngine((ConcatenateEngine) primitive);
        } else if (primitive instanceof ConcatenateGen4Engine) {
            instructions = this.formatConcatenateEngine((ConcatenateGen4Engine) primitive);
        } else if (primitive instanceof DistinctGen4Engine) {
            instructions = this.formatDistinctEngine((DistinctGen4Engine) primitive);
        } else if (primitive instanceof DeleteEngine) {
            instructions = this.formatDeleteEngine((DeleteEngine) primitive);
        } else if (primitive instanceof UpdateEngine) {
            instructions = this.formatUpdateEngine((UpdateEngine) primitive);
        } else if (primitive instanceof SendEngine) {
            instructions = this.formatSendEngine((SendEngine) primitive);
        } else if (primitive instanceof ScalarAggregateGen4Engine) {
            instructions = this.formatScalarAggregateGen4Engine((ScalarAggregateGen4Engine) primitive);
        } else if (primitive instanceof MemorySortGen4Engine) {
            instructions = this.formatMemorySortGen4Engine((MemorySortGen4Engine) primitive);
        } else if (primitive instanceof OrderedAggregateGen4Engine) {
            instructions = this.formatOrderedAggregateGen4Engine((OrderedAggregateGen4Engine) primitive);
        } else if (primitive instanceof RouteGen4Engine) {
            instructions = this.formatRouteEngine((RouteGen4Engine) primitive);
        } else if (primitive instanceof LimitGen4Engine) {
            instructions = this.formatLimitEngine((LimitGen4Engine) primitive);
        } else if (primitive instanceof JoinGen4Engine) {
            instructions = this.formatJoinGen4Engine((JoinGen4Engine) primitive);
        } else if (primitive instanceof SimpleProjectionGen4Engine) {
            instructions = this.formatSimpleProjection((SimpleProjectionGen4Engine) primitive);
        } else if (primitive instanceof FilterGen4Engine) {
            instructions = this.formatFilterEngine((FilterGen4Engine) primitive);
        }

        TestPlan testPlan = new TestPlan();
        testPlan.setQueryType(plan.getStatementType().name().replaceAll("Stmt", "").toUpperCase());
        testPlan.setOriginal(query);
        testPlan.setInstructions(instructions);
        return testPlan;
    }

    private Instructions formatDistinctEngine(DistinctGen4Engine engine) {
        Instructions instructions = new Instructions();
        instructions.setOperatorType("Distinct");

        instructions.setResultColumns(engine.getCheckCols().size());

        List<Instructions> inputList = new ArrayList<>();
        PrimitiveEngine engineInput = engine.getSource();
        if (engineInput instanceof RouteGen4Engine) {
            inputList.add(this.formatRouteEngine((RouteGen4Engine) engineInput));
        } else if (engineInput instanceof OrderedAggregateGen4Engine) {
            inputList.add(this.formatOrderedAggregateGen4Engine((OrderedAggregateGen4Engine) engineInput));
        } else if (engineInput instanceof ScalarAggregateGen4Engine) {
            inputList.add(this.formatScalarAggregateGen4Engine((ScalarAggregateGen4Engine) engineInput));
        } else if (engineInput instanceof MemorySortGen4Engine) {
            inputList.add(this.formatMemorySortGen4Engine((MemorySortGen4Engine) engineInput));
        } else if (engineInput instanceof LimitGen4Engine) {
            inputList.add(this.formatLimitEngine((LimitGen4Engine) engineInput));
        } else if (engineInput instanceof JoinGen4Engine) {
            inputList.add(this.formatJoinGen4Engine((JoinGen4Engine) engineInput));
        } else if (engineInput instanceof SimpleProjectionGen4Engine) {
            inputList.add(this.formatSimpleProjection((SimpleProjectionGen4Engine) engineInput));
        } else if (engineInput instanceof FilterGen4Engine) {
            inputList.add(this.formatFilterEngine((FilterGen4Engine) engineInput));
        }
        instructions.setInputs(inputList);
        return instructions;
    }

    private Instructions formatLimitEngine(LimitGen4Engine engine) {
        Instructions instructions = new Instructions();
        instructions.setOperatorType("Limit");
        try {
            if ((engine.getCount() != null) && (engine.getCount() instanceof EvalEngine.Literal)) {
                instructions.setCount(engine.getCount(new NoopVCursor(), null));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        PrimitiveEngine engineInput = engine.getInput();
        if (engineInput != null) {
            List<Instructions> inputList = new ArrayList<>();
            if (engineInput instanceof RouteGen4Engine) {
                inputList.add(this.formatRouteEngine((RouteGen4Engine) engineInput));
            } else if (engineInput instanceof OrderedAggregateGen4Engine) {
                inputList.add(this.formatOrderedAggregateGen4Engine((OrderedAggregateGen4Engine) engineInput));
            } else if (engineInput instanceof ScalarAggregateGen4Engine) {
                inputList.add(this.formatScalarAggregateGen4Engine((ScalarAggregateGen4Engine) engineInput));
            } else if (engineInput instanceof MemorySortGen4Engine) {
                inputList.add(this.formatMemorySortGen4Engine((MemorySortGen4Engine) engineInput));
            } else if (engineInput instanceof JoinGen4Engine) {
                inputList.add(this.formatJoinGen4Engine((JoinGen4Engine) engineInput));
            } else if (engineInput instanceof SimpleProjectionGen4Engine) {
                inputList.add(this.formatSimpleProjection((SimpleProjectionGen4Engine) engineInput));
            } else if (engineInput instanceof FilterGen4Engine) {
                inputList.add(this.formatFilterEngine((FilterGen4Engine) engineInput));
            }
            instructions.setInputs(inputList);
        }
        return instructions;
    }

    private Instructions formatRouteEngine(RouteGen4Engine engine) {
        VKeyspace vKeyspace = engine.getRoutingParameters().getKeyspace();
        Keyspace keyspace = new Keyspace();
        keyspace.setName(vKeyspace.getName());
        keyspace.setSharded(vKeyspace.getSharded());
        Instructions instructions = new Instructions();
        instructions.setOperatorType("Route");
        Engine.RouteOpcode routeOpcode = engine.getRoutingParameters().getRouteOpcode();
        instructions.setVariant(routeOpcode.name());
        instructions.setKeyspace(keyspace);
        instructions.setFieldQuery(engine.getFieldQuery());
        instructions.setQuery(engine.getQuery());
        instructions.setTable(StringUtil.isNullOrEmpty(engine.getTableName()) ? null : engine.getTableName());
        List<EvalEngine.Expr> sysTableKeyspaceExpr = engine.getRoutingParameters().getSystableTableSchema();
        if (sysTableKeyspaceExpr != null && !sysTableKeyspaceExpr.isEmpty()) {
            List<String> exprList = new ArrayList<>();
            for (EvalEngine.Expr expr : sysTableKeyspaceExpr) {
                if (expr instanceof EvalEngine.Literal) {
                    exprList.add(expr.string());
                }
            }
            instructions.setSysTableKeyspaceExpr(exprList);
        }
        if (engine.getTruncateColumnCount() > 0) {
            instructions.setResultColumns(engine.getTruncateColumnCount());
        }
        instructions.setOrderBy(buildOrderbyParamString(engine.getOrderBy()));
        List<Object> values = new ArrayList<>();
        for (EvalEngine.Expr expr : engine.getRoutingParameters().getValues()) {
            if (expr == null) {
                continue;
            }
            String string = expr.string();
            values.add(string);
        }
        if (CollectionUtils.isNotEmpty(values)) {
            instructions.setValueList(values);
        }
        return instructions;
    }

    private String buildOrderbyParamString(List<OrderByParamsGen4> orderBys) {
        if (CollectionUtils.isNotEmpty(orderBys)) {
            List<String> results = new ArrayList<>();
            for (OrderByParamsGen4 obp : orderBys) {
                String val = String.valueOf(obp.getCol());
                if (obp.getStarColFixedIndex() > obp.getCol()) {
                    val = String.valueOf(obp.getStarColFixedIndex());
                }
                if (obp.getWeightStrCol() != -1 && obp.getWeightStrCol() != obp.getCol()) {
                    val = String.format("(%s|%d)", val, obp.getWeightStrCol());
                }
                if (obp.isDesc()) {
                    val += " DESC";
                } else {
                    val += " ASC";
                }
                results.add(val);
            }
            return String.join(", ", results);
        }
        return null;
    }

    private Instructions formatScalarAggregateGen4Engine(ScalarAggregateGen4Engine engine) {
        Instructions instructions = new Instructions();
        instructions.setOperatorType("Aggregate");
        instructions.setVariant("Scalar");

        instructions.setAggregates(buildAggregateParamsString(engine.getAggregates()));
        if (engine.getTruncateColumnCount() > 0) {
            instructions.setResultColumns(engine.getTruncateColumnCount());
        }
        PrimitiveEngine engineInput = engine.getInput();

        if (engineInput != null) {
            List<Instructions> inputList = new ArrayList<>();
            if (engineInput instanceof RouteGen4Engine) {
                inputList.add(this.formatRouteEngine((RouteGen4Engine) engineInput));
            } else if (engineInput instanceof OrderedAggregateGen4Engine) {
                inputList.add(this.formatOrderedAggregateGen4Engine((OrderedAggregateGen4Engine) engineInput));
            } else if (engineInput instanceof ScalarAggregateGen4Engine) {
                inputList.add(this.formatScalarAggregateGen4Engine((ScalarAggregateGen4Engine) engineInput));
            } else if (engineInput instanceof MemorySortGen4Engine) {
                inputList.add(this.formatMemorySortGen4Engine((MemorySortGen4Engine) engineInput));
            } else if (engineInput instanceof LimitGen4Engine) {
                inputList.add(this.formatLimitEngine((LimitGen4Engine) engineInput));
            } else if (engineInput instanceof JoinGen4Engine) {
                inputList.add(this.formatJoinGen4Engine((JoinGen4Engine) engineInput));
            } else if (engineInput instanceof SimpleProjectionGen4Engine) {
                inputList.add(this.formatSimpleProjection((SimpleProjectionGen4Engine) engineInput));
            } else if (engineInput instanceof FilterGen4Engine) {
                inputList.add(this.formatFilterEngine((FilterGen4Engine) engineInput));
            }
            instructions.setInputs(inputList);
        }
        return instructions;
    }

    private String buildAggregateParamsString(List<AbstractAggregateGen4.AggregateParams> aggregates) {
        if (CollectionUtils.isEmpty(aggregates)) {
            return null;
        }
        List<String> results = new ArrayList<>();
        for (AbstractAggregateGen4.AggregateParams ap : aggregates) {
            String result;
            String keyCol = String.valueOf(ap.getCol());
            if (ap.isWAssigned()) {
                keyCol = String.format("%s|%d", keyCol, ap.getWCol());
            }
            String dispOrigOp = "";
            if (ap.getOrigOpcode() != null && ap.getOrigOpcode() != ap.getOpcode()) {
                dispOrigOp = "_" + getAggregateOpcodeG4String(ap.getOrigOpcode());
            }
            if (!ap.getAlias().equals("")) {
                String alias;
                switch (ap.getOpcode()) {
                    case AggregateCountStar:
                    case AggregateCount:
                    case AggregateSum:
                    case AggregateMin:
                    case AggregateMax:
                    case AggregateCountDistinct:
                    case AggregateSumDistinct:
                        alias = ap.getAlias().toLowerCase();
                        break;
                    default:
                        alias = ap.getAlias();
                }

                result = String.format("%s%s(%s) AS %s", getAggregateOpcodeG4String(ap.getOpcode()), dispOrigOp, keyCol, alias);
            } else {
                result = String.format("%s%s(%s)", getAggregateOpcodeG4String(ap.getOpcode()), dispOrigOp, keyCol);
            }
            results.add(result);
        }
        return String.join(", ", results);
    }

    private String getAggregateOpcodeG4String(Engine.AggregateOpcodeG4 opcodeG4) {
        for (Map.Entry<String, Engine.AggregateOpcodeG4> entry : AbstractAggregateGen4.SUPPORTED_AGGREGATES.entrySet()) {
            if (entry.getValue() == opcodeG4) {
                return entry.getKey();
            }
        }
        return "ERROR";
    }

    private Instructions formatSendEngine(SendEngine engine) {
        VKeyspace vKeyspace = engine.getKeyspace();
        Keyspace keyspace = new Keyspace();
        keyspace.setName(vKeyspace.getName());
        keyspace.setSharded(vKeyspace.getSharded());

        Instructions instructions = new Instructions();
        instructions.setOperatorType("Send");
        instructions.setQuery(engine.getQuery());
        instructions.setKeyspace(keyspace);
        instructions.setTableName(engine.getTableName());
        instructions.setMultiShardAutocommit(engine.isMultishardAutocommit());
        DestinationShard destinationShard = (DestinationShard) engine.getTargetDestination();
        instructions.setTargetDestination(destinationShard.getDestination());
        instructions.setDML(engine.isDML());
        instructions.setSingleShardOnly(engine.isSingleShardOnly());
        instructions.setShardNameNeeded(engine.isShardNameNeeded());

        return instructions;
    }

    private Instructions formatRouteEngine(RouteEngine engine) {
        VKeyspace vKeyspace = engine.getKeyspace();
        Engine.RouteOpcode routeOpcode = engine.getRouteOpcode();

        Keyspace keyspace = new Keyspace();
        keyspace.setName(vKeyspace.getName());
        keyspace.setSharded(vKeyspace.getSharded());

        Instructions instructions = new Instructions();
        instructions.setOperatorType("Route");
        instructions.setVariant(routeOpcode.name());
        instructions.setKeyspace(keyspace);
        instructions.setFieldQuery(engine.getFieldQuery());
        instructions.setQuery(engine.getQuery());
        instructions.setTable(StringUtil.isNullOrEmpty(engine.getTableName()) ? null : engine.getTableName());
        List<EvalEngine.Expr> sysTableKeyspaceExpr = engine.getSysTableKeyspaceExpr();
        if (sysTableKeyspaceExpr != null && !sysTableKeyspaceExpr.isEmpty()) {
            List<String> exprList = new ArrayList<>();
            for (EvalEngine.Expr expr : sysTableKeyspaceExpr) {
                if (expr instanceof EvalEngine.Literal) {
                    exprList.add(expr.string());
                }
            }
            instructions.setSysTableKeyspaceExpr(exprList);
        }
        List<Object> values = getValueList(engine.getVtPlanValueList());
        if (values != null) {
            instructions.setValueList(values);
        }
        return instructions;
    }

    private Instructions formatOrderedAggregateEngine(OrderedAggregateEngine engine) {
        Instructions instructions = new Instructions();
        instructions.setOperatorType("Aggregate");
        instructions.setVariant("Ordered");

        List<OrderedAggregateEngine.AggregateParams> aggregateParamsList = engine.getAggregateParamsList();
        if (aggregateParamsList != null && !aggregateParamsList.isEmpty()) {
            for (OrderedAggregateEngine.AggregateParams aggregateParams : aggregateParamsList) {
                Engine.AggregateOpcode opcode = aggregateParams.getOpcode();
                Integer col = aggregateParams.getCol();
                switch (opcode) {
                    case AggregateCount:
                        instructions.setAggregates("count(" + col + ")");
                        break;
                    case AggregateSum:
                        instructions.setAggregates("sum(" + col + ")");
                        break;
                    case AggregateMin:
                        instructions.setAggregates("min(" + col + ")");
                        break;
                    case AggregateMax:
                        instructions.setAggregates("max(" + col + ")");
                        break;
                    case AggregateCountDistinct:
                        instructions.setAggregates("count_distinct(" + col + ") AS " + aggregateParams.getAlias().toLowerCase());
                        break;
                    case AggregateSumDistinct:
                        instructions.setAggregates("sum_distinct(" + col + ") AS " + aggregateParams.getAlias().toLowerCase());
                        break;
                    case AggregateAvgSum:
                        instructions.setAggregates("avg_sum(" + col + ") AS " + aggregateParams.getAlias().toLowerCase() + ";");
                        break;
                    case AggregateAvgCount:
                        String pre = instructions.getAggregates();
                        instructions.setAggregates(pre + "avg_count(" + col + ")");
                        break;
                }
            }
        }

        instructions.setDistinct(engine.isHasDistinct());

        List<Integer> keyList = engine.getKeyList();
        if (keyList != null && !keyList.isEmpty()) {
            instructions.setGroupBy(keyList.stream().map(String::valueOf).collect(Collectors.joining(", ")));
        }
        PrimitiveEngine engineInput = engine.getInput();
        if (engineInput != null) {
            List<Instructions> inputList = new ArrayList<>();
            if (engineInput instanceof RouteEngine) {
                inputList.add(this.formatRouteEngine((RouteEngine) engineInput));
            } else if (engineInput instanceof OrderedAggregateEngine) {
                inputList.add(this.formatOrderedAggregateEngine((OrderedAggregateEngine) engineInput));
            } else if (engineInput instanceof MemorySortEngine) {
                inputList.add(this.formatMemorySortEngine((MemorySortEngine) engineInput));
            } else if (engineInput instanceof LimitEngine) {
                inputList.add(this.formatLimitEngine((LimitEngine) engineInput));
            }
            instructions.setInputs(inputList);
        }
        return instructions;
    }

    private Instructions formatOrderedAggregateGen4Engine(OrderedAggregateGen4Engine engine) {
        Instructions instructions = new Instructions();
        instructions.setOperatorType("Aggregate");
        instructions.setVariant("Ordered");

        instructions.setGroupBy(buildGroupByParamsString(engine.getGroupByKeys()));
        instructions.setAggregates(buildAggregateParamsString(engine.getAggregates()));

        if (engine.getTruncateColumnCount() > 0) {
            instructions.setResultColumns(engine.getTruncateColumnCount());
        }

        PrimitiveEngine engineInput = engine.getInput();
        if (engineInput != null) {
            List<Instructions> inputList = new ArrayList<>();
            if (engineInput instanceof RouteGen4Engine) {
                inputList.add(this.formatRouteEngine((RouteGen4Engine) engineInput));
            } else if (engineInput instanceof OrderedAggregateGen4Engine) {
                inputList.add(this.formatOrderedAggregateGen4Engine((OrderedAggregateGen4Engine) engineInput));
            } else if (engineInput instanceof ScalarAggregateGen4Engine) {
                inputList.add(this.formatScalarAggregateGen4Engine((ScalarAggregateGen4Engine) engineInput));
            } else if (engineInput instanceof MemorySortGen4Engine) {
                inputList.add(this.formatMemorySortGen4Engine((MemorySortGen4Engine) engineInput));
            } else if (engineInput instanceof LimitGen4Engine) {
                inputList.add(this.formatLimitEngine((LimitGen4Engine) engineInput));
            } else if (engineInput instanceof JoinGen4Engine) {
                inputList.add(this.formatJoinGen4Engine((JoinGen4Engine) engineInput));
            } else if (engineInput instanceof SimpleProjectionGen4Engine) {
                inputList.add(this.formatSimpleProjection((SimpleProjectionGen4Engine) engineInput));
            } else if (engineInput instanceof FilterGen4Engine) {
                inputList.add(this.formatFilterEngine((FilterGen4Engine) engineInput));
            }
            instructions.setInputs(inputList);
        }
        return instructions;
    }

    private String buildGroupByParamsString(List<GroupByParams> groupByKeys) {
        if (CollectionUtils.isEmpty(groupByKeys)) {
            return null;
        }
        List<String> results = new ArrayList<>();
        for (GroupByParams gbp : groupByKeys) {
            String result;
            if (gbp.getWeightStringCol() == -1 || gbp.getKeyCol().equals(gbp.getWeightStringCol())) {
                result = String.valueOf(gbp.getKeyCol());
            } else {
                result = String.format("(%d|%d)", gbp.getKeyCol(), gbp.getWeightStringCol());
            }
            results.add(result);
        }
        return String.join(", ", results);
    }

    private Instructions formatMemorySortEngine(MemorySortEngine engine) {
        Instructions instructions = new Instructions();
        instructions.setOperatorType("Sort");
        instructions.setVariant("Memory");

        List<OrderByParams> orderByParamsList = engine.getOrderByParams();
        if (orderByParamsList != null && !orderByParamsList.isEmpty()) {
            String orderBy = orderByParamsList.stream()
                .map(orderByParams -> orderByParams.getCol() + " " + (orderByParams.getDesc() ? "DESC" : "ASC"))
                .collect(Collectors.joining(", "));
            instructions.setOrderBy(orderBy);
        }

        PrimitiveEngine engineInput = engine.getInput();
        if (engineInput != null) {
            List<Instructions> inputList = new ArrayList<>();
            if (engineInput instanceof RouteEngine) {
                inputList.add(this.formatRouteEngine((RouteEngine) engineInput));
            } else if (engineInput instanceof OrderedAggregateEngine) {
                inputList.add(this.formatOrderedAggregateEngine((OrderedAggregateEngine) engineInput));
            } else if (engineInput instanceof MemorySortEngine) {
                inputList.add(this.formatMemorySortEngine((MemorySortEngine) engineInput));
            } else if (engineInput instanceof LimitEngine) {
                inputList.add(this.formatLimitEngine((LimitEngine) engineInput));
            } else if (engineInput instanceof JoinEngine) {
                inputList.add(this.formatJoinEngine((JoinEngine) engineInput));
            } else if (engineInput instanceof SubQueryEngine) {
                inputList.add(this.formatSubqueryEngine((SubQueryEngine) engineInput));
            }
            instructions.setInputs(inputList);
        }
        return instructions;
    }

    private Instructions formatMemorySortGen4Engine(MemorySortGen4Engine engine) {
        Instructions instructions = new Instructions();
        instructions.setOperatorType("Sort");
        instructions.setVariant("Memory");
        instructions.setOrderBy(buildOrderbyParamString(engine.getOrderByParams()));

        if (engine.getTruncateColumnCount() > 0) {
            instructions.setResultColumns(engine.getTruncateColumnCount());
        }

        PrimitiveEngine engineInput = engine.getInput();
        if (engineInput != null) {
            List<Instructions> inputList = new ArrayList<>();
            if (engineInput instanceof RouteGen4Engine) {
                inputList.add(this.formatRouteEngine((RouteGen4Engine) engineInput));
            } else if (engineInput instanceof OrderedAggregateGen4Engine) {
                inputList.add(this.formatOrderedAggregateGen4Engine((OrderedAggregateGen4Engine) engineInput));
            } else if (engineInput instanceof ScalarAggregateGen4Engine) {
                inputList.add(this.formatScalarAggregateGen4Engine((ScalarAggregateGen4Engine) engineInput));
            } else if (engineInput instanceof MemorySortGen4Engine) {
                inputList.add(this.formatMemorySortGen4Engine((MemorySortGen4Engine) engineInput));
            } else if (engineInput instanceof LimitGen4Engine) {
                inputList.add(this.formatLimitEngine((LimitGen4Engine) engineInput));
            } else if (engineInput instanceof JoinGen4Engine) {
                inputList.add(this.formatJoinGen4Engine((JoinGen4Engine) engineInput));
            } else if (engineInput instanceof SimpleProjectionGen4Engine) {
                inputList.add(this.formatSimpleProjection((SimpleProjectionGen4Engine) engineInput));
            } else if (engineInput instanceof FilterGen4Engine) {
                inputList.add(this.formatFilterEngine((FilterGen4Engine) engineInput));
            }

            instructions.setInputs(inputList);
        }
        return instructions;
    }

    private Instructions formatLimitEngine(LimitEngine engine) {
        Instructions instructions = new Instructions();
        instructions.setOperatorType("Limit");
        try {
            if (!engine.getCount().getVtValue().isNull()) {
                instructions.setCount(engine.getCount().getVtValue().toInt());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        PrimitiveEngine engineInput = engine.getInput();
        if (engineInput != null) {
            List<Instructions> inputList = new ArrayList<>();
            if (engineInput instanceof RouteEngine) {
                inputList.add(this.formatRouteEngine((RouteEngine) engineInput));
            } else if (engineInput instanceof OrderedAggregateEngine) {
                inputList.add(this.formatOrderedAggregateEngine((OrderedAggregateEngine) engineInput));
            } else if (engineInput instanceof MemorySortEngine) {
                inputList.add(this.formatMemorySortEngine((MemorySortEngine) engineInput));
            } else if (engineInput instanceof LimitEngine) {
                inputList.add(this.formatLimitEngine((LimitEngine) engineInput));
            } else if (engineInput instanceof JoinEngine) {
                inputList.add(this.formatJoinEngine((JoinEngine) engineInput));
            } else if (engineInput instanceof SubQueryEngine) {
                inputList.add(this.formatSubqueryEngine((SubQueryEngine) engineInput));
            } else if (engineInput instanceof PulloutSubqueryEngine) {
                inputList.add(this.formatPulloutSubqueryEngine((PulloutSubqueryEngine) engineInput));
            }
            instructions.setInputs(inputList);
        }
        return instructions;
    }

    private Instructions formatInsertEngine(InsertEngine engine) {
        VKeyspace vKeyspace = engine.getKeyspace();
        Keyspace keyspace = new Keyspace();
        keyspace.setName(vKeyspace.getName());
        keyspace.setSharded(vKeyspace.getSharded());

        Instructions instructions = new Instructions();
        instructions.setOperatorType("Insert");
        instructions.setVariant(engine.getInsertOpcode().name().replace("Insert", ""));
        instructions.setKeyspace(keyspace);
        instructions.setTargetTabletType("MASTER");
        instructions.setMultiShardAutocommit(engine.getMultiShardAutocommit());
        instructions.setQuery(engine.getQuery());
        instructions.setTableName(engine.getTableName());
        return instructions;
    }

    private Instructions formatJoinEngine(JoinEngine engine) {
        Instructions instructions = new Instructions();
        instructions.setOperatorType("Join");
        instructions.setVariant(engine.getOpcode().name().replace("Normal", ""));
        instructions.setJoinColumnIndexes(engine.getCols());
        instructions.setTableName(engine.getTableName());

        List<PrimitiveEngine> inputs = engine.inputs();
        List<Instructions> inputList = new ArrayList<>();
        for (PrimitiveEngine engineInput : inputs) {
            if (engineInput != null) {
                if (engineInput instanceof RouteEngine) {
                    inputList.add(this.formatRouteEngine((RouteEngine) engineInput));
                } else if (engineInput instanceof OrderedAggregateEngine) {
                    inputList.add(this.formatOrderedAggregateEngine((OrderedAggregateEngine) engineInput));
                } else if (engineInput instanceof MemorySortEngine) {
                    inputList.add(this.formatMemorySortEngine((MemorySortEngine) engineInput));
                } else if (engineInput instanceof LimitEngine) {
                    inputList.add(this.formatLimitEngine((LimitEngine) engineInput));
                }
            }
        }
        instructions.setInputs(inputList);
        return instructions;
    }

    private Instructions formatSimpleProjection(SimpleProjectionGen4Engine engine) {
        Instructions instructions = new Instructions();
        instructions.setOperatorType("SimpleProjection");
        instructions.setColumns(engine.getCols());
        List<PrimitiveEngine> inputs = engine.inputs();
        List<Instructions> inputList = new ArrayList<>();
        for (PrimitiveEngine engineInput : inputs) {
            if (engineInput instanceof RouteGen4Engine) {
                inputList.add(this.formatRouteEngine((RouteGen4Engine) engineInput));
            } else if (engineInput instanceof OrderedAggregateGen4Engine) {
                inputList.add(this.formatOrderedAggregateGen4Engine((OrderedAggregateGen4Engine) engineInput));
            } else if (engineInput instanceof ScalarAggregateGen4Engine) {
                inputList.add(this.formatScalarAggregateGen4Engine((ScalarAggregateGen4Engine) engineInput));
            } else if (engineInput instanceof MemorySortGen4Engine) {
                inputList.add(this.formatMemorySortGen4Engine((MemorySortGen4Engine) engineInput));
            } else if (engineInput instanceof LimitGen4Engine) {
                inputList.add(this.formatLimitEngine((LimitGen4Engine) engineInput));
            } else if (engineInput instanceof JoinGen4Engine) {
                inputList.add(this.formatJoinGen4Engine((JoinGen4Engine) engineInput));
            } else if (engineInput instanceof SimpleProjectionGen4Engine) {
                inputList.add(this.formatSimpleProjection((SimpleProjectionGen4Engine) engineInput));
            } else if (engineInput instanceof FilterGen4Engine) {
                inputList.add(this.formatFilterEngine((FilterGen4Engine) engineInput));
            }
        }
        instructions.setInputs(inputList);
        return instructions;
    }

    private Instructions formatFilterEngine(FilterGen4Engine engine) {
        Instructions instructions = new Instructions();
        instructions.setOperatorType("Filter");
        instructions.setPredicate(engine.getAstPredicate().toString());

        List<PrimitiveEngine> inputs = engine.inputs();
        List<Instructions> inputList = new ArrayList<>();
        for (PrimitiveEngine engineInput : inputs) {
            if (engineInput instanceof RouteGen4Engine) {
                inputList.add(this.formatRouteEngine((RouteGen4Engine) engineInput));
            } else if (engineInput instanceof OrderedAggregateGen4Engine) {
                inputList.add(this.formatOrderedAggregateGen4Engine((OrderedAggregateGen4Engine) engineInput));
            } else if (engineInput instanceof ScalarAggregateGen4Engine) {
                inputList.add(this.formatScalarAggregateGen4Engine((ScalarAggregateGen4Engine) engineInput));
            } else if (engineInput instanceof MemorySortGen4Engine) {
                inputList.add(this.formatMemorySortGen4Engine((MemorySortGen4Engine) engineInput));
            } else if (engineInput instanceof LimitGen4Engine) {
                inputList.add(this.formatLimitEngine((LimitGen4Engine) engineInput));
            } else if (engineInput instanceof JoinGen4Engine) {
                inputList.add(this.formatJoinGen4Engine((JoinGen4Engine) engineInput));
            } else if (engineInput instanceof SimpleProjectionGen4Engine) {
                inputList.add(this.formatSimpleProjection((SimpleProjectionGen4Engine) engineInput));
            } else if (engineInput instanceof FilterGen4Engine) {
                inputList.add(this.formatFilterEngine((FilterGen4Engine) engineInput));
            }
        }
        instructions.setInputs(inputList);
        return instructions;
    }

    private Instructions formatJoinGen4Engine(JoinGen4Engine engine) {
        Instructions instructions = new Instructions();
        instructions.setOperatorType("Join");
        instructions.setVariant(engine.getOpcode().name().replace("Normal", ""));
        instructions.setJoinColumnIndexes(engine.getCols());
        instructions.setTableName(engine.getTableName());

        List<PrimitiveEngine> inputs = engine.inputs();
        List<Instructions> inputList = new ArrayList<>();
        for (PrimitiveEngine engineInput : inputs) {
            if (engineInput instanceof RouteGen4Engine) {
                inputList.add(this.formatRouteEngine((RouteGen4Engine) engineInput));
            } else if (engineInput instanceof OrderedAggregateGen4Engine) {
                inputList.add(this.formatOrderedAggregateGen4Engine((OrderedAggregateGen4Engine) engineInput));
            } else if (engineInput instanceof ScalarAggregateGen4Engine) {
                inputList.add(this.formatScalarAggregateGen4Engine((ScalarAggregateGen4Engine) engineInput));
            } else if (engineInput instanceof MemorySortGen4Engine) {
                inputList.add(this.formatMemorySortGen4Engine((MemorySortGen4Engine) engineInput));
            } else if (engineInput instanceof LimitGen4Engine) {
                inputList.add(this.formatLimitEngine((LimitGen4Engine) engineInput));
            } else if (engineInput instanceof JoinGen4Engine) {
                inputList.add(this.formatJoinGen4Engine((JoinGen4Engine) engineInput));
            } else if (engineInput instanceof SimpleProjectionGen4Engine) {
                inputList.add(this.formatSimpleProjection((SimpleProjectionGen4Engine) engineInput));
            } else if (engineInput instanceof FilterGen4Engine) {
                inputList.add(this.formatFilterEngine((FilterGen4Engine) engineInput));
            }
        }
        instructions.setInputs(inputList);
        return instructions;
    }

    private Instructions formatSubqueryEngine(SubQueryEngine engine) {
        Instructions instructions = new Instructions();
        instructions.setOperatorType("Subquery");
        instructions.setColumns(engine.getCols());

        PrimitiveEngine engineInput = engine.getSubqueryEngine();
        if (engineInput != null) {
            List<Instructions> inputList = new ArrayList<>();
            if (engineInput instanceof RouteEngine) {
                inputList.add(this.formatRouteEngine((RouteEngine) engineInput));
            } else if (engineInput instanceof OrderedAggregateEngine) {
                inputList.add(this.formatOrderedAggregateEngine((OrderedAggregateEngine) engineInput));
            } else if (engineInput instanceof MemorySortEngine) {
                inputList.add(this.formatMemorySortEngine((MemorySortEngine) engineInput));
            } else if (engineInput instanceof LimitEngine) {
                inputList.add(this.formatLimitEngine((LimitEngine) engineInput));
            } else if (engineInput instanceof JoinEngine) {
                inputList.add(this.formatJoinEngine((JoinEngine) engineInput));
            } else if (engineInput instanceof ConcatenateEngine) {
                inputList.add(this.formatConcatenateEngine((ConcatenateEngine) engineInput));
            }
            instructions.setInputs(inputList);
        }
        return instructions;
    }

    private Instructions formatPulloutSubqueryEngine(PulloutSubqueryEngine engine) {
        Instructions instructions = new Instructions();
        instructions.setOperatorType("Subquery");
        instructions.setVariant(engine.getOpcode().name());

        PrimitiveEngine subqueryEngine = engine.getSubquery();
        PrimitiveEngine underlyingEngine = engine.getUnderlying();
        List<Instructions> inputList = new ArrayList<>();
        if (subqueryEngine instanceof RouteEngine) {
            inputList.add(this.formatRouteEngine((RouteEngine) subqueryEngine));
        } else if (subqueryEngine instanceof PulloutSubqueryEngine) {
            inputList.add(this.formatPulloutSubqueryEngine((PulloutSubqueryEngine) subqueryEngine));
        }
        if (underlyingEngine instanceof RouteEngine) {
            inputList.add(this.formatRouteEngine((RouteEngine) underlyingEngine));
        } else if (underlyingEngine instanceof PulloutSubqueryEngine) {
            inputList.add(this.formatPulloutSubqueryEngine((PulloutSubqueryEngine) underlyingEngine));
        }
        instructions.setInputs(inputList);
        return instructions;
    }

    private Instructions formatProjectionEngine(ProjectionEngine engine) {
        Instructions instructions = new Instructions();
        instructions.setOperatorType("Projection");
        instructions.setStrColumns(engine.getCols());

        List<String> expressionList = new ArrayList<>();
        for (EvalEngine.Expr expr : engine.getExprs()) {
            String str = "";
            if (expr instanceof EvalEngine.Literal) {
                str = this.formatEvalResult(((EvalEngine.Literal) expr).getVal());
            } else if (expr instanceof EvalEngine.BinaryOp) {
                EvalEngine.Expr leftExpr = ((EvalEngine.BinaryOp) expr).getLeft();
                if (leftExpr instanceof EvalEngine.Literal) {
                    str = this.formatEvalResult(((EvalEngine.Literal) leftExpr).getVal());
                }
                str += " " + ((EvalEngine.BinaryOp) expr).getExpr().string() + " ";
                EvalEngine.Expr rightExpr = ((EvalEngine.BinaryOp) expr).getRight();
                if (rightExpr instanceof EvalEngine.Literal) {
                    str += this.formatEvalResult(((EvalEngine.Literal) rightExpr).getVal());
                }
            }
            expressionList.add(str);
        }
        instructions.setExpressions(expressionList);

        PrimitiveEngine inputEngine = engine.getInput();
        List<Instructions> inputList = new ArrayList<>();
        if (inputEngine instanceof SingleRowEngine) {
            inputList.add(this.formatSingleRowEngine());
        }
        instructions.setInputs(inputList);
        return instructions;
    }

    private Instructions formatSingleRowEngine() {
        Instructions instructions = new Instructions();
        instructions.setOperatorType("SingleRow");
        return instructions;
    }

    private Instructions formatConcatenateEngine(ConcatenateEngine engine) {
        Instructions instructions = new Instructions();
        instructions.setOperatorType("Concatenate");

        List<Instructions> inputList = new ArrayList<>();
        for (PrimitiveEngine engineInput : engine.getSourceList()) {
            if (engineInput != null) {
                if (engineInput instanceof RouteEngine) {
                    inputList.add(this.formatRouteEngine((RouteEngine) engineInput));
                } else if (engineInput instanceof OrderedAggregateEngine) {
                    inputList.add(this.formatOrderedAggregateEngine((OrderedAggregateEngine) engineInput));
                } else if (engineInput instanceof MemorySortEngine) {
                    inputList.add(this.formatMemorySortEngine((MemorySortEngine) engineInput));
                } else if (engineInput instanceof LimitEngine) {
                    inputList.add(this.formatLimitEngine((LimitEngine) engineInput));
                }
            }
        }
        instructions.setInputs(inputList);
        return instructions;
    }

    private Instructions formatConcatenateEngine(ConcatenateGen4Engine engine) {
        Instructions instructions = new Instructions();
        instructions.setOperatorType("Concatenate");

        List<Instructions> inputList = new ArrayList<>();
        for (PrimitiveEngine engineInput : engine.getSourceList()) {
            if (engineInput instanceof RouteGen4Engine) {
                inputList.add(this.formatRouteEngine((RouteGen4Engine) engineInput));
            } else if (engineInput instanceof OrderedAggregateGen4Engine) {
                inputList.add(this.formatOrderedAggregateGen4Engine((OrderedAggregateGen4Engine) engineInput));
            } else if (engineInput instanceof ScalarAggregateGen4Engine) {
                inputList.add(this.formatScalarAggregateGen4Engine((ScalarAggregateGen4Engine) engineInput));
            } else if (engineInput instanceof MemorySortGen4Engine) {
                inputList.add(this.formatMemorySortGen4Engine((MemorySortGen4Engine) engineInput));
            } else if (engineInput instanceof LimitGen4Engine) {
                inputList.add(this.formatLimitEngine((LimitGen4Engine) engineInput));
            } else if (engineInput instanceof JoinGen4Engine) {
                inputList.add(this.formatJoinGen4Engine((JoinGen4Engine) engineInput));
            } else if (engineInput instanceof SimpleProjectionGen4Engine) {
                inputList.add(this.formatSimpleProjection((SimpleProjectionGen4Engine) engineInput));
            } else if (engineInput instanceof FilterGen4Engine) {
                inputList.add(this.formatFilterEngine((FilterGen4Engine) engineInput));
            }
        }
        instructions.setInputs(inputList);
        return instructions;
    }

    private Instructions formatUpdateEngine(UpdateEngine engine) {
        VKeyspace vKeyspace = engine.getKeyspace();
        Keyspace keyspace = new Keyspace();
        keyspace.setName(vKeyspace.getName());
        keyspace.setSharded(vKeyspace.getSharded());

        Instructions instructions = new Instructions();
        instructions.setOperatorType("Update");
        instructions.setVariant(engine.getOpcode().name());
        instructions.setKeyspace(keyspace);
        instructions.setTargetTabletType("MASTER");
        instructions.setMultiShardAutocommit(engine.isMultiShardAutocommit());
        instructions.setQuery(SQLUtils.toMySqlString(engine.getQuery(), SQLUtils.NOT_FORMAT_OPTION));
        instructions.setTableName(engine.getTableName());
        List<Object> values = getValueList(engine.getVtPlanValueList());
        if (values != null) {
            instructions.setValueList(values);
        }
        return instructions;
    }

    private Instructions formatDeleteEngine(DeleteEngine engine) {
        VKeyspace vKeyspace = engine.getKeyspace();
        Keyspace keyspace = new Keyspace();
        keyspace.setName(vKeyspace.getName());
        keyspace.setSharded(vKeyspace.getSharded());

        Instructions instructions = new Instructions();
        instructions.setOperatorType("Delete");
        instructions.setVariant(engine.getOpcode().name());
        instructions.setKeyspace(keyspace);
        instructions.setTargetTabletType("MASTER");
        instructions.setMultiShardAutocommit(engine.isMultiShardAutocommit());
        instructions.setQuery(SQLUtils.toMySqlString(engine.getQuery(), SQLUtils.NOT_FORMAT_OPTION));
        instructions.setTableName(engine.getTableName());
        List<Object> values = getValueList(engine.getVtPlanValueList());
        if (values != null) {
            instructions.setValueList(values);
        }
        return instructions;
    }

    private String formatEvalResult(EvalResult evalResult) {
        String str = "";
        Query.Type type = evalResult.getType();
        switch (type) {
            case INT64:
                str = type + "(" + evalResult.getIval() + ")";
                break;
            case UINT64:
                str = type + "(" + evalResult.getUval() + ")";
                break;
            case FLOAT64:
                str = type + "(" + evalResult.getFval() + ")";
                break;
            default:
                break;
        }
        return str;
    }

    private List<Object> getValueList(List<VtPlanValue> vtPlanValueList) {
        if (CollectionUtils.isEmpty(vtPlanValueList)) {
            return null;
        }
        List<Object> values = new ArrayList<>();
        VtPlanValue vtPlanValue1 = vtPlanValueList.get(0);
        if (vtPlanValue1.getVtValue() != null && !vtPlanValue1.getVtValue().isNull() && CollectionUtils.isEmpty(vtPlanValue1.getVtPlanValueList())) {
            values.add(getVtValue(vtPlanValue1.getVtValue()));
        }
        if (vtPlanValue1.getVtValue() != null && vtPlanValue1.getVtValue().isNull() && StringUtils.isNotEmpty(vtPlanValue1.getKey())) {
            values.add(":" + vtPlanValue1.getKey());
        }
        if (vtPlanValue1.getVtValue() == null && StringUtils.isNotEmpty(vtPlanValue1.getListKey())) {
            values.add("::" + vtPlanValue1.getListKey());
        }

        List<String> invalues = new ArrayList<>();
        for (VtPlanValue vtPlanValue : vtPlanValue1.getVtPlanValueList()) {
            if (vtPlanValue.getVtValue() != null && !vtPlanValue.getVtValue().isNull() && CollectionUtils.isEmpty(vtPlanValue.getVtPlanValueList())) {
                invalues.add(getVtValue(vtPlanValue.getVtValue()));
            }
            if (vtPlanValue.getVtValue() != null && vtPlanValue.getVtValue().isNull() && StringUtils.isNotEmpty(vtPlanValue.getKey())) {
                invalues.add(":" + vtPlanValue.getKey());
            }
            if (vtPlanValue.getVtValue() == null && StringUtils.isNotEmpty(vtPlanValue.getListKey())) {
                values.add("::" + vtPlanValue.getListKey());
            }
        }
        if (CollectionUtils.isNotEmpty(invalues)) {
            String join = String.join(", ", invalues);
            values.add("(" + join + ")");
        }
        return values;
    }

    private String getVtValue(VtValue vtValue) {
        Object object = null;
        try {
            switch (vtValue.getVtType()) {
                case CHAR:
                case VARBINARY:
                case VARCHAR:
                case TEXT:
                case TIME:
                    object = "\"" + vtValue + "\"";
                    break;
                case BIT:
                    object = vtValue.toBoolean();
                    break;
                case INT8:
                case UINT8:
                case INT16:
                case UINT16:
                case INT24:
                case UINT24:
                case INT32:
                    object = vtValue.toInt();
                    break;
                case UINT32:
                case INT64:
                    object = vtValue.toLong();
                    break;
                case UINT64:
                    object = new BigInteger(vtValue.toString());
                    break;
                case DECIMAL:
                case FLOAT32:
                case FLOAT64:
                    object = vtValue.toDecimal();
                    break;
                case DATE:
                case YEAR:
                    object = vtValue.toString();
                    break;
                case DATETIME:
                case TIMESTAMP:
                    object = vtValue.toString();
                    break;
                case BLOB:
                case BINARY:
                    object = Hex.encodeHexString(vtValue.getVtValue());
                    break;
                case NULL_TYPE:
                    object = null;
                    break;
                default:
                    throw new RuntimeException("unknown data type:" + vtValue.getVtType());
            }
        } catch (SQLException throwables) {
            Assert.fail();
        }
        return vtValue.getVtType().toString().toUpperCase() + "(" + object + ")";
    }

    @Data
    @AllArgsConstructor
    private static class TestCase {
        private String file;

        private Integer lineno;

        private String input;

        private String output;

        private String output2ndPlanner;

        private String comments;
    }

    @Data
    private static class TestPlan {
        @JsonProperty(value = "QueryType", index = 1)
        private String queryType;

        @JsonProperty(value = "Original", index = 2)
        private String original;

        @JsonProperty(value = "Instructions", index = 3)
        private Instructions instructions;

        @JsonProperty(value = "errorMessage", index = 4)
        private String errorMessage = null;

        @Override
        public String toString() {
            return JsonUtil.toJSONString(this);
        }
    }

    @Data
    private static class Instructions {
        @JsonProperty(value = "OperatorType", index = 1)
        private String operatorType;

        @JsonProperty(value = "Count", index = 2)
        private Integer count;

        @JsonProperty(value = "Offset", index = 3)
        private Integer offset;

        @JsonProperty(value = "Variant", index = 4)
        private String variant;

        @JsonProperty(value = "Aggregates", index = 5)
        private String aggregates;

        @JsonProperty(value = "Distinct", index = 6)
        private Boolean distinct;

        @JsonProperty(value = "GroupBy", index = 7)
        private String groupBy;

        @JsonProperty(value = "OrderBy", index = 8)
        private String orderBy;

        @JsonProperty(value = "Keyspace", index = 9)
        private Keyspace keyspace;

        @JsonProperty(value = "TargetTabletType", index = 10)
        private String targetTabletType;

        @JsonProperty(value = "MultiShardAutocommit", index = 11)
        private Boolean multiShardAutocommit;

        @JsonProperty(value = "FieldQuery", index = 12)
        private String fieldQuery;

        @JsonProperty(value = "Query", index = 13)
        private String query;

        @JsonProperty(value = "Table", index = 14)
        private String table;

        @JsonProperty(value = "Values", index = 15)
        private List<Object> valueList;

        @JsonProperty(value = "Vindex", index = 16)
        private String vindex;

        @JsonProperty(value = "JoinColumnIndexes", index = 17)
        private List<Integer> joinColumnIndexes;

        @JsonProperty(value = "TableName", index = 18)
        private String tableName;

        @JsonProperty(value = "Columns", index = 19)
        private List<Integer> columns;

        @JsonProperty(value = "StrColumns", index = 20)
        private List<String> strColumns;

        @JsonProperty(value = "SysTableKeyspaceExpr", index = 21)
        private List<String> sysTableKeyspaceExpr;

        @JsonProperty(value = "Expressions", index = 22)
        private List<String> expressions;

        @JsonProperty(value = "ResultColumns", index = 23)
        private Integer resultColumns;

        @JsonProperty(value = "Inputs", index = 24)
        private List<Instructions> inputs;

        @JsonProperty(value = "TargetDestination", index = 25)
        private String targetDestination;

        @JsonProperty(value = "IsDML", index = 26)
        private boolean isDML;

        @JsonProperty(value = "SingleShardOnly", index = 27)
        private boolean singleShardOnly;

        @JsonProperty(value = "ShardNameNeeded", index = 28)
        private boolean shardNameNeeded;

        @JsonProperty(value = "Predicate", index = 29)
        private String predicate;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Instructions)) {
                return false;
            }
            Instructions that = (Instructions) o;
            return Objects.equal(operatorType, that.operatorType) &&
                Objects.equal(variant, that.variant) &&
                Objects.equal(aggregates, that.aggregates) &&
                Objects.equal(distinct, that.distinct) &&
                Objects.equal(groupBy, that.groupBy) &&
                Objects.equal(orderBy, that.orderBy) &&
                Objects.equal(count, that.count) &&
                Objects.equal(keyspace, that.keyspace) &&
                Objects.equal(targetTabletType, that.targetTabletType) &&
                Objects.equal(multiShardAutocommit, that.multiShardAutocommit) &&
                Objects.equal(fieldQuery, that.fieldQuery) &&
                Objects.equal(query, that.query) &&
                Objects.equal(table, that.table) &&
                Objects.equal(valueList, that.valueList) &&
                Objects.equal(joinColumnIndexes, that.joinColumnIndexes) &&
                Objects.equal(tableName, that.tableName) &&
                Objects.equal(columns, that.columns) &&
                Objects.equal(strColumns, that.strColumns) &&
                Objects.equal(sysTableKeyspaceExpr, that.sysTableKeyspaceExpr) &&
                Objects.equal(expressions, that.expressions) &&
                Objects.equal(inputs, that.inputs) &&
                Objects.equal(targetDestination, that.targetDestination) &&
                Objects.equal(isDML, that.isDML) &&
                Objects.equal(singleShardOnly, that.singleShardOnly) &&
                Objects.equal(shardNameNeeded, that.shardNameNeeded) &&
                Objects.equal(predicate, that.predicate);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(operatorType, variant, aggregates, distinct, groupBy, orderBy, count, keyspace, targetTabletType, multiShardAutocommit, fieldQuery, query, table, valueList,
                joinColumnIndexes, tableName, columns, strColumns, sysTableKeyspaceExpr, expressions, inputs, targetDestination, isDML, singleShardOnly, shardNameNeeded, predicate);
        }

        @Override
        public String toString() {
            return JsonUtil.toJSONString(this);
        }
    }

    @Data
    private static class Destination {
        @JsonProperty(value = "Name", index = 1)
        private String name;

        @JsonProperty(value = "Sharded", index = 2)
        private Boolean sharded;

        @Override
        public String toString() {
            return JsonUtil.toJSONString(this);
        }
    }

    @Data
    private static class Keyspace {
        @JsonProperty(value = "Name", index = 1)
        private String name;

        @JsonProperty(value = "Sharded", index = 2)
        private Boolean sharded;

        @Override
        public String toString() {
            return JsonUtil.toJSONString(this);
        }
    }
}
