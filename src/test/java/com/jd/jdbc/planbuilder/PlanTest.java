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
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.key.DestinationShard;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.util.JsonUtil;
import com.jd.jdbc.vindexes.VKeyspace;
import io.netty.util.internal.StringUtil;
import io.vitess.proto.Query;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.Test;

import static io.netty.util.internal.StringUtil.NEWLINE;
import static org.junit.Assert.assertEquals;

public class PlanTest extends AbstractPlanTest {

    @Test
    public void testOne() throws IOException {
        VSchemaManager vm = loadSchema("src/test/resources/plan/plan_schema.json");
        testFile("src/test/resources/plan/one_cases.txt", vm, 0);
    }

    @Test
    public void testPlan() throws IOException {
        VSchemaManager vm = loadSchema("src/test/resources/plan/plan_schema.json");
        testFile("src/test/resources/plan/aggr_cases.txt", vm, 0);
        testFile("src/test/resources/plan/dml_insert_cases.txt", vm, 0);
        testFile("src/test/resources/plan/filter_cases.txt", vm, 0);
        testFile("src/test/resources/plan/from_cases.txt", vm, 0);
        testFile("src/test/resources/plan/memory_sort_cases.txt", vm, 0);
        testFile("src/test/resources/plan/postprocess_cases.txt", vm, 0);
        testFile("src/test/resources/plan/select_cases.txt", vm, 0);
        testFile("src/test/resources/plan/union_cases.txt", vm, 0);
        testFile("src/test/resources/plan/dml_delete_cases.txt", vm, 0);
        testFile("src/test/resources/plan/dml_update_cases.txt", vm, 0);
    }

    @Test
    public void testDestination() throws IOException {
        VSchemaManager vm = loadSchema("src/test/resources/plan/plan_schema.json");
        testFile("src/test/resources/plan/destination_case.txt", vm, 0);
    }

    private void testFile(String filename, VSchemaManager vm, Integer startPos) throws IOException {
        List<TestCase> testCaseList = iterateExecFile(filename);
        for (TestCase testCase : testCaseList) {

            if (testCase.lineno <= startPos) {
                continue;
            }

            printComment("Test Case: " + testCase.comments);
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
                Plan plan = build(testCase.input, vm);
                TestPlan fromCode = this.format(plan, testCase.input);
                printInfo("From Code: " + fromCode);

                assertEquals(printFail("File: " + filename + ", Line: " + testCase.lineno + " is [FAIL]"), fromFile, fromCode);
                printOk("File: " + filename + ", Line: " + testCase.lineno + " is [OK]");
            } catch (Exception e) {
                String error = e.getMessage().replaceAll("\"", "");
                printInfo("From Code: " + error);
                assertEquals(printFail("File: " + filename + ", Line: " + testCase.lineno + " is [FAIL]"), fromFile.errorMessage.toLowerCase(), error.toLowerCase());
                printOk("File: " + filename + ", Line: " + testCase.lineno + " is [OK]");
            }
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
            if (StringUtil.isNullOrEmpty(line) || NEWLINE.equalsIgnoreCase(line)) {
                continue;
            }
            if (line.startsWith("#")) {
                comments.append(line);
                continue;
            }
            line = line.substring(1).substring(0, line.length() - 2);
            String l;
            StringBuilder output = new StringBuilder();
            while ((l = br.readLine()) != null) {
                lineno++;
                output.append(l);
                if (l.startsWith("}") || l.startsWith("\"")) {
                    break;
                }
            }
            testCaseList.add(new TestCase(filename, lineno, line, output.toString(), comments.toString()));
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
        } else if (primitive instanceof DeleteEngine) {
            instructions = this.formatDeleteEngine((DeleteEngine) primitive);
        } else if (primitive instanceof UpdateEngine) {
            instructions = this.formatUpdateEngine((UpdateEngine) primitive);
        } else if (primitive instanceof SendEngine) {
            instructions = this.formatSendEngine((SendEngine) primitive);
        }

        TestPlan testPlan = new TestPlan();
        testPlan.setQueryType(plan.getStatementType().name().replaceAll("Stmt", "").toUpperCase());
        testPlan.setOriginal(query);
        testPlan.setInstructions(instructions);
        return testPlan;
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
                    Query.Type type = ((EvalEngine.Literal) expr).getVal().getType();
                    exprList.add(type.name() + "(\"" + expr.string() + "\")");
                }
            }
            instructions.setSysTableKeyspaceExpr(exprList);
        }
        return instructions;
    }

    private Instructions formatOrderedAggregateEngine(OrderedAggregateEngine engine) {
        Instructions instructions = new Instructions();
        instructions.setOperatorType("Aggregate");
        instructions.setVariant("Ordered");

        List<OrderedAggregateEngine.AggregateParams> aggregateParamsList = engine.getAggregateParamsList();
        if (aggregateParamsList != null && !aggregateParamsList.isEmpty()) {
            OrderedAggregateEngine.AggregateParams aggregateParams = aggregateParamsList.get(0);
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
            }
        }

        instructions.setDistinct(engine.getHasDistinct());

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
        return instructions;
    }

    private String formatEvalResult(EvalEngine.EvalResult evalResult) {
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

    @Data
    @AllArgsConstructor
    private static class TestCase {
        private String file;

        private Integer lineno;

        private String input;

        private String output;

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

        private String errorMessage;

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

        @JsonProperty(value = "Inputs", index = 23)
        private List<Instructions> inputs;

        @JsonProperty(value = "TargetDestination", index = 24)
        private String targetDestination;

        @JsonProperty(value = "IsDML", index = 25)
        private boolean isDML;

        @JsonProperty(value = "SingleShardOnly", index = 26)
        private boolean singleShardOnly;

        @JsonProperty(value = "ShardNameNeeded", index = 27)
        private boolean shardNameNeeded;


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
                Objects.equal(shardNameNeeded, that.shardNameNeeded);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(operatorType, variant, aggregates, distinct, groupBy, orderBy, count, keyspace, targetTabletType, multiShardAutocommit, fieldQuery, query, table, joinColumnIndexes,
                tableName, columns, strColumns, sysTableKeyspaceExpr, expressions, inputs, targetDestination, isDML, singleShardOnly, shardNameNeeded);
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
