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

package com.jd.jdbc.planbuilder.gen4;

import com.jd.jdbc.planbuilder.gen4.operator.OperatorUtil;
import com.jd.jdbc.planbuilder.gen4.operator.logical.Concatenate;
import com.jd.jdbc.planbuilder.gen4.operator.logical.LogicalOperator;
import com.jd.jdbc.planbuilder.gen4.operator.logical.QueryGraph;
import com.jd.jdbc.planbuilder.semantics.Analyzer;
import com.jd.jdbc.planbuilder.semantics.FakeSI;
import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import io.netty.util.internal.StringUtil;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AllArgsConstructor;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import testsuite.TestSuite;
import vschema.Vschema;

public class OperatorTest extends TestSuite {

    @Test
    public void testOperator() throws IOException, InterruptedException {
        String fileName = "src/test/resources/plan/Gen4/operator_test_data.txt";
        List<TestCase> testCases = readTestCase(fileName);
        testOperator(fileName, testCases);
    }

    @Test
    @Ignore
    public void testOne() throws IOException, InterruptedException {
        String fileName = "src/test/resources/plan/Gen4/operator_test_data_one.txt";
        List<TestCase> testCases = readTestCase(fileName);
        testOperator(fileName, testCases);
    }

    private void testOperator(String fileName, List<TestCase> testCases) throws InterruptedException {
        Map<String, Vschema.ColumnVindex> vindexTables = new HashMap<>();
        vindexTables.put("user_index", Vschema.ColumnVindex.newBuilder().setName("hash").build());
        FakeSI si = new FakeSI(new HashMap<>(), vindexTables);
        CountDownLatch latch = new CountDownLatch(testCases.size());
        ExecutorService executorService = getThreadPool(10,10);
        AtomicBoolean errorFlag = new AtomicBoolean(true);
        for (TestCase tc : testCases) {
            executorService.execute(() -> {
                try {
                    SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(tc.query);
                    SemTable semTable = Analyzer.analyze((SQLSelectStatement) stmt, "", si);
                    LogicalOperator optree = OperatorUtil.createLogicalOperatorFromAST(stmt, semTable);
                    String output = testString(optree);
                    if (!tc.expected.equals(output)) {
                        System.out.println(printFail(fileName + "/" + tc.line + "  is [FAIL], \nexecput:" + tc.expected + "\noutput:" + output));
                        errorFlag.set(false);
                    } else {
                        printOk(fileName + "/" + tc.line + "  SQL:" + tc.query + " is [OK]");
                    }
                } catch (Exception e) {
                    errorFlag.set(false);
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        Assert.assertTrue(printFail("testOperator is [FAIL]"), errorFlag.get());
    }

    private String testString(LogicalOperator op) {
        if (op instanceof QueryGraph) {
            return "QueryGraph: " + qgTestString((QueryGraph) op);
        }
        if (op instanceof Concatenate) {
            List<String> inners = new ArrayList<>();
            Concatenate concatenate = (Concatenate) op;
            for (LogicalOperator source : concatenate.getSources()) {
                inners.add(indent(testString(source)));
            }
            if (concatenate.getOrderBy() != null) {
                inners.add(indent(SQLUtils.toMySqlString(concatenate.getOrderBy()).toLowerCase()));
            }
            if (concatenate.getLimit() != null) {
                inners.add(indent(SQLUtils.toMySqlString(concatenate.getLimit()).toLowerCase()));
            }
            String dist = "";
            if (concatenate.isDistinct()) {
                dist = "(distinct)";
            }
            return String.format("Concatenate%s {\n%s\n}", dist, String.join(",\n", inners));
        }
        return null;
    }

    private static String indent(String s) {
        String[] lines = s.split("\\n");
        for (int i = 0; i < lines.length; i++) {
            lines[i] = "\t" + lines[i];
        }
        return String.join("\n", lines);
    }

    private String qgTestString(QueryGraph op) {
        return String.format("{\n" + "Tables:\n" + "%s%s%s\n" + "}", tableNames(op), crossPredicateString(op), noDepsString(op));
    }

    private String tableNames(QueryGraph qg) {
        StringBuilder tables = new StringBuilder();
        for (int i = 0; i < qg.getTables().size(); i++) {
            tables.append(tableName(qg.getTables().get(i)));
            if (i + 1 != qg.getTables().size()) {
                tables.append("\n");
            }
        }
        return tables.toString();
    }

    // the following code is only used by tests
    private String tableName(QueryTable qt) {
        String alias = "";
        if (qt.getAlias().getAlias() != null) {
            alias = "AS" + qt.getAlias().getAlias();
        }
        List<String> preds = new ArrayList<>();
        for (SQLExpr predicate : qt.getPredicates()) {
            preds.add(predicate.toString());
        }
        String where = "";
        if (preds.size() > 0) {
            where = " where " + String.join(" and ", preds);
        }
        return "\t" + qt.getId() + ":" + qt.getTable().getName() + alias + where;
    }

    private String crossPredicateString(QueryGraph op) {
        if (op.getInnerJoins().size() == 0) {
            return "";
        }
        //todo join
        return null;

    }

    private String noDepsString(QueryGraph op) {
        if (op.getNoDeps() == null) {
            return "";
        }

        return String.format("\nForAll: %s", op.getNoDeps().toString().replaceAll("\n", " ").replaceAll("AND", "and"));

    }

    private List<TestCase> readTestCase(String filename) throws IOException {
        List<TestCase> testCaseList = new ArrayList<>();
        BufferedReader br = Files.newBufferedReader(Paths.get(filename), StandardCharsets.UTF_8);
        String line;
        int lineno = 0;
        int tmpLine;
        while ((line = br.readLine()) != null) {
            lineno++;
            tmpLine = lineno;
            if (StringUtil.isNullOrEmpty(line) || line.trim().isEmpty()) {
                continue;
            }
            String query = line;
            while (query.startsWith("#")) {
                query = br.readLine();
                lineno++;
            }
            StringBuilder expected = new StringBuilder();
            while ((line = br.readLine()) != null) {
                lineno++;
                expected.append(line);
                if (Objects.equals(line, "}")) {
                    break;
                }
                expected.append("\n");
            }

            testCaseList.add(new TestCase(tmpLine, query, expected.toString()));
        }

        return testCaseList;
    }

    @AllArgsConstructor
    private static class TestCase {
        private int line;

        private String query;

        private String expected;
    }

}
