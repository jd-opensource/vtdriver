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

package com.jd.jdbc.planbuilder.gen4.operator.physical;

import com.google.common.collect.Lists;
import com.jd.BaseTest;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import org.junit.Assert;
import org.junit.Test;

public class RoutePlanningTest extends BaseTest {

    @Test
    public void tryRewriteOrToIn() {
        @AllArgsConstructor
        class TestCase {
            SQLExpr orExpr;

            List<SQLExpr> targetExprs;

            @Override
            public String toString() {
                return "orExpr:" + orExpr;
            }
        }
        List<TestCase> testCaseList = new ArrayList<>();
        testCaseList.add(new TestCase(SQLUtils.toMySqlExpr("a = 1 or a = 2"), Lists.newArrayList(SQLUtils.toMySqlExpr("a in (1,2)"))));
        testCaseList.add(new TestCase(SQLUtils.toMySqlExpr("a = 1 or a = 2 or a = 3"), Lists.newArrayList(SQLUtils.toMySqlExpr("a in (1,2,3)"))));
        testCaseList.add(new TestCase(SQLUtils.toMySqlExpr("1 = a or a = 2 or a = 3"), Lists.newArrayList(SQLUtils.toMySqlExpr("a in (1,2,3)"))));
        testCaseList.add(new TestCase(SQLUtils.toMySqlExpr("1 = a or 2 = a or a = 3"), Lists.newArrayList(SQLUtils.toMySqlExpr("a in (1,2,3)"))));
        testCaseList.add(new TestCase(SQLUtils.toMySqlExpr("a = 1 or a in (2,3)"), Lists.newArrayList(SQLUtils.toMySqlExpr("a in (1,2,3)"))));
        testCaseList.add(new TestCase(SQLUtils.toMySqlExpr("1 = a or a in (2,3)"), Lists.newArrayList(SQLUtils.toMySqlExpr("a in (1,2,3)"))));
        testCaseList.add(new TestCase(SQLUtils.toMySqlExpr("a in (1,4) or a in (2,3)"), Lists.newArrayList(SQLUtils.toMySqlExpr("a in (1,4,2,3)"))));
        testCaseList.add(new TestCase(SQLUtils.toMySqlExpr("b = 0 or a = 1 or a = 2"), Lists.newArrayList(SQLUtils.toMySqlExpr("a in (1,2)"))));
        testCaseList.add(new TestCase(SQLUtils.toMySqlExpr("b = 0 or a = 1 or a = 2 or a = 3"), Lists.newArrayList(SQLUtils.toMySqlExpr("a in (1,2,3)"))));
        testCaseList.add(new TestCase(SQLUtils.toMySqlExpr("b = 0 or 1 = a or a = 2 or a = 3"), Lists.newArrayList(SQLUtils.toMySqlExpr("a in (1,2,3)"))));
        testCaseList.add(new TestCase(SQLUtils.toMySqlExpr("b = 0 or 1 = a or 2 = a or a = 3"), Lists.newArrayList(SQLUtils.toMySqlExpr("a in (1,2,3)"))));
        testCaseList.add(new TestCase(SQLUtils.toMySqlExpr("b = 0 or a = 1 or a in (2,3)"), Lists.newArrayList(SQLUtils.toMySqlExpr("a in (1,2,3)"))));
        testCaseList.add(new TestCase(SQLUtils.toMySqlExpr("b = 0 or 1 = a or a in (2,3)"), Lists.newArrayList(SQLUtils.toMySqlExpr("a in (1,2,3)"))));
        testCaseList.add(new TestCase(SQLUtils.toMySqlExpr("b = 0 or a in (1,4) or a in (2,3)"), Lists.newArrayList(SQLUtils.toMySqlExpr("a in (1,4,2,3)"))));
        testCaseList.add(new TestCase(SQLUtils.toMySqlExpr("c = 100 or c = 99 or a = 1 or a = 2"), Lists.newArrayList(SQLUtils.toMySqlExpr("a in (1,2)"), SQLUtils.toMySqlExpr("c in (100,99)"))));
        testCaseList.add(new TestCase(SQLUtils.toMySqlExpr("c = 100 or 99 = c or a = 1 or a = 2 or a = 3"), Lists.newArrayList(SQLUtils.toMySqlExpr("a in (1,2,3)"), SQLUtils.toMySqlExpr("c in (100,99)"))));
        testCaseList.add(new TestCase(SQLUtils.toMySqlExpr("c = '100' or c = '99' or 1 = a or a = 2 or a = 3"), Lists.newArrayList(SQLUtils.toMySqlExpr("a in (1,2,3)"), SQLUtils.toMySqlExpr("c in ('100','99')"))));
        testCaseList.add(new TestCase(SQLUtils.toMySqlExpr("c in (100,99) or 1 = a or 2 = a or a = 3"), Lists.newArrayList(SQLUtils.toMySqlExpr("a in (1,2,3)"))));

        for (TestCase testCase : testCaseList) {
            List<SQLExpr> sqlExprList = RoutePlanning.tryRewriteOrToIn(testCase.orExpr);
            Assert.assertEquals(testCase.targetExprs, sqlExprList);
            printOk(testCase + "\n");
        }
    }
}