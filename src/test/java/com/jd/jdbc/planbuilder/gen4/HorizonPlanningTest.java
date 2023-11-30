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

import com.jd.BaseTest;
import com.jd.jdbc.planbuilder.semantics.SemTable;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.ToString;
import org.junit.Assert;
import org.junit.Test;

public class HorizonPlanningTest extends BaseTest {

    @Test
    public void testCheckIfAlreadyExists() {
        List<CheckIfAlreadyExistsTest> tests = Arrays.asList(
            new CheckIfAlreadyExistsTest("No alias, both ColName", new SQLSelectItem(new SQLIdentifierExpr("id")), getSQLSelectQuery("select id"), 0),
            new CheckIfAlreadyExistsTest("Aliased expression and ColName", new SQLSelectItem(new SQLIdentifierExpr("user_id")), getSQLSelectQuery("select user_id,id"), 0),
            new CheckIfAlreadyExistsTest("Non-ColName expressions", new SQLSelectItem(new SQLIdentifierExpr("test")), getSQLSelectQuery("select test"), 0),
            new CheckIfAlreadyExistsTest("No alias, multiple ColName in projection", new SQLSelectItem(new SQLIdentifierExpr("id")), getSQLSelectQuery("select foo,id"), 1),
            new CheckIfAlreadyExistsTest("No matching entry", new SQLSelectItem(new SQLIdentifierExpr("id")), getSQLSelectQuery("select foo,name"), -1),
            new CheckIfAlreadyExistsTest("No AliasedExpr in projection", new SQLSelectItem(new SQLIdentifierExpr("id")), getSQLSelectQuery("select user,people"), -1)
        );
        SemTable semTable = new SemTable();
        for (CheckIfAlreadyExistsTest tt : tests) {
            int got = HorizonPlanning.checkIfAlreadyExists(tt.getExpr(), tt.getSel(), semTable);
            Assert.assertEquals(tt.getWant(), got);
            printOk("testCheckIfAlreadyExists [OK] , case= " + tt);
        }
    }

    private SQLSelectQuery getSQLSelectQuery(String sql) {
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        if (stmt instanceof SQLSelectStatement) {
            return ((SQLSelectStatement) stmt).getSelect().getQuery();
        }
        return null;
    }

    @ToString
    private static class CheckIfAlreadyExistsTest {
        private final String name;

        @Getter
        private final SQLSelectItem expr;

        @Getter
        private final SQLSelectQuery sel;

        @Getter
        private final int want;

        CheckIfAlreadyExistsTest(String name, SQLSelectItem expr, SQLSelectQuery sel, int want) {
            this.name = name;
            this.expr = expr;
            this.sel = sel;
            this.want = want;
        }
    }
}
