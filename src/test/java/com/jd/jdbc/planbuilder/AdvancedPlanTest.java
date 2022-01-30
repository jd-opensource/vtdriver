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

import com.jd.jdbc.VSchemaManager;
import com.jd.jdbc.engine.Plan;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLJoinTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import org.junit.Test;

public class AdvancedPlanTest extends AbstractPlanTest {

    @Test
    public void case01() {
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement("select m1.col from unsharded as m1 join unsharded as m2");
        System.out.println("stmt = " + stmt);

        stmt = SQLUtils.parseSingleMysqlStatement("select t1.col from t1, t2");
        System.out.println("stmt = " + stmt);

        stmt = SQLUtils.parseSingleMysqlStatement("select t1.col from (select t2.id from t2)");
        System.out.println("stmt = " + stmt);

        stmt = SQLUtils.parseSingleMysqlStatement("select t1.col from t1 where t1.id = (select t2.id from t2)");
        System.out.println("stmt = " + stmt);

        stmt = SQLUtils.parseSingleMysqlStatement("select t1.col from t1 union select t2.col from t2");
        System.out.println("stmt = " + stmt);

        stmt = SQLUtils.parseSingleMysqlStatement("select * from t1 where id = ?");
        System.out.println("stmt = " + stmt);

        stmt = SQLUtils.parseSingleMysqlStatement("select * from t1 left join t2 on t1.id = t2.id");
        System.out.println("stmt = " + stmt);

        stmt = SQLUtils.parseSingleMysqlStatement("select * from t1 left join (select * from t2) t3 on t1.id = t3.id");
        System.out.println("stmt = " + stmt);

        stmt = SQLUtils.parseSingleMysqlStatement("select * from t1, t2, t3 join t4, t5");
        System.out.println("stmt = " + stmt);

        SQLSelectStatement selectStatement = (SQLSelectStatement) stmt;
        SQLTableSource tableSource = selectStatement.getSelect().getQueryBlock().getFrom();
        SQLJoinTableSource joinTableSource = generate((SQLJoinTableSource) tableSource);
        System.out.println("joinTableSource = " + joinTableSource);

        stmt = SQLUtils.parseSingleMysqlStatement("SELECT m1.col FROM unsharded as m1 JOIN unsharded as m2 RIGHT JOIN unsharded as m3 ON m1.a = m2.b");
        System.out.println("stmt = " + stmt);
    }

    private static SQLJoinTableSource generate(SQLJoinTableSource joinTableSource) {
        SQLTableSource left = joinTableSource.getLeft();
        SQLTableSource right = joinTableSource.getRight();
        SQLJoinTableSource.JoinType joinType = joinTableSource.getJoinType();
        SQLExpr condition = joinTableSource.getCondition();
        if (left instanceof SQLJoinTableSource) {
            left = generate((SQLJoinTableSource) left);
        }
        SQLExpr cloneCondition = null;
        if (condition != null) {
            cloneCondition = condition.clone();
        }
        return new SQLJoinTableSource(left.clone(), joinType, right.clone(), cloneCondition);
    }

    @Test
    public void case02() throws Exception {
        VSchemaManager vm = loadSchema("src/test/resources/plan/plan_schema.json");
        Plan plan = build("select * from unsharded_a left join unsharded_b on unsharded_a.id = unsharded_b.id", vm);
        System.out.println("plan = " + plan);
    }
}
