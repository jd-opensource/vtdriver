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

import com.google.common.collect.Lists;
import com.jd.BaseTest;
import com.jd.jdbc.planbuilder.semantics.Analyzer;
import com.jd.jdbc.planbuilder.semantics.FakeSI;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.expr.SQLCharExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntegerExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLMethodInvokeExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectOrderByItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import lombok.ToString;
import org.junit.Assert;
import org.junit.Test;

public class QueryProjectionTest extends BaseTest {

    @Test
    public void testQP() {
        SQLMethodInvokeExpr concatMethodExpr = new SQLMethodInvokeExpr("CONCAT");
        concatMethodExpr.addParameter(new SQLIdentifierExpr("last_name"));
        concatMethodExpr.addParameter(new SQLCharExpr(", "));
        concatMethodExpr.addParameter(new SQLIdentifierExpr("first_name"));

        QP[] tcases = new QP[] {
            new QP("select * from user"),
            new QP("select 1, count(1) from user"),
            new QP("select max(id) from user"),
            new QP("select 1, count(1) from user order by 1",
                Lists.newArrayList(new QueryProjection.OrderBy(new SQLSelectOrderByItem(new SQLIntegerExpr(1)), new SQLIntegerExpr(1)))),
            new QP("select id from user order by col, id, 1",
                Lists.newArrayList(new QueryProjection.OrderBy(new SQLSelectOrderByItem(new SQLIdentifierExpr("col")), new SQLIdentifierExpr("col")),
                    new QueryProjection.OrderBy(new SQLSelectOrderByItem(new SQLIdentifierExpr("id")), new SQLIdentifierExpr("id")),
                    new QueryProjection.OrderBy(new SQLSelectOrderByItem(new SQLIdentifierExpr("id")), new SQLIdentifierExpr("id")))
            ),
            new QP("SELECT CONCAT(last_name,', ',first_name) AS full_name FROM mytable ORDER BY full_name",
                Lists.newArrayList(new QueryProjection.OrderBy(new SQLSelectOrderByItem(new SQLIdentifierExpr("full_name")), concatMethodExpr))),
            new QP("select count(*) b from user group by b", "Can't group on 'COUNT(*)'")
        };

        for (QP tcase : tcases) {
            String sql = tcase.sql;
            String expErr = tcase.expErr;
            List<QueryProjection.OrderBy> expOrder = tcase.expOrder;
            try {
                SQLStatement parse = SQLUtils.parseSingleMysqlStatement(sql);
                Analyzer.analyze((SQLSelectStatement) parse, "", new FakeSI(new HashMap<>(), null));
                QueryProjection qp = new QueryProjection();
                MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) ((SQLSelectStatement) parse).getSelect().getQuery();
                qp.createQPFromSelect(query);

                if (expErr != null) {
                    Assert.fail();
                } else {
                    Assert.assertEquals(query.getSelectList().size(), qp.getSelectExprs().size());
                    Assert.assertEquals(expOrder.size(), qp.getOrderExprs().size());
                    for (int i = 0; i < expOrder.size(); i++) {
                        Assert.assertEquals(expOrder.get(i).getInner(), qp.getOrderExprs().get(i).getInner());
                        Assert.assertEquals(expOrder.get(i).getWeightStrExpr(), qp.getOrderExprs().get(i).getWeightStrExpr());
                    }
                    printOk("testQP [OK] , case= " + tcase);
                }
            } catch (Exception e) {
                Assert.assertEquals(expErr, e.getMessage());
            }
        }
    }

    @ToString
    class QP {
        String sql;

        String expErr;

        List<QueryProjection.OrderBy> expOrder;

        public QP(String sql) {
            this.sql = sql;
            this.expOrder = new ArrayList<>();
        }

        public QP(String sql, List<QueryProjection.OrderBy> expOrder) {
            this.sql = sql;
            this.expOrder = expOrder;
        }

        public QP(String sql, String expErr) {
            this.sql = sql;
            this.expErr = expErr;
        }
    }
}



