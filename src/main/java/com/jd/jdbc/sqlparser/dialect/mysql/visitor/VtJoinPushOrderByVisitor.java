/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jd.jdbc.sqlparser.dialect.mysql.visitor;

import com.jd.jdbc.planbuilder.JoinPlan;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLQueryExpr;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;


public class VtJoinPushOrderByVisitor extends MySqlASTVisitorAdapter {
    private final JoinPlan jb;

    @Getter
    private final List<SQLException> exceptionList;

    public VtJoinPushOrderByVisitor(final JoinPlan jb) {
        this.jb = jb;
        this.exceptionList = new ArrayList<>();
    }

    @Override
    public boolean visit(final SQLIdentifierExpr x) {
        if (x.getMetadata().origin().order() > this.jb.getLeft().order()) {
            exceptionList.add(new SQLException("unsupported: order by spans across shards"));
        }
        return false;
    }

    @Override
    public boolean visit(final SQLPropertyExpr x) {
        if (x.getMetadata().origin().order() > this.jb.getLeft().order()) {
            exceptionList.add(new SQLException("unsupported: order by spans across shards"));
        }
        return false;
    }

    @Override
    public boolean visit(final SQLQueryExpr x) {
        exceptionList.add(new SQLException("unsupported: order by has subquery"));
        return false;
    }
}
