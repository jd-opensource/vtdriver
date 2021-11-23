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

import com.jd.jdbc.planbuilder.Builder;
import com.jd.jdbc.planbuilder.PrimitiveBuilder;
import com.jd.jdbc.planbuilder.Symtab;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.expr.SQLExistsExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLInSubQueryExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelect;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQuery;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.utils.JdbcConstants;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.Getter;


public class VtFindOriginVisitor extends MySqlASTVisitorAdapter {
    private final PrimitiveBuilder pb;

    @Getter
    private Builder highestOrigin;

    @Getter
    private final List<SubqueryInfo> subqueryInfoList;

    @Getter
    private final List<SQLException> exceptionList;

    @Getter
    private final Map<SQLSelect, SQLExpr> constructsMap;

    public VtFindOriginVisitor(final PrimitiveBuilder pb, final Builder highestOrigin, final List<SubqueryInfo> subqueryInfoList) {
        this.pb = pb;
        this.highestOrigin = highestOrigin;
        this.subqueryInfoList = subqueryInfoList;
        this.exceptionList = new ArrayList<>();
        this.constructsMap = new HashMap<>(16, 1);
    }

    @Override
    public boolean visit(final SQLIdentifierExpr expr) {
        processColName(expr);
        return false;
    }

    @Override
    public boolean visit(final SQLPropertyExpr expr) {
        processColName(expr);
        return false;
    }

    @Override
    public boolean visit(final SQLInSubQueryExpr x) {
        this.constructsMap.put(x.getSubQuery(), x);
        return true;
    }

    @Override
    public boolean visit(final SQLExistsExpr x) {
        this.constructsMap.put(x.getSubQuery(), x);
        return true;
    }

    @Override
    public boolean visit(final SQLSelect x) {
        PrimitiveBuilder spb = new PrimitiveBuilder(this.pb.getVm(), this.pb.getDefaultKeyspace(), this.pb.getJointab());
        SQLSelectQuery selectQuery = x.getQuery();
        if (selectQuery instanceof MySqlSelectQueryBlock) {
            try {
                spb.processSelect(new SQLSelectStatement(x, JdbcConstants.MYSQL), pb.getSymtab());
            } catch (SQLException e) {
                exceptionList.add(e);
                return false;
            }
        } else if (selectQuery instanceof SQLUnionQuery) {
            try {
                spb.processUnion((SQLUnionQuery) selectQuery, pb.getSymtab());
            } catch (SQLException e) {
                exceptionList.add(e);
                return false;
            }
        } else {
            exceptionList.add(new SQLException("BUG: unexpected SELECT type: " + SQLUtils.toMySqlString(x)));
            return false;
        }

        SubqueryInfo sqi = new SubqueryInfo(x, spb.getBuilder());
        for (SQLName extern : spb.getSymtab().getExterns()) {
            // No error expected. These are resolved externs.
            try {
                Symtab.FindResponse findResponse = pb.getSymtab().find(extern);
                Builder newOrigin = findResponse.getOrigin();
                Boolean isLocal = findResponse.getIsLocal();
                if (!isLocal) {
                    continue;
                }
                if (this.highestOrigin.order() < newOrigin.order()) {
                    this.highestOrigin = newOrigin;
                }
                if (sqi.origin == null) {
                    sqi.origin = newOrigin;
                } else if (sqi.origin.order() < newOrigin.order()) {
                    sqi.origin = newOrigin;
                }
            } catch (SQLException e) {
                exceptionList.add(e);
                return false;
            }
        }
        this.subqueryInfoList.add(sqi);
        return false;
    }

    private void processColName(final SQLName expr) {
        try {
            Symtab.FindResponse findResponse = this.pb.getSymtab().find(expr);
            Boolean isLocal = findResponse.getIsLocal();
            Builder newOrigin = findResponse.getOrigin();
            if (isLocal && newOrigin.order() > this.highestOrigin.order()) {
                this.highestOrigin = newOrigin;
            }
        } catch (SQLException e) {
            exceptionList.add(new SQLException(e.getMessage(), e.getCause()));
        }
    }

    @Data
    public static class SubqueryInfo {
        private final SQLSelect ast;

        private final Builder bldr;

        private Builder origin;

        public SubqueryInfo(final SQLSelect ast, final Builder bldr) {
            this.ast = ast;
            this.bldr = bldr;
        }
    }
}
