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

import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.JoinEngine;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.expr.SQLLiteralExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLMethodInvokeExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNullExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectOrderByItem;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtJoinPushOrderByVisitor;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;

public class JoinPlan implements Builder {
    private final List<ResultColumn> resultColumnList;

    private final Map<ResultColumn, Integer> weightStrings;

    private final JoinEngine joinEngine;

    private Integer order;

    /**
     * leftOrder stores the order number of the left node. This is
     * used for a b-tree style traversal towards the target route.
     * Let us assume the following execution tree:
     * J9
     * /  \
     * /    \
     * J3     J8
     * / \    /  \
     * R1  R2  J6  R7
     * / \
     * R4 R5
     * In the above trees, the suffix numbers indicate the
     * execution order. The leftOrder for the joins will then
     * be as follows:
     * J3: 1
     * J6: 4
     * J8: 6
     * J9: 3
     * The route to R4 would be:
     * Go right from J9->J8 because Left(J9)==3, which is <4.
     * Go left from J8->J6 because Left(J8)==6, which is >=4.
     * Go left from J6->R4 because Left(J6)==4, the destination.
     * Look for 'isOnLeft' to see how these numbers are used.
     */
    private Integer leftOrder;

    /**
     * Left and Right are the nodes for the join.
     */
    @Getter
    private Builder left;

    private Builder right;

    public JoinPlan(Map<ResultColumn, Integer> weightStrings, Builder left, Builder right, JoinEngine joinEngine) {
        this.weightStrings = weightStrings;
        this.left = left;
        this.right = right;
        this.joinEngine = joinEngine;
        this.resultColumnList = new ArrayList<>();
    }

    @Override
    public Integer order() {
        return this.order;
    }

    @Override
    public List<ResultColumn> resultColumns() {
        return this.resultColumnList;
    }

    @Override
    public void reorder(Integer order) {
        this.left.reorder(order);
        this.leftOrder = this.left.order();
        this.right.reorder(this.leftOrder);
        this.order = this.right.order() + 1;
    }

    @Override
    public Builder first() throws SQLException {
        return this.left.first();
    }

    @Override
    public void pushFilter(PrimitiveBuilder pb, SQLExpr filter, String whereType, Builder origin) throws SQLException {
        if (this.isOnLeft(origin.order())) {
            this.left.pushFilter(pb, filter, whereType, origin);
            return;
        }
        if (Engine.JoinOpcode.LeftJoin.equals(this.joinEngine.getOpcode())) {
            throw new SQLException("unsupported: cross-shard left join and where clause");
        }
        this.right.pushFilter(pb, filter, whereType, origin);
    }

    @Override
    public PushSelectResponse pushSelect(PrimitiveBuilder pb, SQLSelectItem expr, Builder origin) throws SQLException {
        ResultColumn rc;
        if (this.isOnLeft(origin.order())) {
            PushSelectResponse pushSelectResponse = this.left.pushSelect(pb, expr, origin);
            rc = pushSelectResponse.getResultColumn();
            Integer colNumber = pushSelectResponse.getColNumber();
            this.joinEngine.getCols().add(-colNumber - 1);
        } else {
            // Pushing of non-trivial expressions not allowed for RHS of left joins.
            if (!(expr.getExpr() instanceof SQLName) && Engine.JoinOpcode.LeftJoin.equals(this.joinEngine.getOpcode())) {
                throw new SQLException("unsupported: cross-shard left join and column expressions");
            }

            PushSelectResponse pushSelectResponse = this.right.pushSelect(pb, expr, origin);
            rc = pushSelectResponse.getResultColumn();
            Integer colNumber = pushSelectResponse.getColNumber();
            this.joinEngine.getCols().add(colNumber + 1);
        }
        this.resultColumnList.add(rc);
        return new PushSelectResponse(rc, this.resultColumnList.size() - 1);
    }

    @Override
    public void makeDistinct() throws SQLException {
        throw new SQLException("unsupported: distinct on cross-shard join");
    }

    @Override
    public void pushGroupBy(SQLSelectGroupByClause groupBy) throws SQLException {
        if (groupBy == null || groupBy.getItems() == null || groupBy.getItems().isEmpty()) {
            return;
        }
        throw new SQLFeatureNotSupportedException("unupported: group by on cross-shard join");
    }

    @Override
    public Builder pushOrderBy(SQLOrderBy orderBy) throws SQLException {
        boolean isSpecial = false;
        if (orderBy == null) {
            isSpecial = true;
        } else {
            List<SQLSelectOrderByItem> orderByList = orderBy.getItems();
            switch (orderByList.size()) {
                case 0:
                    isSpecial = true;
                    break;
                case 1:
                    SQLExpr expr = orderByList.get(0).getExpr();
                    if (expr instanceof SQLNullExpr) {
                        isSpecial = true;
                    } else if (expr instanceof SQLMethodInvokeExpr) {
                        String methodName = ((SQLMethodInvokeExpr) expr).getMethodName();
                        if ("rand".equalsIgnoreCase(methodName)) {
                            isSpecial = true;
                        }
                    }
                    break;
                default:
                    break;
            }
        }

        if (isSpecial) {
            this.left = this.left.pushOrderBy(orderBy);
            this.right = this.right.pushOrderBy(orderBy);
            return this;
        }

        List<SQLSelectOrderByItem> orderByList = orderBy.getItems();
        for (SQLSelectOrderByItem orderByItem : orderByList) {
            SQLExpr orderByExpr = orderByItem.getExpr();
            if (orderByExpr instanceof SQLLiteralExpr) {
                // This block handles constructs that use ordinals for 'ORDER BY'. For example:
                // SELECT a, b, c FROM t1, t2 ORDER BY 1, 2, 3.
                Integer num = PlanBuilder.resultFromNumber(this.resultColumnList, (SQLLiteralExpr) orderByExpr);
                if (this.resultColumns().get(num).getColumn().origin().order() > this.left.order()) {
                    return MemorySortPlan.newMemorySortPlan(this, orderBy);
                }
            } else {
                // Analyze column references within the expression to make sure they all
                // go to the left.
                VtJoinPushOrderByVisitor visitor = new VtJoinPushOrderByVisitor(this);
                orderByExpr.accept(visitor);
                List<SQLException> exceptionList = visitor.getExceptionList();
                if (exceptionList != null && !exceptionList.isEmpty()) {
                    return MemorySortPlan.newMemorySortPlan(this, orderBy);
                }
            }
        }

        // There were no errors. We can push the order by to the left-most route.
        this.left = this.left.pushOrderBy(orderBy);
        this.right = this.right.pushOrderBy(null);
        return this;
    }

    @Override
    public void setUpperLimit(SQLExpr count) throws SQLException {

    }

    @Override
    public void pushMisc(MySqlSelectQueryBlock sel) {
        this.left.pushMisc(sel);
        this.right.pushMisc(sel);
    }

    @Override
    public void wireup(Builder bldr, Jointab jt) throws SQLException {
        this.right.wireup(bldr, jt);
        this.left.wireup(bldr, jt);
    }

    @Override
    public void supplyVar(Integer from, Integer to, SQLName colName, String varName) throws SQLException {
        if (!this.isOnLeft(from)) {
            this.right.supplyVar(from, to, colName, varName);
            return;
        }
        if (this.isOnLeft(to)) {
            this.left.supplyVar(from, to, colName, varName);
            return;
        }
        if (this.joinEngine.getVars().containsKey(varName)) {
            // Looks like somebody else already requested this.
            return;
        }
        Column c = colName.getMetadata();
        for (int i = 0; i < this.resultColumnList.size(); i++) {
            ResultColumn rc = this.resultColumnList.get(i);
            if (this.joinEngine.getCols().get(i) > 0) {
                continue;
            }
            if (rc.getColumn().equals(c)) {
                this.joinEngine.getVars().put(varName, -this.joinEngine.getCols().get(i) - 1);
                return;
            }
        }
        this.joinEngine.getVars().put(varName, this.left.supplyCol(colName).getColNumber());
    }

    @Override
    public SupplyColResponse supplyCol(SQLName col) throws SQLException {
        Column c = col.getMetadata();
        for (int i = 0; i < this.resultColumnList.size(); i++) {
            ResultColumn rc = this.resultColumnList.get(i);
            if (rc.getColumn().equals(c)) {
                return new SupplyColResponse(rc, i);
            }
        }

        Integer routeNumber = c.origin().order();
        ResultColumn rc;
        if (this.isOnLeft(routeNumber)) {
            SupplyColResponse response = this.left.supplyCol(col);
            rc = response.getRc();
            Integer sourceCol = response.getColNumber();
            this.joinEngine.getCols().add(-sourceCol - 1);
        } else {
            SupplyColResponse response = this.right.supplyCol(col);
            rc = response.getRc();
            Integer sourceCol = response.getColNumber();
            this.joinEngine.getCols().add(sourceCol + 1);
        }
        this.resultColumnList.add(rc);
        return new SupplyColResponse(rc, this.joinEngine.getCols().size() - 1);
    }

    @Override
    public Integer supplyWeightString(Integer colNumber) throws SQLException {
        ResultColumn rc = this.resultColumnList.get(colNumber);
        if (this.weightStrings.containsKey(rc)) {
            return this.weightStrings.get(rc);
        }
        Integer routeNumber = rc.getColumn().origin().order();
        if (this.isOnLeft(routeNumber)) {
            Integer sourceCol = this.left.supplyWeightString(-this.joinEngine.getCols().get(colNumber) - 1);
            this.joinEngine.getCols().add(-sourceCol - 1);
        } else {
            Integer sourceCol = this.right.supplyWeightString(this.joinEngine.getCols().get(colNumber) - 1);
            this.joinEngine.getCols().add(sourceCol + 1);
        }
        this.resultColumnList.add(rc);
        this.weightStrings.put(rc, this.joinEngine.getCols().size() - 1);
        return this.joinEngine.getCols().size() - 1;
    }

    @Override
    public void pushLock(String lock) throws SQLException {
        this.left.pushLock(lock);
        this.right.pushLock(lock);
    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        this.joinEngine.setLeft(this.left.getPrimitiveEngine());
        this.joinEngine.setRight(this.right.getPrimitiveEngine());
        return this.joinEngine;
    }

    @Override
    public boolean isSplitTablePlan() {
        return left.isSplitTablePlan() || right.isSplitTablePlan();
    }

    /**
     * isOnLeft returns true if the specified route number
     * is on the left side of the join. If false, it means
     * the node is on the right.
     *
     * @param nodeNum
     * @return
     */
    private Boolean isOnLeft(int nodeNum) {
        return nodeNum <= this.leftOrder;
    }
}
