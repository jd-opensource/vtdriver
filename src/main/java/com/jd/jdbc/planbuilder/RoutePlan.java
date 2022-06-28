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
import com.jd.jdbc.engine.OrderByParams;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.RouteEngine;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.SQLOrderingSpecification;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLInListExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLLiteralExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLMethodInvokeExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNullExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefListExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLJoinTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectOrderByItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQuery;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRouteWireupFixUpAstVisitor;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRouteWireupVarFormatVisitor;
import com.jd.jdbc.sqltypes.VtPlanValue;
import com.jd.jdbc.vindexes.SingleColumn;
import com.jd.jdbc.vindexes.hash.BinaryHash;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import static com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator.Equality;
import static com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator.IsNot;

@Data
public class RoutePlan extends AbstractRoutePlan {
    private static final String SYSTEM_TABLE_INFORMATION_SCHEMA = "information_schema";

    private static final String SYSTEM_TABLE_PERFORMANCE_SCHEMA = "performance_schema";

    private static final String SYSTEM_TABLE_SYS = "sys";

    private static final String SYSTEM_TABLE_MYSQL = "mysql";

    private RouteEngine routeEngine;

    public RoutePlan(SQLSelectQuery stmt) {
        super(stmt);
    }

    public static Boolean systemTable(String qualifier) {
        return SYSTEM_TABLE_INFORMATION_SCHEMA.equalsIgnoreCase(qualifier)
            || SYSTEM_TABLE_PERFORMANCE_SCHEMA.equalsIgnoreCase(qualifier)
            || SYSTEM_TABLE_SYS.equalsIgnoreCase(qualifier)
            || SYSTEM_TABLE_MYSQL.equalsIgnoreCase(qualifier);
    }

    public AbstractRoutePlan resolve() {
        AbstractRoutePlan current = this;
        while (current.redirect != null) {
            current = current.redirect;
        }
        return current;
    }

    /**
     * PushOrderBy satisfies the builder interface.
     *
     * @param orderBy
     * @return
     */
    @Override
    public Builder pushOrderBy(SQLOrderBy orderBy) throws SQLException {
        if (orderBy == null) {
            return this;
        }
        List<SQLSelectOrderByItem> orderByList = orderBy.getItems();
        if (orderByList == null || orderByList.isEmpty()) {
            return this;
        } else if (orderByList.size() == 1) {
            boolean isSpecial = false;
            SQLExpr expr = orderByList.get(0).getExpr();
            if (expr instanceof SQLNullExpr) {
                isSpecial = true;
            } else if (expr instanceof SQLMethodInvokeExpr) {
                String methodName = ((SQLMethodInvokeExpr) expr).getMethodName();
                if ("rand".equalsIgnoreCase(methodName)) {
                    isSpecial = true;
                }
            }
            if (isSpecial) {
                SQLOrderBy sqlOrderBy = new SQLOrderBy();
                sqlOrderBy.addItem(orderByList.get(0));
                ((MySqlSelectQueryBlock) this.select).setOrderBy(sqlOrderBy);
                return this;
            }
        }

        if (this.isSingleShard()) {
            SQLOrderBy sqlOrderBy = new SQLOrderBy();
            for (SQLSelectOrderByItem orderByItem : orderByList) {
                if (orderByItem.getType() == null) {
                    orderByItem.setType(SQLOrderingSpecification.ASC);
                }
                sqlOrderBy.addItem(orderByItem);
            }
            if (this.select instanceof MySqlSelectQueryBlock) {
                ((MySqlSelectQueryBlock) this.select).setOrderBy(sqlOrderBy);
            } else if (this.select instanceof SQLUnionQuery) {
                ((SQLUnionQuery) this.select).setOrderBy(sqlOrderBy);
            }
            return this;
        }

        // If it's a scatter, we have to populate the OrderBy field.
        SQLOrderBy sqlOrderBy = new SQLOrderBy();
        for (SQLSelectOrderByItem orderByItem : orderByList) {
            if (orderByItem.getType() == null) {
                orderByItem.setType(SQLOrderingSpecification.ASC);
            }
            int colNumber = -1;
            SQLExpr expr = orderByItem.getExpr();
            if (expr instanceof SQLLiteralExpr) {
                colNumber = PlanBuilder.resultFromNumber(this.resultColumns, (SQLLiteralExpr) expr);
            } else if (expr instanceof SQLName) {
                Column metadataCol = ((SQLName) expr).getMetadata();
                for (int i = 0; i < this.resultColumns.size(); i++) {
                    if (this.resultColumns.get(i).getColumn() == metadataCol) {
                        colNumber = i;
                        break;
                    }
                }
            } else {
                throw new SQLException("unsupported: in scatter query: complex order by expression:" + expr.toString());
            }
            // If column is not found, then the order by is referencing
            // a column that's not on the select list.
            if (colNumber == -1) {
                throw new SQLException("unsupported: in scatter query: order by must reference a column in the select list:" + expr);
            }

            OrderByParams orderByParams = new OrderByParams(colNumber, orderByItem.getType() == SQLOrderingSpecification.DESC);
            this.routeEngine.getOrderBy().add(orderByParams);
            sqlOrderBy.addItem(orderByItem);
        }
        ((MySqlSelectQueryBlock) this.select).setOrderBy(sqlOrderBy);

        return new MergeSortPlan(this);
    }

    /**
     * Wireup satisfies the builder interface.
     *
     * @param bldr
     */
    @Override
    public void wireup(Builder bldr, Jointab jt) throws SQLException {
        if (this.routeEngine.getVtPlanValueList() == null || this.routeEngine.getVtPlanValueList().isEmpty()) {
            SQLExpr vals = this.condition;
            if (vals instanceof SQLBinaryOpExpr) {
                VtPlanValue pv = this.procureValues(bldr, jt, ((SQLBinaryOpExpr) vals).getRight());
                this.routeEngine.setVtPlanValueList(new ArrayList<VtPlanValue>() {{
                    add(pv);
                }});
                ((SQLBinaryOpExpr) vals).setRight(new SQLVariantRefListExpr("::" + Engine.LIST_VAR_NAME));
            } else if (vals instanceof SQLInListExpr) {
                VtPlanValue pv = this.procureValues(bldr, jt, vals);
                this.routeEngine.setVtPlanValueList(new ArrayList<VtPlanValue>() {{
                    add(pv);
                }});
                ((SQLInListExpr) vals).setTargetList(new ArrayList<SQLExpr>() {{
                    add(new SQLVariantRefListExpr("::" + Engine.LIST_VAR_NAME));
                }});
            } else if (vals == null) {
                // no-op
            } else {
                VtPlanValue pv = this.procureValues(bldr, jt, vals);
                if (pv.getVtPlanValueList() == null || pv.getVtPlanValueList().isEmpty()) {
                    this.routeEngine.setVtPlanValueList(new ArrayList<VtPlanValue>() {{
                        add(pv);
                    }});
                } else {
                    this.routeEngine.setVtPlanValueList(pv.getVtPlanValueList());
                }
            }
        }

        // Fix up the AST.
        VtRouteWireupFixUpAstVisitor fixUpAstVisitor = new VtRouteWireupFixUpAstVisitor(this);
        this.select.accept(fixUpAstVisitor);

        // Substitute table names
        for (TableSubstitution sub : this.tableSubstitutionList) {
            sub.oldExprTableSource = sub.newExprTableSource;
        }

        VtRouteWireupVarFormatVisitor varFormatVisitor = new VtRouteWireupVarFormatVisitor(this, jt, bldr);
        this.select.accept(varFormatVisitor);

        // Generate query while simultaneously resolving values.
        SQLSelectQuery selectQuery;
        SQLSelectQuery selectFieldQuery;
        if (this.select instanceof MySqlSelectQueryBlock) {
            selectQuery = this.generateQuery((MySqlSelectQueryBlock) this.select, false);
            selectFieldQuery = this.generateQuery((MySqlSelectQueryBlock) this.select, true);
        } else if (this.select instanceof SQLUnionQuery) {
            selectQuery = this.generateQuery((SQLUnionQuery) this.select, false);
            selectFieldQuery = this.generateQuery((SQLUnionQuery) this.select, true);
        } else {
            throw new SQLException("unsupported statement: " + SQLUtils.toMySqlString(this.select, SQLUtils.NOT_FORMAT_OPTION).trim());
        }
        this.routeEngine.setQuery(SQLUtils.toMySqlString(selectQuery, SQLUtils.NOT_FORMAT_OPTION).trim());
        this.routeEngine.setSelectQuery(this.select);
        this.routeEngine.setFieldQuery(SQLUtils.toMySqlString(selectFieldQuery, SQLUtils.NOT_FORMAT_OPTION).trim());
        this.routeEngine.setSelectFieldQuery(selectFieldQuery);
    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        return this.routeEngine;
    }

    @Override
    public boolean isSplitTablePlan() {
        return false;
    }

    private boolean isSameShardPinnedQuery(RoutePlan newRoutePlan) {
        return Engine.RouteOpcode.SelectEqualUnique.equals(newRoutePlan.getRouteEngine().getRouteOpcode())
            && this.routeEngine.isQueryPinnedTable() && newRoutePlan.routeEngine.isQueryPinnedTable()
            && !this.routeEngine.getPinned().isEmpty()
            && this.routeEngine.getPinned().equalsIgnoreCase(newRoutePlan.routeEngine.getPinned());
    }

    public boolean joinCanMerge(PrimitiveBuilder pb, RoutePlan rightRoutePlan, SQLJoinTableSource joinTableSource) {
        if (!this.routeEngine.getKeyspace().getName().equals(rightRoutePlan.getRouteEngine().getKeyspace().getName())) {
            return false;
        }
        if (Engine.RouteOpcode.SelectReference.equals(rightRoutePlan.getRouteEngine().getRouteOpcode())) {
            return true;
        }
        switch (this.routeEngine.getRouteOpcode()) {
            case SelectUnsharded:
            case SelectDBA:
                return this.routeEngine.getRouteOpcode().equals(rightRoutePlan.getRouteEngine().getRouteOpcode());
            case SelectEqualUnique:
                if (Engine.RouteOpcode.SelectEqualUnique.equals(rightRoutePlan.getRouteEngine().getRouteOpcode())
                    && this.routeEngine.getVindex().toString().equals(rightRoutePlan.getRouteEngine().getVindex().toString())
                    && PlanBuilder.valEqual(this.condition, rightRoutePlan.getCondition())) {
                    return true;
                }
                if (isSameShardPinnedQuery(rightRoutePlan)) {
                    return true;
                }
                break;
            case SelectReference:
                return true;
            case SelectNext:
                return false;
            default:
                break;
        }
        if (joinTableSource == null) {
            return false;
        }
        for (SQLExpr filter : PlanBuilder.splitAndExpression(new ArrayList<>(), joinTableSource.getCondition())) {
            if (this.canMergeOnFilter(pb, rightRoutePlan, filter)) {
                return true;
            }
        }
        return false;
    }

    public Boolean subqueryCanMerge(PrimitiveBuilder pb, RoutePlan inner) {
        if (!this.routeEngine.getKeyspace().getName().equals(inner.routeEngine.getKeyspace().getName())) {
            return false;
        }
        switch (this.routeEngine.getRouteOpcode()) {
            case SelectUnsharded:
            case SelectDBA:
            case SelectReference:
                return this.routeEngine.getRouteOpcode().equals(inner.routeEngine.getRouteOpcode())
                    || Engine.RouteOpcode.SelectReference.equals(inner.routeEngine.getRouteOpcode());
            case SelectEqualUnique:
                // Check if they target the same shard.
                if (Engine.RouteOpcode.SelectEqualUnique.equals(inner.routeEngine.getRouteOpcode())
                    && this.routeEngine.getVindex().toString().equals(inner.routeEngine.getVindex().toString())
                    && PlanBuilder.valEqual(this.condition, inner.condition)) {
                    return true;
                }
                if (isSameShardPinnedQuery(inner)) {
                    return true;
                }
                break;
            case SelectNext:
                return false;
            default:
                break;
        }
        // Any sharded plan (including SelectEqualUnique) can merge on a reference table subquery.
        // This excludes the case of SelectReference with a sharded subquery.
        if (Engine.RouteOpcode.SelectReference.equals(inner.routeEngine.getRouteOpcode())) {
            return true;
        }
        SQLExpr vals = inner.condition;
        if (vals instanceof SQLName) {
            return pb.getSymtab().vindex(vals, this).toString().equalsIgnoreCase(inner.routeEngine.getVindex().toString());
        }
        return false;
    }

    public Boolean mergeUnion(RoutePlan right) {
        if (this.unionCanMerge(right)) {
            this.getTableSubstitutionList().addAll(right.getTableSubstitutionList());
            right.setRedirect(this);
            return true;
        }
        return false;
    }

    public Boolean unionCanMerge(RoutePlan rrb) {
        if (!this.routeEngine.getKeyspace().getName().equalsIgnoreCase(rrb.routeEngine.getKeyspace().getName())) {
            return false;
        }
        Engine.RouteOpcode routeOpcode = this.routeEngine.getRouteOpcode();
        switch (routeOpcode) {
            case SelectUnsharded:
            case SelectDBA:
            case SelectReference:
                // 左查询为dual
                if (PlanBuilder.isDualTable(this) && rrb.routeEngine.getRouteOpcode() == Engine.RouteOpcode.SelectEqualUnique) {
                    return true;
                }
                return this.routeEngine.getRouteOpcode().equals(rrb.routeEngine.getRouteOpcode());
            case SelectEqualUnique:
                // Check if they target the same shard.
                if (rrb.routeEngine.getRouteOpcode().equals(Engine.RouteOpcode.SelectEqualUnique)
                    && this.routeEngine.getVindex().toString().equals(rrb.routeEngine.getVindex().toString())
                    && PlanBuilder.valEqual(this.condition, rrb.condition)) {
                    return true;
                }
                if (isSameShardPinnedQuery(rrb)) {
                    return true;
                }
                if (PlanBuilder.isDualTable(rrb)) {
                    return true;
                }
                break;
            case SelectNext:
                return false;
        }
        return false;
    }

    private Boolean canMergeOnFilter(PrimitiveBuilder pb, RoutePlan rightRoutePlan, SQLExpr filter) {
        if (!(filter instanceof SQLBinaryOpExpr)) {
            return false;
        }
        if (!Equality.equals(((SQLBinaryOpExpr) filter).getOperator())) {
            return false;
        }
        SQLExpr left = ((SQLBinaryOpExpr) filter).getLeft();
        SQLExpr right = ((SQLBinaryOpExpr) filter).getRight();
        SingleColumn leftVindex = pb.getSymtab().vindex(left, this);
        if (leftVindex == null) {
            SQLExpr temp = left;
            left = right;
            right = temp;
            leftVindex = pb.getSymtab().vindex(left, this);
        }
        if (leftVindex == null || !leftVindex.isUnique()) {
            return false;
        }
        SingleColumn rightVindex = pb.getSymtab().vindex(right, rightRoutePlan);
        if (rightVindex == null) {
            return false;
        }
        return rightVindex.toString().equals(leftVindex.toString());
    }

    /**
     * UpdatePlan evaluates the primitive against the specified
     * filter. If it's an improvement, the primitive is updated.
     * We assume that the filter has already been pushed into
     * the route.
     *
     * @param pb
     * @param filter
     */
    @Override
    public void updatePlan(PrimitiveBuilder pb, SQLExpr filter) throws SQLException {
        switch (this.routeEngine.getRouteOpcode()) {
            // For these opcodes, a new filter will not make any difference, so we can just exit early
            case SelectUnsharded:
            case SelectNext:
            case SelectDBA:
            case SelectReference:
            case SelectNone:
                return;
            default:
                break;
        }
        ComputePlanResponse computePlanResponse = this.computePlan(pb, filter);
        Engine.RouteOpcode opcode = computePlanResponse.getOpcode();
        SingleColumn vindex = computePlanResponse.getVindex();
        SQLExpr condition = computePlanResponse.getCondition();
        if (Engine.RouteOpcode.SelectScatter.equals(opcode)) {
            return;
        }
        // If we get SelectNone in next filters, override the previous route plan.
        if (Engine.RouteOpcode.SelectNone.equals(opcode)) {
            this.updateRoute(opcode, vindex, condition);
            return;
        }
        switch (this.routeEngine.getRouteOpcode()) {
            case SelectEqualUnique:
                if (Engine.RouteOpcode.SelectEqualUnique.equals(opcode) && vindex.cost() < this.routeEngine.getVindex().cost()) {
                    this.updateRoute(opcode, vindex, condition);
                }
                break;
            case SelectEqual:
                switch (opcode) {
                    case SelectEqualUnique:
                        this.updateRoute(opcode, vindex, condition);
                        break;
                    case SelectEqual:
                        if (vindex.cost() < this.routeEngine.getVindex().cost()) {
                            this.updateRoute(opcode, vindex, condition);
                        }
                        break;
                    default:
                        break;
                }
                break;
            case SelectIN:
                switch (opcode) {
                    case SelectEqualUnique:
                    case SelectEqual:
                        this.updateRoute(opcode, vindex, condition);
                        break;
                    case SelectIN:
                        if (vindex.cost() < this.routeEngine.getVindex().cost()) {
                            this.updateRoute(opcode, vindex, condition);
                        }
                        break;
                    default:
                        break;
                }
                break;
            case SelectScatter:
                switch (opcode) {
                    case SelectEqualUnique:
                    case SelectEqual:
                    case SelectIN:
                        /*case SelectNone:*/
                        this.updateRoute(opcode, vindex, condition);
                        break;
                    default:
                        break;
                }
                break;
            default:
                break;
        }
    }

    /**
     * @param opcode
     * @param vindex
     * @param condition
     */
    private void updateRoute(Engine.RouteOpcode opcode, SingleColumn vindex, SQLExpr condition) {
        this.routeEngine.setRouteOpcode(opcode);
        this.routeEngine.setVindex(vindex);
        this.condition = condition;
    }

    /**
     * computes the plan for the specified filter.
     *
     * @param pb
     * @param filter
     * @return
     */
    private ComputePlanResponse computePlan(PrimitiveBuilder pb, SQLExpr filter) throws SQLException {
        if (filter instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) filter;
            switch (binaryOpExpr.getOperator()) {
                case Equality:
                    return this.computeEqualPlan(pb, binaryOpExpr);
                case Is:
                case IsNot:
                    return this.computeIsPlan(pb, binaryOpExpr);
                default:
                    break;
            }
        } else if (filter instanceof SQLInListExpr) {
            SQLInListExpr inListExpr = (SQLInListExpr) filter;
            if (inListExpr.isNot()) {
                return this.computeNotINPlan(pb, inListExpr);
            } else {
                return this.computeINPlan(pb, inListExpr);
            }
        }
        return new ComputePlanResponse(Engine.RouteOpcode.SelectScatter, null, null);
    }

    /**
     * computeEqualPlan computes the plan for an equality constraint.
     *
     * @param pb
     * @param binaryOpExpr
     * @return
     * @throws SQLException
     */
    private ComputePlanResponse computeEqualPlan(PrimitiveBuilder pb, SQLBinaryOpExpr binaryOpExpr) throws SQLException {
        SQLExpr left = binaryOpExpr.getLeft();
        SQLExpr right = binaryOpExpr.getRight();

        if (right instanceof SQLNullExpr) {
            return new ComputePlanResponse(Engine.RouteOpcode.SelectNone, null, null);
        }

        SingleColumn vindex = pb.getSymtab().vindex(left, this);
        if (vindex == null) {
            SQLExpr temp = left;
            left = right;
            right = temp;
            vindex = pb.getSymtab().getColumnVindex(left, this);
            if (vindex == null) {
                return new ComputePlanResponse(Engine.RouteOpcode.SelectScatter, null, null);
            }
        }
        if (!this.exprIsValue(right)) {
            return new ComputePlanResponse(Engine.RouteOpcode.SelectScatter, null, null);
        }
        if (vindex.isUnique()) {
            return new ComputePlanResponse(Engine.RouteOpcode.SelectEqualUnique, vindex, right);
        }
        return new ComputePlanResponse(Engine.RouteOpcode.SelectEqual, vindex, right);
    }

    /**
     * computeEqualPlan computes the plan for an equality constraint.
     *
     * @param pb
     * @param binaryOpExpr
     * @return
     * @throws SQLException
     */
    private ComputePlanResponse computeIsPlan(PrimitiveBuilder pb, SQLBinaryOpExpr binaryOpExpr) throws SQLException {
        // we only handle IS NULL correct. IsExpr can contain other expressions as well
        if (binaryOpExpr.getOperator() == IsNot && (binaryOpExpr.getRight() instanceof SQLNullExpr)) {
            return new ComputePlanResponse(Engine.RouteOpcode.SelectScatter, null, null);
        }

        SQLExpr expr = binaryOpExpr.getLeft();
        BinaryHash vindex = pb.getSymtab().getColumnVindex(expr, this);
        // fallback to scatter gather if there is no vindex
        if (vindex == null) {
            return new ComputePlanResponse(Engine.RouteOpcode.SelectScatter, null, null);
        }
        if (vindex.isUnique()) {
            return new ComputePlanResponse(Engine.RouteOpcode.SelectEqualUnique, vindex, new SQLNullExpr());
        }
        return new ComputePlanResponse(Engine.RouteOpcode.SelectEqual, vindex, new SQLNullExpr());
    }


    /**
     * computeINPlan computes the plan for an IN constraint.
     *
     * @param pb
     * @param sqlInListExpr
     * @return
     * @throws SQLException
     */
    private ComputePlanResponse computeINPlan(PrimitiveBuilder pb, SQLInListExpr sqlInListExpr) throws SQLException {
        SQLExpr leftExpr = sqlInListExpr.getExpr();
        List<SQLExpr> right = sqlInListExpr.getTargetList();

        BinaryHash vindex = pb.getSymtab().getColumnVindex(leftExpr, this);
        if (vindex == null) {
            return new ComputePlanResponse(Engine.RouteOpcode.SelectScatter, null, null);
        }

        if ((right.size() == 1) && (right.get(0) instanceof SQLNullExpr)) {
            return new ComputePlanResponse(Engine.RouteOpcode.SelectNone, null, null);
        }

        for (SQLExpr sqlExpr : right) {
            if (!exprIsValue(sqlExpr)) {
                return new ComputePlanResponse(Engine.RouteOpcode.SelectScatter, null, null);
            }
        }

        return new ComputePlanResponse(Engine.RouteOpcode.SelectIN, vindex, sqlInListExpr);
    }


    /**
     * computeNotInPlan looks for null values to produce a SelectNone if found
     *
     * @param pb
     * @param sqlInListExpr
     * @return
     * @throws SQLException
     */
    private ComputePlanResponse computeNotINPlan(PrimitiveBuilder pb, SQLInListExpr sqlInListExpr) throws SQLException {
        List<SQLExpr> right = sqlInListExpr.getTargetList();

        for (SQLExpr sqlExpr : right) {
            if (sqlExpr instanceof SQLNullExpr) {
                return new ComputePlanResponse(Engine.RouteOpcode.SelectNone, null, null);
            }
        }

        return new ComputePlanResponse(Engine.RouteOpcode.SelectScatter, null, null);
    }

    public Boolean mergeSubquery(PrimitiveBuilder pb, RoutePlan inner) {
        if (this.subqueryCanMerge(pb, inner)) {
            this.tableSubstitutionList.addAll(inner.getTableSubstitutionList());
            inner.setRedirect(this);
            return true;
        }
        return false;
    }

    public Boolean isSingleShard() {
        switch (this.routeEngine.getRouteOpcode()) {
            case SelectUnsharded:
            case SelectDBA:
            case SelectNext:
            case SelectEqualUnique:
            case SelectReference:
                return true;
            default:
                return false;
        }
    }

    @Getter
    @AllArgsConstructor
    private static class ComputePlanResponse {
        private final Engine.RouteOpcode opcode;

        private final SingleColumn vindex;

        private final SQLExpr condition;
    }
}
