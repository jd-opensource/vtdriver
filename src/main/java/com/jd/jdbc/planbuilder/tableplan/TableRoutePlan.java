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
limitations under the Licensesrc/main/java/com/jd/jdbc/vitess/resultset/AbstractDatabaseMetaDataResultSet.java.
*/

package com.jd.jdbc.planbuilder.tableplan;

import com.google.common.collect.Lists;
import com.jd.jdbc.VSchemaManager;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.OrderByParams;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.table.TableRouteEngine;
import com.jd.jdbc.planbuilder.AbstractRoutePlan;
import com.jd.jdbc.planbuilder.Builder;
import com.jd.jdbc.planbuilder.Column;
import com.jd.jdbc.planbuilder.Jointab;
import com.jd.jdbc.planbuilder.MergeSortPlan;
import com.jd.jdbc.planbuilder.PlanBuilder;
import com.jd.jdbc.planbuilder.PrimitiveBuilder;
import com.jd.jdbc.planbuilder.RoutePlan;
import com.jd.jdbc.planbuilder.Symtab;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.SQLOrderingSpecification;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator;
import com.jd.jdbc.sqlparser.ast.expr.SQLInListExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLLiteralExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLMethodInvokeExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNullExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefListExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelect;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectOrderByItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQuery;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.TableRouteGetEngineVisitor;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtTableRouteWireupFixUpAstVisitor;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtTableRouteWireupVarFormatVisitor;
import com.jd.jdbc.sqlparser.utils.TableNameUtils;
import com.jd.jdbc.sqltypes.VtPlanValue;
import com.jd.jdbc.tindexes.LogicTable;
import com.jd.jdbc.tindexes.TableIndex;
import com.jd.jdbc.vitess.VitessDataSource;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TableRoutePlan extends AbstractRoutePlan {

    private TableRouteEngine tableRouteEngine;

    private VSchemaManager vm;

    public TableRoutePlan(final SQLSelectQuery stmt, final VSchemaManager vm) {
        super(stmt);
        this.vm = vm;
    }

    @Override
    public void wireup(final Builder bldr, final Jointab jt) throws SQLException {
        if (this.tableRouteEngine.getVtPlanValueList() == null || this.tableRouteEngine.getVtPlanValueList().isEmpty()) {
            SQLExpr vals = this.getCondition();
            if (vals instanceof SQLBinaryOpExpr) {
                VtPlanValue pv = this.procureValues(bldr, jt, ((SQLBinaryOpExpr) vals).getRight());
                this.tableRouteEngine.setVtPlanValueList(Lists.newArrayList(pv));
                ((SQLBinaryOpExpr) vals).setRight(new SQLVariantRefListExpr("::" + Engine.LIST_VAR_NAME));
            } else if (vals instanceof SQLInListExpr) {
                VtPlanValue pv = this.procureValues(bldr, jt, vals);
                this.tableRouteEngine.setVtPlanValueList(Lists.newArrayList(pv));
                ((SQLInListExpr) vals).setTargetList(Lists.newArrayList(new SQLVariantRefListExpr("::" + Engine.LIST_VAR_NAME)));
            } else if (vals == null) {
                // no-op
            } else {
                VtPlanValue pv = this.procureValues(bldr, jt, vals);
                if (pv.getVtPlanValueList() == null || pv.getVtPlanValueList().isEmpty()) {
                    this.tableRouteEngine.setVtPlanValueList(Lists.newArrayList(pv));
                } else {
                    this.tableRouteEngine.setVtPlanValueList(pv.getVtPlanValueList());
                }
            }
        }

        // Fix up the AST.
        VtTableRouteWireupFixUpAstVisitor fixUpAstVisitor = new VtTableRouteWireupFixUpAstVisitor(this);
        this.getSelect().accept(fixUpAstVisitor);

        // Substitute table names
        for (RoutePlan.TableSubstitution sub : this.getTableSubstitutionList()) {
            sub.setOldExprTableSource(sub.getNewExprTableSource());
        }

        VtTableRouteWireupVarFormatVisitor varFormatVisitor = new VtTableRouteWireupVarFormatVisitor(this, jt, bldr);
        this.getSelect().accept(varFormatVisitor);

        // Generate query while simultaneously resolving values.
        SQLSelectQuery selectQuery;
        SQLSelectQuery selectFieldQuery;
        if (this.getSelect() instanceof MySqlSelectQueryBlock) {
            selectQuery = this.generateQuery((MySqlSelectQueryBlock) this.getSelect(), false);
            selectFieldQuery = this.generateQuery((MySqlSelectQueryBlock) this.getSelect(), true);
        } else if (this.getSelect() instanceof SQLUnionQuery) {
            selectQuery = this.generateQuery((SQLUnionQuery) this.getSelect(), false);
            selectFieldQuery = this.generateQuery((SQLUnionQuery) this.getSelect(), true);
        } else {
            throw new SQLException("unsupported statement: " + SQLUtils.toMySqlString(this.getSelect(), SQLUtils.NOT_FORMAT_OPTION).trim());
        }

        this.tableRouteEngine.setSelectQuery(this.getSelect());
    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        List<LogicTable> ltbs = PlanBuilder.getLogicTables(this.getTableRouteEngine().getKeyspaceName(), this.getSelect());
        this.getTableRouteEngine().setLogicTables(ltbs);
        Map<String, String> switchTable = new HashMap<>();
        for (LogicTable ltb : ltbs) {
            switchTable.put(ltb.getLogicTable(), ltb.getFirstActualTableName());
        }
        VtPlanValue planValue = null;
        if (this.getTableRouteEngine().getVtPlanValueList() != null && !this.getTableRouteEngine().getVtPlanValueList().isEmpty()) {
            planValue = this.getTableRouteEngine().getVtPlanValueList().get(0);
        }
        TableRouteGetEngineVisitor visitor = new TableRouteGetEngineVisitor(switchTable, planValue);
        SQLSelectQuery cloneQuery = this.tableRouteEngine.getSelectQuery().clone();
        cloneQuery.accept(visitor);
        SQLSelectStatement stmt = new SQLSelectStatement(new SQLSelect(cloneQuery));
        PrimitiveEngine engine = PlanBuilder.buildSelectPlan(stmt, this.vm, this.tableRouteEngine.getKeyspaceName());
        this.tableRouteEngine.setExecuteEngine(engine);
        return this.tableRouteEngine;
    }

    public Boolean isSingleShard() {
        switch (this.tableRouteEngine.getRouteOpcode()) {
            case SelectEqual:
            case SelectEqualUnique:
                return true;
            default:
                return false;
        }
    }

    @Override
    public Builder pushOrderBy(final SQLOrderBy orderBy) throws SQLException {
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
                ((MySqlSelectQueryBlock) this.getSelect()).setOrderBy(sqlOrderBy);
                return this;
            }
        }

        SQLOrderBy sqlOrderBy = new SQLOrderBy();
        for (SQLSelectOrderByItem orderByItem : orderByList) {
            if (orderByItem.getType() == null) {
                orderByItem.setType(SQLOrderingSpecification.ASC);
            }
            int colNumber = -1;
            SQLExpr expr = orderByItem.getExpr();
            if (expr instanceof SQLLiteralExpr) {
                colNumber = PlanBuilder.resultFromNumber(this.getResultColumns(), (SQLLiteralExpr) expr);
            } else if (expr instanceof SQLName) {
                Column metadataCol = ((SQLName) expr).getMetadata();
                for (int i = 0; i < this.getResultColumns().size(); i++) {
                    if (this.getResultColumns().get(i).getColumn() == metadataCol) {
                        colNumber = i;
                        break;
                    }
                }
            } else {
                throw new SQLFeatureNotSupportedException("unsupported: in sharded table query: complex order by expression:" + expr.toString());
            }
            // If column is not found, then the order by is referencing
            // a column that's not on the select list.
            if (colNumber == -1) {
                throw new SQLFeatureNotSupportedException("unsupported: in sharded table query: order by must reference a column in the select list:" + expr);
            }

            OrderByParams orderByParams = new OrderByParams(colNumber, orderByItem.getType() == SQLOrderingSpecification.DESC);
            this.tableRouteEngine.getOrderBy().add(orderByParams);
            sqlOrderBy.addItem(orderByItem);
        }
        ((MySqlSelectQueryBlock) this.getSelect()).setOrderBy(sqlOrderBy);

        return new MergeSortPlan(this);
    }

    /**
     * computes the plan for the specified filter.
     *
     * @param pb
     * @param filter
     * @return
     */
    private ComputeTablePlanResponse computePlan(final PrimitiveBuilder pb, final SQLExpr filter) throws SQLException {
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
        return new ComputeTablePlanResponse(Engine.TableRouteOpcode.SelectScatter, null, null);
    }

    /**
     * computeEqualPlan computes the plan for an equality constraint.
     *
     * @param pb
     * @param binaryOpExpr
     * @return
     * @throws SQLException
     */
    private ComputeTablePlanResponse computeEqualPlan(final PrimitiveBuilder pb, final SQLBinaryOpExpr binaryOpExpr) throws SQLException {
        SQLExpr left = binaryOpExpr.getLeft();
        SQLExpr right = binaryOpExpr.getRight();

        if (right instanceof SQLNullExpr) {
            return new ComputeTablePlanResponse(Engine.TableRouteOpcode.SelectNone, null, null);
        }

        TableIndex vindex = this.findTIndex(pb, left);
        if (vindex == null) {
            SQLExpr temp = left;
            left = right;
            right = temp;
            vindex = this.findTIndex(pb, left);
            if (vindex == null) {
                return new ComputeTablePlanResponse(Engine.TableRouteOpcode.SelectScatter, null, null);
            }
        }
        if (!this.exprIsValue(right)) {
            return new ComputeTablePlanResponse(Engine.TableRouteOpcode.SelectScatter, null, null);
        }
        return new ComputeTablePlanResponse(Engine.TableRouteOpcode.SelectEqual, vindex, right);
    }

    /**
     * computeEqualPlan computes the plan for an equality constraint.
     *
     * @param pb
     * @param binaryOpExpr
     * @return
     * @throws SQLException
     */
    private ComputeTablePlanResponse computeIsPlan(final PrimitiveBuilder pb, final SQLBinaryOpExpr binaryOpExpr) throws SQLException {
        // we only handle IS NULL correct. IsExpr can contain other expressions as well
        if (binaryOpExpr.getOperator() == SQLBinaryOperator.IsNot && (binaryOpExpr.getRight() instanceof SQLNullExpr)) {
            return new ComputeTablePlanResponse(Engine.TableRouteOpcode.SelectScatter, null, null);
        }

        SQLExpr expr = binaryOpExpr.getLeft();
        TableIndex vindex = this.findTIndex(pb, expr);
        // fallback to scatter gather if there is no vindex
        if (vindex == null) {
            return new ComputeTablePlanResponse(Engine.TableRouteOpcode.SelectScatter, null, null);
        }
        return new ComputeTablePlanResponse(Engine.TableRouteOpcode.SelectEqual, vindex, new SQLNullExpr());
    }

    /**
     * computeINPlan computes the plan for an IN constraint.
     *
     * @param pb
     * @param sqlInListExpr
     * @return
     * @throws SQLException
     */
    private ComputeTablePlanResponse computeINPlan(final PrimitiveBuilder pb, final SQLInListExpr sqlInListExpr) throws SQLException {
        SQLExpr leftExpr = sqlInListExpr.getExpr();
        List<SQLExpr> right = sqlInListExpr.getTargetList();

        TableIndex vindex = this.findTIndex(pb, leftExpr);
        if (vindex == null) {
            return new ComputeTablePlanResponse(Engine.TableRouteOpcode.SelectScatter, null, null);
        }

        if ((right.size() == 1) && (right.get(0) instanceof SQLNullExpr)) {
            return new ComputeTablePlanResponse(Engine.TableRouteOpcode.SelectNone, null, null);
        }

        for (SQLExpr sqlExpr : right) {
            if (!exprIsValue(sqlExpr)) {
                return new ComputeTablePlanResponse(Engine.TableRouteOpcode.SelectScatter, null, null);
            }
        }

        return new ComputeTablePlanResponse(Engine.TableRouteOpcode.SelectIN, vindex, sqlInListExpr);
    }

    /**
     * computeNotInPlan looks for null values to produce a SelectNone if found.
     *
     * @param pb
     * @param sqlInListExpr
     * @return
     * @throws SQLException
     */
    private ComputeTablePlanResponse computeNotINPlan(final PrimitiveBuilder pb, final SQLInListExpr sqlInListExpr) throws SQLException {
        List<SQLExpr> right = sqlInListExpr.getTargetList();

        for (SQLExpr sqlExpr : right) {
            if (sqlExpr instanceof SQLNullExpr) {
                return new ComputeTablePlanResponse(Engine.TableRouteOpcode.SelectNone, null, null);
            }
        }

        return new ComputeTablePlanResponse(Engine.TableRouteOpcode.SelectScatter, null, null);
    }

    private TableIndex findTIndex(final PrimitiveBuilder pb, final SQLExpr col) throws SQLException {
        if (col instanceof SQLName) {
            LogicTable ltb = this.searchTable(pb, (SQLName) col);
            if (ltb.getTindexCol().getColumnName().equalsIgnoreCase(((SQLName) col).getSimpleName())) {
                return ltb.getTableIndex();
            }
        }
        return null;
    }

    private LogicTable searchTable(final PrimitiveBuilder pb, final SQLName col) throws SQLException {
        Symtab.Table table = pb.getSymtab().findOriginTable(col);
        if (table == null) {
            throw new SQLException("cannot find origin table");
        }
        String tableName = TableNameUtils.getTableSimpleName(table.getTableName());
        LogicTable ltb = VitessDataSource.getLogicTable(pb.getDefaultKeyspace(), tableName);
        if (ltb == null) {
            throw new SQLException("cannot find origin table");
        }
        return ltb;
    }

    /**
     * UpdatePlan evaluates the primitive against the specified
     * filter. If it's an improvement, the primitive is updated.
     * We assume that the filter has already been pushed into
     * the route.
     *
     * @param pb
     * @param filter
     * @throws SQLException
     */
    @Override
    public void updatePlan(final PrimitiveBuilder pb, final SQLExpr filter) throws SQLException {
        switch (this.tableRouteEngine.getRouteOpcode()) {
            // For these opcodes, a new filter will not make any difference, so we can just exit early
            case SelectNext:
            case SelectNone:
                return;
            default:
                break;
        }
        ComputeTablePlanResponse computePlanResponse = this.computePlan(pb, filter);
        Engine.TableRouteOpcode opcode = computePlanResponse.getOpcode();
        TableIndex vindex = computePlanResponse.getTindex();
        SQLExpr condition = computePlanResponse.getCondition();
        if (Engine.TableRouteOpcode.SelectScatter.equals(opcode)) {
            return;
        }
        // If we get SelectNone in next filters, override the previous route plan.
        if (Engine.TableRouteOpcode.SelectNone.equals(opcode)) {
            this.updateRoute(opcode, vindex, condition);
            return;
        }
        switch (this.tableRouteEngine.getRouteOpcode()) {
            case SelectEqual:
                if (opcode == Engine.TableRouteOpcode.SelectEqualUnique) {
                    this.updateRoute(opcode, vindex, condition);
                }
                break;
            case SelectIN:
                switch (opcode) {
                    case SelectEqualUnique:
                    case SelectEqual:
                        this.updateRoute(opcode, vindex, condition);
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
    private void updateRoute(final Engine.TableRouteOpcode opcode, final TableIndex vindex, final SQLExpr condition) {
        this.tableRouteEngine.setRouteOpcode(opcode);
        this.tableRouteEngine.setTableIndex(vindex);
        this.setCondition(condition);
    }

    @Override
    public boolean isSplitTablePlan() {
        return true;
    }

    @Getter
    @AllArgsConstructor
    private static class ComputeTablePlanResponse {
        private final Engine.TableRouteOpcode opcode;

        private final TableIndex tindex;

        private final SQLExpr condition;
    }
}
