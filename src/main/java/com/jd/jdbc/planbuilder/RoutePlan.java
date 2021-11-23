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
import com.jd.jdbc.sqlparser.SqlParser;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLLimit;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.SQLOrderingSpecification;
import com.jd.jdbc.sqlparser.ast.SQLSetQuantifier;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLInListExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntegerExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLLiteralExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLMethodInvokeExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNullExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefListExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLJoinTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelect;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectOrderByItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLSubqueryTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionOperator;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQueryTableSource;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRouteWireupFixUpAstVisitor;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRouteWireupVarFormatVisitor;
import com.jd.jdbc.sqltypes.VtPlanValue;
import com.jd.jdbc.vindexes.SingleColumn;
import com.jd.jdbc.vindexes.hash.BinaryHash;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import static com.jd.jdbc.sqlparser.SqlParser.HAVING_STR;
import static com.jd.jdbc.sqlparser.SqlParser.WHERE_STR;
import static com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator.Equality;
import static com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator.IsNot;
import static com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator.NotEqual;

@Data
public class RoutePlan implements Builder {
    private static final String SYSTEM_TABLE_INFORMATION_SCHEMA = "information_schema";

    private static final String SYSTEM_TABLE_PERFORMANCE_SCHEMA = "performance_schema";

    private static final String SYSTEM_TABLE_SYS = "sys";

    private static final String SYSTEM_TABLE_MYSQL = "mysql";

    private Integer order;

    // Redirect may point to another route if this route
    // was merged with it. The Resolve function chases
    // this pointer till the last un-redirected route.
    private RoutePlan redirect;

    // Select is the AST for the query fragment that will be
    // executed by this route.
    private SQLSelectQuery select;

    private ArrayList<ResultColumn> resultColumns = new ArrayList<>();

    private HashMap<ResultColumn, Integer> weightStrings = new HashMap<>();

    private List<TableSubstitution> tableSubstitutionList = new ArrayList<>();

    private SQLExpr condition;

    private RouteEngine routeEngine;

    public RoutePlan(SQLSelectQuery stmt) {
        this.select = stmt;
        this.order = 1;
    }

    public static Boolean systemTable(String qualifier) {
        return SYSTEM_TABLE_INFORMATION_SCHEMA.equalsIgnoreCase(qualifier)
            || SYSTEM_TABLE_PERFORMANCE_SCHEMA.equalsIgnoreCase(qualifier)
            || SYSTEM_TABLE_SYS.equalsIgnoreCase(qualifier)
            || SYSTEM_TABLE_MYSQL.equalsIgnoreCase(qualifier);
    }

    public RoutePlan resolve() {
        RoutePlan current = this;
        while (current.redirect != null) {
            current = current.redirect;
        }
        return current;
    }

    @Override
    public Integer order() {
        return this.order;
    }

    @Override
    public void reorder(Integer order) {
        this.order = order + 1;
    }

    @Override
    public List<ResultColumn> resultColumns() {
        return this.resultColumns;
    }

    @Override
    public Builder first() {
        return this;
    }

    /**
     * satisfies the builder interface.
     * The primitive will be updated if the new filter improves the plan.
     *
     * @param pb
     * @param filter
     * @param whereType
     * @param origin
     */
    @Override
    public void pushFilter(PrimitiveBuilder pb, SQLExpr filter, String whereType, Builder origin) throws SQLException {
        switch (whereType) {
            case WHERE_STR:
                ((MySqlSelectQueryBlock) this.select).addWhere(filter);
                break;
            case HAVING_STR:
                SQLSelectGroupByClause groupBy = ((MySqlSelectQueryBlock) this.select).getGroupBy();
                if (groupBy == null) {
                    groupBy = new SQLSelectGroupByClause();
                }
                groupBy.addHaving(filter);
                ((MySqlSelectQueryBlock) this.select).setGroupBy(groupBy);
            default:
                break;
        }
        this.updatePlan(pb, filter);
    }

    /**
     * PushSelect satisfies the builder interface.
     *
     * @param pb
     * @param expr
     * @param origin
     * @return
     */
    @Override
    public PushSelectResponse pushSelect(PrimitiveBuilder pb, SQLSelectItem expr, Builder origin) {
        ((MySqlSelectQueryBlock) this.select).addSelectItem(expr);

        ResultColumn resultColumn = Symtab.newResultColumn(expr, this);
        this.resultColumns.add(resultColumn);

        return new PushSelectResponse(resultColumn, this.resultColumns.size() - 1);
    }

    /**
     * MakeDistinct satisfies the builder interface.
     */
    @Override
    public void makeDistinct() {
        ((MySqlSelectQueryBlock) this.select).setDistionOption(SQLSetQuantifier.DISTINCT);
    }

    /**
     * PushGroupBy satisfies the builder interface.
     *
     * @param groupBy
     */
    @Override
    public void pushGroupBy(SQLSelectGroupByClause groupBy) {
        if (groupBy == null) {
            return;
        }
        SQLSelectGroupByClause newGroupBy = new SQLSelectGroupByClause();
        for (SQLExpr item : groupBy.getItems()) {
            newGroupBy.addItem(item);
        }
        ((MySqlSelectQueryBlock) this.select).setGroupBy(newGroupBy);
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
     * SetLimit adds a LIMIT clause to the route.
     *
     * @param limit
     */
    public void setLimit(SQLLimit limit) {
        if (this.select instanceof MySqlSelectQueryBlock) {
            ((MySqlSelectQueryBlock) this.select).setLimit(limit);
        } else if (this.select instanceof SQLUnionQuery) {
            ((SQLUnionQuery) this.select).setLimit(limit);
        }
    }

    /**
     * SetUpperLimit satisfies the builder interface.
     * The route pushes the limit regardless of the plan.
     * If it's a scatter query, the rows returned will be
     * more than the upper limit, but enough for the limit
     * primitive to chop off where needed.
     *
     * @param count
     */
    @Override
    public void setUpperLimit(SQLExpr count) {
        ((MySqlSelectQueryBlock) this.select).setLimit(new SQLLimit(count));
    }

    /**
     * PushMisc satisfies the builder interface.
     *
     * @param sel
     */
    @Override
    public void pushMisc(MySqlSelectQueryBlock sel) {
        ((MySqlSelectQueryBlock) this.select).setForUpdate(sel.isForUpdate());
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
    public void supplyVar(Integer from, Integer to, SQLName colName, String varName) throws SQLException {
        // route is an atomic primitive. So, SupplyVar cannot be
        // called on it.
        throw new SQLException("BUG: route is an atomic node.");
    }

    @Override
    public SupplyColResponse supplyCol(SQLName col) {
        Column c = col.getMetadata();
        for (int i = 0; i < this.resultColumns.size(); i++) {
            ResultColumn rc = this.resultColumns.get(i);
            if (rc.getColumn() == c) {
                return new SupplyColResponse(rc, i);
            }
        }

        // A new result has to be returned.
        ResultColumn rc = new ResultColumn(c);
        this.resultColumns.add(rc);
        SQLName clone = col.clone();
        ((MySqlSelectQueryBlock) this.select).addSelectItem(new SQLSelectItem(clone));
        return new SupplyColResponse(rc, this.resultColumns.size() - 1);
    }

    protected VtPlanValue procureValues(Builder bldr, Jointab jt, SQLExpr val) throws SQLException {
        if (val instanceof SQLInListExpr) {
            SQLExpr expr1 = ((SQLInListExpr) val).getTargetList().get(0);
            if (expr1 instanceof SQLVariantRefListExpr) {
                return SqlParser.newPlanValue(expr1);
            }
            VtPlanValue pv = new VtPlanValue();
            List<SQLExpr> targetList = ((SQLInListExpr) val).getTargetList();
            for (SQLExpr expr : targetList) {
                VtPlanValue v = this.procureValues(bldr, jt, expr);
                pv.getVtPlanValueList().add(v);
            }
            return pv;
        } else if (val instanceof SQLName) {
            String joinVar = jt.procure(bldr, (SQLName) val, this.order());
            return new VtPlanValue(joinVar);
        }
        return SqlParser.newPlanValue(val);
    }

    private SQLExpr generateWhereExpr(SQLExpr expr) {
        if (expr instanceof SQLBinaryOpExpr) {
            SQLExpr left = ((SQLBinaryOpExpr) expr).getLeft();
            if (left instanceof SQLPropertyExpr) {
                SQLExpr owner = ((SQLPropertyExpr) left).getOwner();
                if (owner instanceof SQLPropertyExpr) {
                    SQLPropertyExpr propertyExpr = new SQLPropertyExpr(
                        new SQLPropertyExpr(((SQLPropertyExpr) owner).getOwner(), ((SQLPropertyExpr) owner).getName()),
                        ((SQLPropertyExpr) left).getName()
                    );
                    ((SQLBinaryOpExpr) expr).setLeft(propertyExpr);
                }
            } else {
                generateWhereExpr(left);
            }
            SQLExpr right = ((SQLBinaryOpExpr) expr).getRight();
            if (!this.exprIsValue(right) && !(right instanceof SQLNullExpr)) {
                generateWhereExpr(right);
            }
        }
        return expr;
    }

    /**
     * generateQuery generates a query with an impossible where.
     * This will be used on the RHS node to fetch field info if the LHS
     * returns no result.
     *
     * @param select
     * @return
     */
    public MySqlSelectQueryBlock generateQuery(MySqlSelectQueryBlock select, Boolean isFieldQuery) throws SQLException {
        MySqlSelectQueryBlock cloneSql = new MySqlSelectQueryBlock();
        if (!isFieldQuery) {
            cloneSql.setDistionOption(select.getDistionOption());
            cloneSql.setLimit(select.getLimit());
            cloneSql.setForUpdate(select.isForUpdate());
        }

        SQLTableSource tableSource = select.getFrom();
        if (tableSource instanceof SQLExprTableSource) {
            SQLExprTableSource originFrom = (SQLExprTableSource) tableSource;
            SQLExpr expr = originFrom.getExpr();
            if (expr instanceof SQLPropertyExpr) {
                String ownernName = ((SQLPropertyExpr) expr).getOwnernName();
                String table = ((SQLPropertyExpr) expr).getSimpleName();
                if (RoutePlan.systemTable(ownernName)) {
                    cloneSql.setFrom(new SQLExprTableSource(new SQLPropertyExpr(ownernName, table), originFrom.getAlias()));
                } else {
                    cloneSql.setFrom(new SQLExprTableSource(new SQLIdentifierExpr(table), originFrom.getAlias()));
                }
            } else {
                cloneSql.setFrom(tableSource.clone());
            }
        } else if (tableSource instanceof SQLUnionQueryTableSource) {
            cloneSql.setFrom(this.generateUnionQueryTableSource((SQLUnionQueryTableSource) tableSource, isFieldQuery));
        } else if (tableSource instanceof SQLJoinTableSource) {
            cloneSql.setFrom(this.generateJoinTableSource((SQLJoinTableSource) tableSource, isFieldQuery));
        } else if (tableSource instanceof SQLSubqueryTableSource) {
            MySqlSelectQueryBlock subSelectQueryBlock = (MySqlSelectQueryBlock) ((SQLSubqueryTableSource) tableSource).getSelect().getQueryBlock();
            MySqlSelectQueryBlock subSelectQueryBlockClone = this.generateQuery(subSelectQueryBlock, isFieldQuery);
            String alias = tableSource.getAlias();
            if (alias != null) {
                cloneSql.setFrom(new SQLSubqueryTableSource(new SQLSelect(subSelectQueryBlockClone), alias));
            } else {
                cloneSql.setFrom(new SQLSubqueryTableSource(subSelectQueryBlockClone));
            }
        }

        for (SQLSelectItem selectItem : select.getSelectList()) {
            SQLExpr selectExpr = selectItem.getExpr();
            if (selectExpr instanceof SQLPropertyExpr) {
                SQLExpr owner = ((SQLPropertyExpr) selectExpr).getOwner();
                if (owner instanceof SQLPropertyExpr) {
                    SQLPropertyExpr newSelectExpr = new SQLPropertyExpr(
                        new SQLIdentifierExpr(((SQLPropertyExpr) owner).getName()),
                        ((SQLPropertyExpr) selectExpr).getName());
                    selectItem.setExpr(newSelectExpr);
                }
                cloneSql.addSelectItem(selectItem.clone());
            } else if (selectExpr instanceof SQLMethodInvokeExpr) {
                SQLExpr owner = ((SQLMethodInvokeExpr) selectExpr).getOwner();
                if (owner instanceof SQLPropertyExpr) {
                    SQLIdentifierExpr identifierExpr = new SQLIdentifierExpr(((SQLPropertyExpr) owner).getName());
                    ((SQLMethodInvokeExpr) selectExpr).setOwner(identifierExpr);
                    selectItem.setExpr(selectExpr);
                }
                cloneSql.addSelectItem(selectItem.clone());
            } else {
                SQLSelectItem sqlExpr = selectItem.clone();
                cloneSql.addSelectItem(sqlExpr);
            }
        }

        SQLExpr selectWhere = select.getWhere();
        if (isFieldQuery) {
            // 1 != 1
            cloneSql.setWhere(new SQLBinaryOpExpr(new SQLIntegerExpr(1),
                NotEqual,
                new SQLIntegerExpr(1)));
        } else if (selectWhere != null) {
            cloneSql.setWhere(this.generateWhereExpr(selectWhere));
        }

        SQLSelectGroupByClause groupBy = select.getGroupBy();
        if (groupBy != null) {
            SQLSelectGroupByClause selectGroupByClause = new SQLSelectGroupByClause();
            for (SQLExpr item : groupBy.getItems()) {
                if (item instanceof SQLPropertyExpr) {
                    SQLExpr owner = ((SQLPropertyExpr) item).getOwner();
                    if (owner instanceof SQLPropertyExpr) {
                        ((SQLPropertyExpr) item).setOwner(((SQLPropertyExpr) owner).getName());
                    }
                }
                SQLExpr cloneItem = item.clone();
                selectGroupByClause.addItem(cloneItem);
            }

            if (!isFieldQuery) {
                SQLExpr having = groupBy.getHaving();
                if (having != null) {
                    selectGroupByClause.setHaving(this.generateWhereExpr(having));
                }
            }

            selectGroupByClause.setWithRollUp(groupBy.isWithRollUp());
            selectGroupByClause.setWithCube(groupBy.isWithCube());
            cloneSql.setGroupBy(selectGroupByClause);
        }

        if (!isFieldQuery) {
            SQLOrderBy orderBy = select.getOrderBy();
            if (orderBy != null) {
                for (SQLSelectOrderByItem selectOrderByItem : orderBy.getItems()) {
                    SQLExpr orderByItemExpr = selectOrderByItem.getExpr();
                    if (orderByItemExpr instanceof SQLPropertyExpr) {
                        SQLExpr owner = ((SQLPropertyExpr) orderByItemExpr).getOwner();
                        if (owner instanceof SQLPropertyExpr) {
                            SQLPropertyExpr propertyExpr = new SQLPropertyExpr(
                                new SQLIdentifierExpr(((SQLPropertyExpr) owner).getName()),
                                ((SQLPropertyExpr) orderByItemExpr).getName()
                            );
                            selectOrderByItem.setExpr(propertyExpr);
                        }
                    }
                }
                cloneSql.setOrderBy(orderBy.clone());
            }
        }
        return cloneSql;
    }

    public SQLUnionQuery generateQuery(SQLUnionQuery unionQuery, Boolean isFieldQuery) throws SQLException {
        SQLUnionQuery cloneSql = new SQLUnionQuery();
        SQLSelectQuery left = unionQuery.getLeft();
        SQLSelectQuery right = unionQuery.getRight();

        if (!isFieldQuery) {
            cloneSql.setLimit(unionQuery.getLimit());
        }

        SQLSelectQuery leftCloneSql;
        if (left instanceof SQLUnionQuery) {
            leftCloneSql = this.generateQuery((SQLUnionQuery) left, isFieldQuery);
        } else if (left instanceof MySqlSelectQueryBlock) {
            leftCloneSql = this.generateQuery((MySqlSelectQueryBlock) left, isFieldQuery);
        } else {
            throw new SQLException("unsupported statement: " + SQLUtils.toMySqlString(unionQuery, SQLUtils.NOT_FORMAT_OPTION).trim());
        }
        leftCloneSql.setBracket(left.isBracket());

        SQLSelectQuery rightCloneSql;
        if (right instanceof SQLUnionQuery) {
            rightCloneSql = this.generateQuery((SQLUnionQuery) right, isFieldQuery);
        } else if (right instanceof MySqlSelectQueryBlock) {
            rightCloneSql = this.generateQuery((MySqlSelectQueryBlock) right, isFieldQuery);
        } else {
            throw new SQLException("unsupported statement: " + SQLUtils.toMySqlString(unionQuery, SQLUtils.NOT_FORMAT_OPTION).trim());
        }
        rightCloneSql.setBracket(right.isBracket());

        cloneSql.setLeft(leftCloneSql);
        cloneSql.setOperator(unionQuery.getOperator());
        cloneSql.setRight(rightCloneSql);

        if (!isFieldQuery) {
            SQLOrderBy orderBy = unionQuery.getOrderBy();
            if (orderBy != null) {
                for (SQLSelectOrderByItem selectOrderByItem : orderBy.getItems()) {
                    SQLExpr orderByItemExpr = selectOrderByItem.getExpr();
                    if (orderByItemExpr instanceof SQLPropertyExpr) {
                        SQLExpr owner = ((SQLPropertyExpr) orderByItemExpr).getOwner();
                        if (owner instanceof SQLPropertyExpr) {
                            SQLPropertyExpr propertyExpr = new SQLPropertyExpr(
                                new SQLIdentifierExpr(((SQLPropertyExpr) owner).getName()),
                                ((SQLPropertyExpr) orderByItemExpr).getName()
                            );
                            selectOrderByItem.setExpr(propertyExpr);
                        }
                    }
                }
                cloneSql.setOrderBy(orderBy.clone());
            }
        }

        return cloneSql;
    }

    /**
     * SupplyWeightString satisfies the builder interface.
     *
     * @param colNumber
     * @return
     */
    @Override
    public Integer supplyWeightString(Integer colNumber) {
        ResultColumn resultColumn = this.resultColumns.get(colNumber);
        if (this.weightStrings.containsKey(resultColumn)) {
            return this.weightStrings.get(resultColumn);
        }

        SQLExpr params = ((MySqlSelectQueryBlock) this.select).getSelectList().get(colNumber).getExpr();
        SQLExpr expr = new SQLMethodInvokeExpr("weight_string", null, params);
        SQLSelectItem selectItem = new SQLSelectItem(expr);
        // It's ok to pass nil for pb and builder because PushSelect doesn't use them.
        PushSelectResponse response = this.pushSelect(null, selectItem, null);
        this.weightStrings.put(resultColumn, response.getColNumber());
        return response.getColNumber();
    }

    @Override
    public void pushLock(String lock) throws SQLException {
    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        return this.routeEngine;
    }

    @Override
    public boolean isSplitTablePlan() {
        return false;
    }

    /**
     * PushAnonymous pushes an anonymous expression like '*' or NEXT VALUES
     * into the select expression list of the route. This function is
     * similar to PushSelect.
     *
     * @param expr
     * @return
     */
    public ResultColumn pushAnonymous(SQLSelectItem expr) {
        ((MySqlSelectQueryBlock) this.select).addSelectItem(expr);

        // We just create a place-holder resultColumn. It won't
        // match anything.
        ResultColumn resultColumn = new ResultColumn();
        resultColumn.setColumn(new Column(this, null, Query.Type.NULL_TYPE, 0));
        resultColumn.setAlias("");
        this.resultColumns.add(resultColumn);

        return resultColumn;
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

    private SQLUnionQueryTableSource generateUnionQueryTableSource(SQLUnionQueryTableSource unionQueryTableSource, Boolean isFieldQuery) throws SQLException {
        SQLUnionQuery unionQuery = unionQueryTableSource.getUnion();
        SQLSelectQuery unionQueryLeft = unionQuery.getLeft();
        SQLUnionOperator operator = unionQuery.getOperator();
        SQLSelectQuery unionQueryRight = unionQuery.getRight();

        SQLSelectQuery cloneLeft = null;
        if (unionQueryLeft instanceof MySqlSelectQueryBlock) {
            cloneLeft = this.generateQuery((MySqlSelectQueryBlock) unionQueryLeft, isFieldQuery);
        } else if (unionQueryLeft instanceof SQLUnionQuery) {
            cloneLeft = this.generateQuery((SQLUnionQuery) unionQueryLeft, isFieldQuery);
        }

        SQLSelectQuery cloneRight = null;
        if (unionQueryRight instanceof MySqlSelectQueryBlock) {
            cloneRight = this.generateQuery((MySqlSelectQueryBlock) unionQueryRight, isFieldQuery);
        } else if (unionQueryRight instanceof SQLUnionQuery) {
            cloneRight = this.generateQuery((SQLUnionQuery) unionQueryRight, isFieldQuery);
        }

        return new SQLUnionQueryTableSource(new SQLUnionQuery(cloneLeft, operator, cloneRight),
            unionQueryTableSource.getAlias());
    }

    private SQLJoinTableSource generateJoinTableSource(SQLJoinTableSource joinTableSource, Boolean isFieldQuery) throws SQLException {
        SQLTableSource cloneLeft = joinTableSource.getLeft().clone();
        SQLTableSource cloneRight = joinTableSource.getRight().clone();
        SQLJoinTableSource.JoinType joinType = joinTableSource.getJoinType();
        if (cloneLeft instanceof SQLJoinTableSource) {
            cloneLeft = generateJoinTableSource((SQLJoinTableSource) cloneLeft, isFieldQuery);
        } else if (cloneLeft instanceof SQLSubqueryTableSource) {
            MySqlSelectQueryBlock subSelectQueryBlock = (MySqlSelectQueryBlock) ((SQLSubqueryTableSource) cloneLeft).getSelect().getQueryBlock();
            subSelectQueryBlock = this.generateQuery(subSelectQueryBlock, isFieldQuery);
            ((SQLSubqueryTableSource) cloneLeft).setSelect(new SQLSelect(subSelectQueryBlock));
        }
        if (cloneRight instanceof SQLJoinTableSource) {
            cloneRight = generateJoinTableSource((SQLJoinTableSource) cloneRight, isFieldQuery);
        } else if (cloneRight instanceof SQLSubqueryTableSource) {
            MySqlSelectQueryBlock subSelectQueryBlock = (MySqlSelectQueryBlock) ((SQLSubqueryTableSource) cloneRight).getSelect().getQueryBlock();
            subSelectQueryBlock = this.generateQuery(subSelectQueryBlock, isFieldQuery);
            ((SQLSubqueryTableSource) cloneRight).setSelect(new SQLSelect(subSelectQueryBlock));
        }
        SQLExpr condition = joinTableSource.getCondition();
        SQLExpr cloneCondition = null;
        if (condition != null) {
            cloneCondition = condition.clone();
        }
        return new SQLJoinTableSource(cloneLeft, joinType, cloneRight, cloneCondition);
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

    public Boolean isLocal(SQLName col) {
        Column metadata = col.getMetadata();
        if (metadata == null) {
            return true;
        }
        return metadata.origin() == this;
    }

    /**
     * exprIsValue returns true if the expression can be treated as a value
     * for the routeOption. External references are treated as value.
     *
     * @param expr
     * @return
     */
    public Boolean exprIsValue(SQLExpr expr) {
        if (expr instanceof SQLName) {
            return this != ((SQLName) expr).getMetadata().origin();
        }
        return SqlParser.isValue(expr);
    }

    @Getter
    @AllArgsConstructor
    private static class ComputePlanResponse {
        private final Engine.RouteOpcode opcode;

        private final SingleColumn vindex;

        private final SQLExpr condition;
    }

    @Getter
    @Setter
    protected static class TableSubstitution {
        private SQLExprTableSource newExprTableSource;

        private SQLExprTableSource oldExprTableSource;
    }
}
