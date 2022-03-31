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

import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.SqlParser;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLLimit;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.SQLSetQuantifier;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLInListExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntegerExpr;
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
import com.jd.jdbc.sqltypes.VtPlanValue;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

import static com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator.NotEqual;

@Getter
@Setter
public abstract class AbstractRoutePlan implements Builder {
    protected ArrayList<ResultColumn> resultColumns = new ArrayList<>();

    protected Integer order;

    // Redirect may point to another route if this route
    // was merged with it. The Resolve function chases
    // this pointer till the last un-redirected route.
    protected AbstractRoutePlan redirect;

    // Select is the AST for the query fragment that will be
    // executed by this route.
    protected SQLSelectQuery select;

    protected SQLExpr condition;

    protected List<TableSubstitution> tableSubstitutionList = new ArrayList<>();

    protected HashMap<ResultColumn, Integer> weightStrings = new HashMap<>();

    public AbstractRoutePlan(SQLSelectQuery stmt) {
        this.select = stmt;
        this.order = 1;
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

    public abstract void updatePlan(PrimitiveBuilder pb, SQLExpr filter) throws SQLException;

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
    public void pushFilter(final PrimitiveBuilder pb, final SQLExpr filter, final String whereType, final Builder origin) throws SQLException {
        switch (whereType) {
            case SqlParser.WHERE_STR:
                ((MySqlSelectQueryBlock) this.getSelect()).addWhere(filter);
                break;
            case SqlParser.HAVING_STR:
                SQLSelectGroupByClause groupBy = ((MySqlSelectQueryBlock) this.getSelect()).getGroupBy();
                if (groupBy == null) {
                    groupBy = new SQLSelectGroupByClause();
                }
                groupBy.addHaving(filter);
                ((MySqlSelectQueryBlock) this.getSelect()).setGroupBy(groupBy);
                break;
            default:
                break;
        }
        this.updatePlan(pb, filter);
    }

    /**
     * MakeDistinct satisfies the builder interface.
     */
    @Override
    public void makeDistinct() {
        ((MySqlSelectQueryBlock) this.select).setDistionOption(SQLSetQuantifier.DISTINCT);
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

    @Override
    public void pushLock(String lock) throws SQLException {
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

    public Boolean isLocal(SQLName col) {
        Column metadata = col.getMetadata();
        if (metadata == null) {
            return true;
        }
        return metadata.origin() == this;
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

    @Getter
    @Setter
    protected static class TableSubstitution {
        protected SQLExprTableSource newExprTableSource;

        protected SQLExprTableSource oldExprTableSource;
    }
}
