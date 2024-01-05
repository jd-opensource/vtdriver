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

import com.jd.jdbc.common.tuple.ImmutablePair;
import com.jd.jdbc.common.tuple.Pair;
import com.jd.jdbc.common.util.CollectionUtils;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.gen4.AbstractAggregateGen4;
import com.jd.jdbc.planbuilder.PlanBuilder;
import com.jd.jdbc.sqlparser.SqlParser;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.SQLOrderingSpecification;
import com.jd.jdbc.sqlparser.ast.SQLSetQuantifier;
import com.jd.jdbc.sqlparser.ast.expr.SQLAggregateExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLAggregateOption;
import com.jd.jdbc.sqlparser.ast.expr.SQLAllColumnExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLExprUtils;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNullExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectOrderByItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQuery;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqlparser.visitor.CheckForInvalidGroupingExpressionsVisitor;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class QueryProjection {
    /**
     * SelectExpr provides whether the columns is aggregation expression or not.
     */
    @Setter
    @Getter
    public static class SelectExpr {
        SQLSelectItem col;

        boolean aggr;

        SelectExpr() {

        }

        SelectExpr(SQLSelectItem col) {
            this.col = col;
        }

        public SelectExpr(boolean aggr, SQLSelectItem col) {
            this.aggr = aggr;
            this.col = col;
        }

        /**
         * GetAliasedExpr returns the SelectExpr as a *sqlparser.AliasedExpr if its type allows it,
         * otherwise an error is returned.
         *
         * @return
         * @throws SQLException
         */
        public SQLSelectItem getAliasedExpr() throws SQLException {
            SqlParser.SelectExpr selectExpr = SqlParser.SelectExpr.type(col);
            if (SqlParser.SelectExpr.AliasedExpr.equals(selectExpr)) {
                return this.col;
            } else if (SqlParser.SelectExpr.StarExpr.equals(selectExpr)) {
                throw new SQLException("unsupported: '*' expression in cross-shard query");
                // return null;
            } else {
                throw new SQLException("not an aliased expression:" + selectExpr.toString());
                // return null;
            }
        }

        /**
         * GetExpr returns the underlying sqlparser.Expr of our SelectExpr
         *
         * @return
         */
        public SQLExpr getExpr() {
            SqlParser.SelectExpr selectExpr = SqlParser.SelectExpr.type(col);
            if (SqlParser.SelectExpr.AliasedExpr.equals(selectExpr)) {
                return this.col.getExpr();
            } else {
                return null;
            }
        }
    }

    /**
     * GroupBy contains the expression to used in group by and also if grouping is needed at VTGate level
     * then what the weight_string function expression to be sent down for evaluation.
     */
    @Setter
    @Getter
    static class GroupBy {
        SQLExpr inner;

        SQLExpr weightStrExpr;

        /**
         * The index at which the user expects to see this column. Set to nil, if the user does not ask for it
         */
        Integer innerIndex;


        /**
         * The original aliased expression that this group by is referring
         */
        SQLSelectItem aliasedExpr;

        public GroupBy(SQLExpr inner) {
            this.inner = inner;

        }

        public GroupBy(SQLExpr inner, SQLExpr weightStrExpr, Integer innerIndex, SQLSelectItem aliasedExpr) {
            this.inner = inner;
            this.weightStrExpr = weightStrExpr;
            this.innerIndex = innerIndex;
            this.aliasedExpr = aliasedExpr;
        }

        @Override
        public GroupBy clone() {
            return new GroupBy(this.inner.clone(), this.weightStrExpr.clone(), this.innerIndex, this.aliasedExpr.clone());
        }

        public OrderBy asOrderBy() {
            SQLSelectOrderByItem inner = new SQLSelectOrderByItem(this.inner);
            inner.setType(SQLOrderingSpecification.ASC);
            return new OrderBy(inner, this.weightStrExpr);
        }

        public SQLSelectItem asAliasedExpr() {
            if (aliasedExpr != null) {
                return aliasedExpr;
            }
            return new SQLSelectItem(this.getInner());
        }
    }

    /**
     * OrderBy contains the expression to used in order by and also if ordering is needed at VTGate level
     * then what the weight_string function expression to be sent down for evaluation.
     */
    static class OrderBy {

        @Getter
        @Setter
        SQLSelectOrderByItem inner;

        @Getter
        @Setter
        SQLExpr weightStrExpr;

        public OrderBy(SQLSelectOrderByItem inner, SQLExpr weightStrExpr) {
            this.inner = inner;
            this.weightStrExpr = weightStrExpr;
        }
    }

    /**
     * Aggr encodes all information needed for aggregation functions
     */
    @Getter
    @Setter
    static class Aggr {

        private SQLSelectItem original;

        private SQLAggregateExpr func;

        private Engine.AggregateOpcodeG4 opCode;

        private String alias;

        // The index at which the user expects to see this aggregated function. Set to nil, if the user does not ask for it
        private Integer index;

        private Boolean distinct = false;

        public Aggr(SQLSelectItem original, Engine.AggregateOpcodeG4 opCode, String alias, Integer index) {
            this.original = original;
            this.opCode = opCode;
            this.alias = alias;
            this.index = index;
            this.func = null;
        }
    }

    List<SelectExpr> selectExprs = new ArrayList<>();

    boolean hasAggr;

    boolean distinct;

    List<GroupBy> groupByExprs = new ArrayList<>();

    List<OrderBy> orderExprs = new ArrayList<>();

    @Getter
    private boolean canPushDownSorting;

    boolean hasStar;

    // AddedColumn keeps a counter for expressions added to solve HAVING expressions the user is not selecting
    int addedColumn;


    /**
     * CreateQPFromSelect creates the QueryProjection for the input *sqlparser.Select
     *
     * @param sel
     * @throws SQLException
     */
    public void createQPFromSelect(SQLSelectQuery sel) throws SQLException {
        this.distinct = ((MySqlSelectQueryBlock) sel).getDistionOption() == SQLSetQuantifier.DISTINCT;
        this.addSelectExpressions((MySqlSelectQueryBlock) sel);
        if (((MySqlSelectQueryBlock) sel).getGroupBy() != null) {
            for (SQLExpr group : ((MySqlSelectQueryBlock) sel).getGroupBy().getItems()) {
                Pair<Integer, SQLSelectItem> retObjs = this.findSelectExprIndexForExpr(group);
                SQLExpr[] retExprs = this.getSimplifiedExpr(group);
                checkForInvalidGroupingExpressions(retExprs[1]);
                GroupBy groupBy = new GroupBy(retExprs[0], retExprs[1], retObjs.getLeft(), retObjs.getRight());
                this.groupByExprs.add(groupBy);
            }
        }

        this.addOrderBy(((MySqlSelectQueryBlock) sel).getOrderBy());

        if (this.distinct && !this.hasAggr) {
            this.groupByExprs.clear();
        }
    }

    /**
     * CreateQPFromUnion creates the QueryProjection for the input *sqlparser.Union
     *
     * @param union
     * @return
     */
    public static QueryProjection createQPFromUnion(SQLSelectQuery union) throws SQLException {
        QueryProjection qp = new QueryProjection();
        MySqlSelectQueryBlock sel = PlanBuilder.getFirstSelect(union);
        qp.addSelectExpressions(sel);
        if (union instanceof SQLUnionQuery) {
            qp.addOrderBy(((SQLUnionQuery) union).getOrderBy());
        }
        return qp;
    }

    /**
     * returns true if we either have aggregate functions or grouping defined
     *
     * @return
     */
    public boolean needsAggregating() {
        return this.hasAggr || !this.groupByExprs.isEmpty();
    }

    public boolean needsDistinct() {
        if (!distinct) {
            return false;
        }

        if (onlyAggr() && groupByExprs.size() == 0) {
            return false;
        }
        return true;
    }

    public boolean onlyAggr() {
        if (!hasAggr) {
            return false;
        }
        for (SelectExpr expr : selectExprs) {
            if (!expr.aggr) {
                return false;
            }
        }
        return true;
    }

    /**
     * GetGrouping returns a copy of the grouping parameters of the QP
     *
     * @return
     */
    public List<GroupBy> getGrouping() {
        return new ArrayList<>(this.groupByExprs);
    }

    public int getColumnCount() {
        return this.selectExprs.size() - this.addedColumn;
    }

    /**
     * AlignGroupByAndOrderBy aligns the group by and order by columns, so they are in the same order
     * The GROUP BY clause is a set - the order between the elements does not make any difference,
     * so we can simply re-arrange the column order
     * We are also free to add more ORDER BY columns than the user asked for which we leverage,
     * so the input is already ordered according to the GROUP BY columns used
     */
    public void alignGroupByAndOrderBy() {
        // The ORDER BY can be performed before the OA

        List<GroupBy> newGrouping = null;
        if (CollectionUtils.isEmpty(this.orderExprs)) {
            // The query didn't ask for any particular order, so we are free to add arbitrary ordering.
            // We'll align the grouping and ordering by the output columns
            newGrouping = this.getGrouping();
            sortGrouping(newGrouping);
            for (GroupBy groupBy : newGrouping) {
                this.orderExprs.add(groupBy.asOrderBy());
            }
        } else {
            // Here we align the GROUP BY and ORDER BY.
            // First step is to make sure that the GROUP BY is in the same order as the ORDER BY
            boolean[] used = new boolean[this.groupByExprs.size()];
            newGrouping = new ArrayList<>();
            for (OrderBy orderExpr : this.orderExprs) {
                for (int i = 0; i < this.groupByExprs.size(); i++) {
                    GroupBy groupingExpr = groupByExprs.get(i);
                    if (!used[i] && SQLExprUtils.equals(groupingExpr.getWeightStrExpr(), orderExpr.getWeightStrExpr())) {
                        newGrouping.add(groupingExpr);
                        used[i] = true;
                    }
                }
            }

            if (newGrouping.size() != this.groupByExprs.size()) {
                // we are missing some groupings. We need to add them both to the new groupings list, but also to the ORDER BY
                for (int i = 0; i < used.length; i++) {
                    boolean added = used[i];
                    if (!added) {
                        GroupBy groupBy = this.groupByExprs.get(i);
                        newGrouping.add(groupBy);
                        this.orderExprs.add(groupBy.asOrderBy());
                    }
                }
            }
        }
        this.groupByExprs = newGrouping;
    }

    public List<Aggr> aggregationExpressions() throws SQLException {
        for (OrderBy orderExpr : this.orderExprs) {
            if (this.isOrderByExprInGroupBy(orderExpr)) {
                continue;
            }

            SQLExpr orderSQLExpr = orderExpr.inner.getExpr();

            boolean found = false;

            for (QueryProjection.SelectExpr selectExpr : this.getSelectExprs()) {
                // if(selectExpr.getCol() instanceof Aliased)
                if (Objects.equals(selectExpr.getCol().getExpr(), orderSQLExpr)) {
                    found = true;
                    break;
                }
            }
            if (found) {
                continue;
            }

            SQLSelectItem newItem = new SQLSelectItem(orderSQLExpr);
            boolean hasAggr = PlanBuilder.selectItemsHasAggregates(Collections.singletonList(newItem));
            this.selectExprs.add(new SelectExpr(hasAggr, newItem));
            this.addedColumn++;
        }
        List<Aggr> out = new ArrayList<>();
        for (int idx = 0; idx < this.selectExprs.size(); idx++) {
            SelectExpr expr = this.selectExprs.get(idx);
            SQLSelectItem aliasedExpr = expr.getAliasedExpr();
            int idxCopy = idx;

            if (!PlanBuilder.selectItemsHasAggregates(Collections.singletonList(expr.getCol()))) {
                if (!this.isExprInGroupByExprs(expr)) {
                    out.add(new Aggr(aliasedExpr, Engine.AggregateOpcodeG4.AggregateRandom, aliasedExpr.toString(), idxCopy));
                }
                continue;
            }

            if (!(expr.getExpr() instanceof SQLAggregateExpr)) {
                throw new SQLException("unsupported: in scatter query: complex aggregate expression");
            }
            SQLAggregateExpr aggrExpr = (SQLAggregateExpr) expr.getExpr();
            String aggrFunName = aggrExpr.getMethodName().toLowerCase();

            Engine.AggregateOpcodeG4 opcode = AbstractAggregateGen4.SUPPORTED_AGGREGATES.get(aggrFunName);
            if (opcode == null) {
                throw new SQLException("unsupported: in scatter query: aggregation function " + aggrFunName);
            }

            if (opcode == Engine.AggregateOpcodeG4.AggregateCount) {
                // count star
                if (aggrExpr.getArguments().size() == 1 && aggrExpr.getArguments().get(0) instanceof SQLAllColumnExpr) {
                    opcode = Engine.AggregateOpcodeG4.AggregateCountStar;
                }
            }

            if (aggrExpr.getOption() == SQLAggregateOption.DISTINCT) {
                if (opcode == Engine.AggregateOpcodeG4.AggregateCount) {
                    opcode = Engine.AggregateOpcodeG4.AggregateCountDistinct;
                } else if (opcode == Engine.AggregateOpcodeG4.AggregateSum) {
                    opcode = Engine.AggregateOpcodeG4.AggregateSumDistinct;
                }
            }

            String alias = aliasedExpr.getAlias();
            if (alias == null) {
                if (aliasedExpr.getExpr() instanceof SQLIdentifierExpr) {
                    alias = ((SQLIdentifierExpr) aliasedExpr.getExpr()).getName();
                } else if (aliasedExpr.getExpr() instanceof SQLPropertyExpr) {
                    alias = ((SQLPropertyExpr) aliasedExpr.getExpr()).getName();
                } else {
                    alias = aliasedExpr.getExpr().toString();
                }
            }

            Aggr addAggr = new Aggr(aliasedExpr, opcode, alias, idxCopy);
            if (aliasedExpr.getExpr() instanceof SQLAggregateExpr) {
                SQLAggregateExpr sqlAggregateExpr = (SQLAggregateExpr) aliasedExpr.getExpr();
                addAggr.setFunc(sqlAggregateExpr);

                if (sqlAggregateExpr.getOption() == SQLAggregateOption.DISTINCT) {
                    addAggr.setDistinct(true);
                }
            }
            out.add(addAggr);
        }
        return out;
    }

    private boolean isExprInGroupByExprs(SelectExpr expr) {
        for (GroupBy groupByExpr : this.groupByExprs) {
            SQLExpr exp = expr.getExpr();
            if (exp == null) {
                return false;
            }
            if (SQLExprUtils.equals(groupByExpr.weightStrExpr, exp)) {
                return true;
            }
        }
        return false;
    }

    private boolean isOrderByExprInGroupBy(OrderBy order) {
        // ORDER BY NULL or Aggregation functions need not be present in group by
        boolean isAggregate = order.weightStrExpr instanceof SQLAggregateExpr;
        if (order.inner.getExpr() instanceof SQLNullExpr || isAggregate) {
            return true;
        }
        for (GroupBy groupByExpr : this.groupByExprs) {
            if (SQLExprUtils.equals(groupByExpr.weightStrExpr, order.weightStrExpr)) {
                return true;
            }
        }
        return false;
    }

    public void sortGrouping(List<GroupBy> newGrouping) {
        newGrouping.sort((o1, o2) -> {
            if (o1.getInnerIndex() == null) {
                return 1;
            } else if (o2.getInnerIndex() == null) {
                return -1;
            }
            return o1.getInnerIndex() - o2.getInnerIndex();
        });
    }

    public void sortAggregations(List<Aggr> aggregations) {
        aggregations.sort((o1, o2) -> {
            if (o1.getIndex() == null) {
                return 1;
            } else if (o2.getIndex() == null) {
                return -1;
            }
            return o1.getIndex() - o2.getIndex();
        });
    }

    private void addSelectExpressions(MySqlSelectQueryBlock sel) throws SQLException {
        for (SQLSelectItem item : sel.getSelectList()) { // same as PrimitiveBuilder.pushSelectRoutes
            SqlParser.SelectExpr selectExpr = SqlParser.SelectExpr.type(item);
            if (SqlParser.SelectExpr.AliasedExpr.equals(selectExpr)) {
                // checkForInvalidAggregations
                SelectExpr col = new SelectExpr(item);
                if (item.getExpr() instanceof SQLAggregateExpr) {
                    this.hasAggr = true;
                    col.setAggr(true);
                }
                this.selectExprs.add(col);
            } else if (SqlParser.SelectExpr.StarExpr.equals(selectExpr)) {
                this.hasStar = true;
                SelectExpr col = new SelectExpr(item);
                this.selectExprs.add(col);
            } else {
                throw new SQLException("BUG: unexpected select expression type: " + item);
            }
        }
    }

    /**
     * FindSelectExprIndexForExpr returns the index of the given expression in the select expressions, if it is part of it
     * returns null otherwise.
     *
     * @param expr
     * @return Pair<Integer, SQLSelectItem>
     */
    private Pair<Integer, SQLSelectItem> findSelectExprIndexForExpr(SQLExpr expr) {
        boolean isCol = expr instanceof SQLName;
        for (int idx = 0; idx < this.selectExprs.size(); idx++) {
            SQLSelectItem col = selectExprs.get(idx).getCol();
            SqlParser.SelectExpr selectExpr = SqlParser.SelectExpr.type(col);
            boolean isAliasedExpr = SqlParser.SelectExpr.AliasedExpr.equals(selectExpr);
            if (!isAliasedExpr) {
                continue;
            }
            if (isCol) {
                boolean isAliasExpr = StringUtils.isNotEmpty(col.getAlias());
                if (isAliasExpr && ((SQLName) expr).getSimpleName().equals(col.getAlias())) {
                    return new ImmutablePair<>(idx, col);
                }
            }

            if (SQLExprUtils.equals(col.getExpr(), expr)) {
                return new ImmutablePair<>(idx, col);
            }
        }
        return ImmutablePair.nullPair();
    }

    /**
     * takes an expression used in ORDER BY or GROUP BY, and returns an expression that is simpler to evaluate
     * <p>
     * If the ORDER BY is against a column alias, we need to remember the expression
     * behind the alias. The weightstring(.) calls needs to be done against that expression and not the alias.
     * Eg - select music.foo as bar, weightstring(music.foo) from music order by bar
     *
     * @param e
     * @return
     */
    public SQLExpr[] getSimplifiedExpr(SQLExpr e) {
        SQLExpr[] ret = new SQLExpr[2];
        ret[0] = e;
        ret[1] = e;
        boolean isColName = e instanceof SQLName;
        if (!isColName) {
            return ret;
        }
        if (e instanceof SQLNullExpr) {
            ret[0] = e;
            ret[1] = null;
            return ret;
        }

        if (e instanceof SQLIdentifierExpr) {
            for (SelectExpr expr : this.selectExprs) {
                SqlParser.SelectExpr selectExpr = SqlParser.SelectExpr.type(expr.getCol());
                boolean isAliasedExpr = SqlParser.SelectExpr.AliasedExpr.equals(selectExpr);
                if (!isAliasedExpr) {
                    continue;
                }
                boolean isAliasExpr = StringUtils.isNotEmpty(expr.getCol().getAlias());
                if (isAliasExpr && ((SQLName) e).getSimpleName().equals(expr.getCol().getAlias())) {
                    ret[1] = expr.getCol().getExpr();
                    return ret;
                }
            }
        }

        return ret;
    }

    private void addOrderBy(SQLOrderBy orderBy) {
        boolean canPushDownSorting = true;
        if (orderBy == null) {
            this.canPushDownSorting = canPushDownSorting;
            return;
        }
        for (SQLSelectOrderByItem item : orderBy.getItems()) {
            SQLExpr[] sqlExprs = this.getSimplifiedExpr(item.getExpr());
            if (sqlExprs[1] instanceof SQLNullExpr) {
                continue;
            }
            SQLSelectOrderByItem newItem = new SQLSelectOrderByItem(sqlExprs[0]);
            newItem.setType(item.getType());
            this.orderExprs.add(new OrderBy(newItem, sqlExprs[1]));
            canPushDownSorting = canPushDownSorting && (!(sqlExprs[1] instanceof SQLAggregateExpr));
        }
        this.canPushDownSorting = canPushDownSorting;
    }

    private void checkForInvalidGroupingExpressions(SQLExpr expr) throws SQLException {
        CheckForInvalidGroupingExpressionsVisitor visitor = new CheckForInvalidGroupingExpressionsVisitor();
        expr.accept(visitor);
        if (visitor.getException() != null) {
            throw visitor.getException();
        }
    }
}
