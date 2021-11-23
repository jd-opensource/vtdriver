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

import com.google.common.collect.Lists;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.Engine.AggregateOpcode;
import com.jd.jdbc.engine.OrderedAggregateEngine;
import com.jd.jdbc.engine.OrderedAggregateEngine.AggregateParams;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.SQLOrderBy;
import com.jd.jdbc.sqlparser.ast.SQLOrderingSpecification;
import com.jd.jdbc.sqlparser.ast.expr.SQLAggregateExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLAggregateOption;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLLiteralExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNullExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectGroupByClause;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectOrderByItem;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqltypes.VtType;
import com.jd.jdbc.vindexes.hash.BinaryHash;
import io.netty.util.internal.StringUtil;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import static com.jd.jdbc.engine.Engine.AggregateOpcode.AggregateCount;
import static com.jd.jdbc.engine.Engine.AggregateOpcode.AggregateCountDistinct;
import static com.jd.jdbc.engine.Engine.AggregateOpcode.AggregateSum;
import static com.jd.jdbc.engine.Engine.AggregateOpcode.AggregateSumDistinct;

public class OrderedAggregatePlan extends ResultsBuilder implements Builder {

    private final OrderedAggregateEngine eaggr;

    private SQLName extraDistinct;

    public OrderedAggregatePlan(Builder input, Truncater truncater, OrderedAggregateEngine eaggr) {
        super(input, truncater);
        this.eaggr = eaggr;
    }


    /**
     * PushFilter satisfies the builder interface.
     *
     * @param pb
     * @param filter
     * @param whereType
     * @param origin
     * @throws SQLException
     */
    @Override
    public void pushFilter(PrimitiveBuilder pb, SQLExpr filter, String whereType, Builder origin) throws SQLException {
        throw new SQLException("unsupported: filtering on results of aggregates");
    }

    /**
     * PushSelect satisfies the builder interface.
     * oa can accept expressions that are normal (a+b), or aggregate (MAX(v)).
     * Normal expressions are pushed through to the underlying route. But aggregate
     * expressions require post-processing. In such cases, oa shares the work with
     * the underlying route: It asks the scatter route to perform the MAX operation
     * also, and only performs the final aggregation with what the route returns.
     * Since the results are expected to be ordered, this is something that can
     * be performed 'as they come'. In this respect, oa is the originator for
     * aggregate expressions like MAX, which will be added to symtab. The underlying
     * MAX sent to the route will not be added to symtab and will not be reachable by
     * others. This functionality depends on the PushOrderBy to request that
     * the rows be correctly ordered.
     *
     * @param pb
     * @param expr
     * @param origin
     * @return
     * @throws SQLException
     */
    @Override
    public PushSelectResponse pushSelect(PrimitiveBuilder pb, SQLSelectItem expr, Builder origin) throws SQLException {
        if (expr.getExpr() instanceof SQLAggregateExpr) {
            SQLAggregateExpr aggrExpr = (SQLAggregateExpr) expr.getExpr();
            if (Engine.supportedAggregates.containsKey(aggrExpr.getMethodName().toLowerCase())) {
                PushAggrResponse pushAggrResponse = this.pushAggr(pb, expr, origin);
                return new PushSelectResponse(pushAggrResponse.resultColumn, pushAggrResponse.colNumber);
            }
        }
        if (PlanBuilder.selectItemsHasAggregates(Lists.newArrayList(expr))) {
            throw new SQLFeatureNotSupportedException("unsupported: in scatter query: complex aggregate expression");
        }
        PushSelectResponse pushSelectResponse = this.getBldr().pushSelect(pb, expr, origin);
        ResultColumn innerRc = pushSelectResponse.getResultColumn();
        this.getResultColumnList().add(innerRc);
        return new PushSelectResponse(innerRc, this.resultColumns().size() - 1);
    }

    @Override
    public void makeDistinct() throws SQLException {
        for (int i = 0; i < this.getResultColumnList().size(); i++) {
            ResultColumn rc = this.getResultColumnList().get(i);
            if (rc.getColumn().origin() == this) {
                throw new SQLException("unsupported: distinct cannot be combined with aggregate functions");
            }
            this.eaggr.getKeyList().add(i);
        }
        this.getBldr().makeDistinct();
    }

    @Override
    public void pushGroupBy(SQLSelectGroupByClause groupBy) throws SQLException {
        if (groupBy != null) {
            int colNumber = -1;
            for (SQLExpr node : groupBy.getItems()) {
                if (node instanceof SQLIdentifierExpr || node instanceof SQLPropertyExpr) {
                    Column column = ((SQLName) node).getMetadata();
                    if (column.origin() == this) {
                        throw new SQLException("group by expression cannot reference an aggregate function: " + node);
                    }
                    for (int i = 0; i < this.resultColumns().size(); i++) {
                        ResultColumn resultColumn = this.resultColumns().get(i);
                        if (resultColumn.getColumn() == column) {
                            colNumber = i;
                            break;
                        }
                    }
                    if (colNumber == -1) {
                        throw new SQLException("unsupported: in scatter query: group by column must reference column in SELECT list");
                    }
                } else if (node instanceof SQLLiteralExpr) {
                    colNumber = PlanBuilder.resultFromNumber(this.getResultColumnList(), (SQLLiteralExpr) node);
                } else {
                    throw new SQLException("unsupported: in scatter query: only simple references allowed");
                }
                this.eaggr.getKeyList().add(colNumber);
            }
            // Append the distinct aggregate if any.
            if (this.extraDistinct != null) {
                groupBy.addItem(this.extraDistinct);
            }
        } else {
            // Append the distinct aggregate if any.
            if (this.extraDistinct != null) {
                groupBy = new SQLSelectGroupByClause();
                groupBy.addItem(this.extraDistinct);
            }
        }
        this.getBldr().pushGroupBy(groupBy);
    }

    /**
     * PushOrderBy pushes the order by expression into the primitive.
     * The requested order must be such that the ordering can be done
     * before the group by, which will allow us to push it down to the
     * route. This is actually true in most use cases, except for situations
     * where ordering is requested on values of an aggregate result.
     * Such constructs will need to be handled by a separate 'Sorter'
     * primitive, after aggregation is done. For example, the following
     * constructs are allowed:
     * 'select a, b, count(*) from t group by a, b order by a desc, b asc'
     * 'select a, b, count(*) from t group by a, b order by b'
     * The following construct is not allowed:
     * 'select a, count(*) from t group by a order by count(*)'
     *
     * @param orderBy
     * @return
     * @throws SQLException
     */
    @Override
    public Builder pushOrderBy(SQLOrderBy orderBy) throws SQLException {
        // Treat order by null as nil order by.
        if (orderBy != null && orderBy.getItems() != null && orderBy.getItems().size() == 1) {
            SQLSelectOrderByItem orderByItem = orderBy.getItems().get(0);
            if (orderByItem.getExpr() instanceof SQLNullExpr) {
                orderBy = null;
            }
        }

        // referenced tracks the keys referenced by the order by clause.
        Boolean[] referenced = new Boolean[this.eaggr.getKeyList().size()];
        Arrays.fill(referenced, false);
        boolean postSort = false;
        SQLOrderBy selfOrderBy = new SQLOrderBy();
        if (orderBy != null && orderBy.getItems() != null) {
            for (SQLSelectOrderByItem orderByItem : orderBy.getItems()) {
                SQLExpr orderByExpr = orderByItem.getExpr();
                // Identify the order by column.
                Column orderByCol;
                if (orderByExpr instanceof SQLLiteralExpr) {
                    int num = PlanBuilder.resultFromNumber(this.getResultColumnList(), (SQLLiteralExpr) orderByExpr);
                    orderByCol = this.getResultColumnList().get(num).getColumn();
                } else if (orderByExpr instanceof SQLName) {
                    orderByCol = ((SQLName) orderByExpr).getMetadata();
                } else {
                    throw new SQLException("unsupported: in scatter query: complex order by expression: " + orderByExpr.toString());
                }

                // Match orderByCol against the group by columns.
                boolean found = false;
                for (int j = 0; j < this.eaggr.getKeyList().size(); j++) {
                    Integer key = this.eaggr.getKeyList().get(j);
                    if (this.getResultColumnList().get(key).getColumn() != orderByCol) {
                        continue;
                    }

                    found = true;
                    referenced[j] = true;
                    if (orderByItem.getType() == null) {
                        orderByItem.setType(SQLOrderingSpecification.ASC);
                    }
                    selfOrderBy.addItem(orderByItem);
                    break;
                }
                if (!found) {
                    postSort = true;
                }
            }
        }

        // Append any unreferenced keys at the end of the order by.
        for (int i = 0; i < this.eaggr.getKeyList().size(); i++) {
            Integer key = this.eaggr.getKeyList().get(i);
            if (referenced[i]) {
                continue;
            }
            // Build a brand new reference for the key.
            SQLName col;
            try {
                col = Symtab.buildColName(this.getBldr().resultColumns(), key);
            } catch (SQLException e) {
                throw new SQLException("generating order by clause: " + e.getMessage());
            }
            SQLSelectOrderByItem orderByItem = new SQLSelectOrderByItem(col);
            orderByItem.setType(SQLOrderingSpecification.ASC);
            selfOrderBy.addItem(orderByItem);
        }

        // Append the distinct aggregate if any.
        if (this.extraDistinct != null) {
            SQLSelectOrderByItem orderByItem = new SQLSelectOrderByItem(this.extraDistinct);
            orderByItem.setType(SQLOrderingSpecification.ASC);
            selfOrderBy.addItem(orderByItem);
        }

        // Push down the order by.
        // It's ok to push the original AST down because all references
        // should point to the route. Only aggregate functions are originated
        // by oa, and we currently don't allow the ORDER BY to reference them.
        Builder bldr = this.getBldr().pushOrderBy(selfOrderBy);
        this.setBldr(bldr);
        if (postSort) {
            return MemorySortPlan.newMemorySortPlan(this, orderBy);
        }
        return this;
    }

    @Override
    public void setUpperLimit(SQLExpr count) throws SQLException {
        this.getBldr().setUpperLimit(count);
    }

    /**
     * pushes miscelleaneous constructs to all the primitives.
     *
     * @param sel
     */
    @Override
    public void pushMisc(MySqlSelectQueryBlock sel) {
        this.getBldr().pushMisc(sel);
    }

    @Override
    public void wireup(Builder bldr, Jointab jt) throws SQLException {
        for (int i = 0; i < this.eaggr.getKeyList().size(); i++) {
            Integer colNumber = this.eaggr.getKeyList().get(i);
            ResultColumn rc = this.getResultColumnList().get(colNumber);
            if (VtType.isText(rc.getColumn().getType())) {
                Map<ResultColumn, Integer> weightStrings = this.getWeightStrings();
                Integer weightColNumber;
                if (weightStrings.containsKey(rc)) {
                    this.eaggr.getKeyList().set(i, weightStrings.get(rc));
                    continue;
                }
                weightColNumber = this.getBldr().supplyWeightString(colNumber);
                this.getWeightStrings().put(rc, weightColNumber);
                this.eaggr.getKeyList().set(i, weightColNumber);
                this.eaggr.setTruncateColumnCount(this.getResultColumnList().size());
            }
        }
        this.getBldr().wireup(bldr, jt);
    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        this.eaggr.setInput(this.getBldr().getPrimitiveEngine());
        return this.eaggr;
    }

    /**
     * @param pb
     * @param expr
     * @param origin
     * @return
     * @throws SQLException
     */
    private PushAggrResponse pushAggr(PrimitiveBuilder pb, SQLSelectItem expr, Builder origin) throws SQLException {
        SQLAggregateExpr aggrExpr = (SQLAggregateExpr) expr.getExpr();
        AggregateOpcode opcode = Engine.supportedAggregates.get(aggrExpr.getMethodName().toLowerCase());
        if (aggrExpr.getArguments().size() != 1) {
            throw new SQLException("unsupported: only one expression allowed inside aggregates: " + aggrExpr);
        }
        NeedDistinctHandlingResponse needDistinctHandlingResponse = this.needDistinctHandling(pb, aggrExpr, opcode);
        Boolean handleDistinct = needDistinctHandlingResponse.getHandleDistinct();
        if (handleDistinct) {
            if (this.extraDistinct != null) {
                throw new SQLException("unsupported: only one distinct aggregation allowed in a select: " + aggrExpr);
            }
            // Push the expression that's inside the aggregate.
            // The column will eventually get added to the group by and order by clauses.
            PushSelectResponse pushSelectResponse = this.getBldr().pushSelect(pb, needDistinctHandlingResponse.getInnerAliased(), origin);
            Integer innerCol = pushSelectResponse.getColNumber();
            this.extraDistinct = Symtab.buildColName(this.getBldr().resultColumns(), innerCol);
            this.eaggr.setHasDistinct(true);
            String alias;
            if (StringUtil.isNullOrEmpty(expr.getAlias())) {
                alias = SQLUtils.toSQLString(expr.getExpr());
            } else {
                alias = expr.getAlias();
            }
            switch (opcode) {
                case AggregateCount:
                    opcode = AggregateCountDistinct;
                    break;
                case AggregateSum:
                    opcode = AggregateSumDistinct;
                    break;
            }
            this.eaggr.getAggregateParamsList().add(new AggregateParams(opcode, innerCol, alias));
        } else {
            PushSelectResponse pushSelectResponse = this.getBldr().pushSelect(pb, expr, origin);
            Integer innerCol = pushSelectResponse.getColNumber();
            this.eaggr.getAggregateParamsList().add(new AggregateParams(opcode, innerCol));
        }

        // Build a new rc with oa as origin because it's semantically different
        // from the expression we pushed down.
        ResultColumn resultColumn = Symtab.newResultColumn(expr, this);
        this.resultColumns().add(resultColumn);
        return new PushAggrResponse(resultColumn, this.resultColumns().size() - 1);
    }

    /**
     * needDistinctHandling returns true if oa needs to handle the distinct clause.
     * If true, it will also return the aliased expression that needs to be pushed
     * down into the underlying route.
     *
     * @param pb
     * @param aggrExpr
     * @param opcode
     * @return
     */
    private NeedDistinctHandlingResponse needDistinctHandling(PrimitiveBuilder pb, SQLAggregateExpr aggrExpr, AggregateOpcode opcode) throws SQLException {
        if (aggrExpr.getOption() != SQLAggregateOption.DISTINCT) {
            return new NeedDistinctHandlingResponse(false, null);
        }
        if (opcode != AggregateCount && opcode != AggregateSum) {
            return new NeedDistinctHandlingResponse(false, null);
        }
        SQLExpr expr = aggrExpr.getArguments().get(0);
        if (!(expr instanceof SQLName)) {
            throw new SQLException("syntax error: " + aggrExpr);
        }
        if (!(this.getBldr() instanceof RoutePlan)) {
            // Unreachable
            return new NeedDistinctHandlingResponse(true, new SQLSelectItem(expr, ((SQLName) expr).getSimpleName()));
        }
        BinaryHash vindex = pb.getSymtab().getColumnVindex(expr, (RoutePlan) this.getBldr());
        if (vindex != null && vindex.isUnique()) {
            return new NeedDistinctHandlingResponse(false, null);
        }
        return new NeedDistinctHandlingResponse(true, new SQLSelectItem(expr));
    }

    @Override
    public String toString() {
        return "OrderedAggregatePlan{" + "extraDistinct=" + extraDistinct + '}';
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public static class PushAggrResponse {
        private ResultColumn resultColumn;

        private Integer colNumber;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    private static class NeedDistinctHandlingResponse {
        private Boolean handleDistinct;

        private SQLSelectItem innerAliased;
    }
}
