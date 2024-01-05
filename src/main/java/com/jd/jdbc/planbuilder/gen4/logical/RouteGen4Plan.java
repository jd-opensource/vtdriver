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

package com.jd.jdbc.planbuilder.gen4.logical;

import com.jd.jdbc.context.PlanningContext;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.gen4.RouteGen4Engine;
import com.jd.jdbc.engine.gen4.VindexLookup;
import com.jd.jdbc.planbuilder.gen4.Gen4Planner;
import com.jd.jdbc.planbuilder.semantics.TableSet;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLLimit;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQuery;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtFormatImpossibleQueryVisitor;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRouteWireupFixUpAstVisitor;
import com.jd.jdbc.vindexes.LookupPlanable;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

public class RouteGen4Plan extends AbstractGen4Plan {

    // Select is the AST for the query fragment that will be
    // executed by this route.
    @Getter
    @Setter
    public SQLSelectQuery select;

    // condition stores the AST condition that will be used
    // to resolve the ERoute Values field.
    @Getter
    public SQLExpr condition;

    // eroute is the primitive being built.
    @Getter
    public RouteGen4Engine eroute;

    // is the engine primitive we will return from the Primitive() method. Note that it could be different than eroute
    public PrimitiveEngine enginePrimitive;

    // tables keeps track of which tables this route is covering
    public TableSet tables;

    public RouteGen4Plan() {
    }

    public RouteGen4Plan(RouteGen4Engine eroute, SQLSelectQuery sel, TableSet tables, SQLExpr condition) {
        this.eroute = eroute;
        this.select = sel;
        this.tables = tables;
        this.condition = condition;
    }

    @Override
    public void wireupGen4(PlanningContext ctx) throws SQLException {
        // this.eroute.setQuery(SQLUtils.toMySqlString(this.select, SQLUtils.NOT_FORMAT_OPTION).trim());
        // this.enginePrimitive = this.eroute;

        // todo

        this.prepareTheAST();

        this.eroute.setQuery(SQLUtils.toMySqlString(this.select, SQLUtils.NOT_FORMAT_OPTION).trim());

        SQLSelectQuery selectClone = this.select.clone();
        VtFormatImpossibleQueryVisitor formatImpossibleQueryVisitor = new VtFormatImpossibleQueryVisitor();
        selectClone.accept(formatImpossibleQueryVisitor);
        this.eroute.setFieldQuery(SQLUtils.toMySqlString(selectClone, SQLUtils.NOT_FORMAT_OPTION).trim());
        this.eroute.setSelectFieldQuery(selectClone);

        if (!(this.eroute.getRoutingParameters().getVindex() instanceof LookupPlanable)) {
            this.enginePrimitive = this.eroute;
            return;
        }

        LookupPlanable planableVindex = (LookupPlanable) this.eroute.getRoutingParameters().getVindex();
        String[] querys = planableVindex.Query();
        String query = querys[0];
        String[] queryArgs = Arrays.copyOfRange(querys, 1, querys.length);

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(query);

        PrimitiveEngine lookupPrimitive = Gen4Planner.gen4SelectStmtPlanner(query, ctx.getVschema().getDefaultKeyspace(), (SQLSelectStatement) stmt, null, ctx.getVschema().getVSchemaManager());

        this.enginePrimitive = new VindexLookup(
            this.eroute.getRoutingParameters().getRouteOpcode(),
            planableVindex,
            this.eroute.getRoutingParameters().getKeyspace(),
            this.eroute.getRoutingParameters().getValues(),
            this.eroute,
            Arrays.asList(queryArgs),
            lookupPrimitive
        );

        this.eroute.getRoutingParameters().setRouteOpcode(Engine.RouteOpcode.ByDestination);
        this.eroute.getRoutingParameters().setValues(null);
        this.eroute.getRoutingParameters().setVindex(null);

        //throw new SQLFeatureNotSupportedException("unsupported vindexloopup");

    }

    @Override
    public PrimitiveEngine getPrimitiveEngine() throws SQLException {
        return enginePrimitive;
    }

    @Override
    public LogicalPlan[] inputs() throws SQLException {
        return new LogicalPlan[0];
    }

    @Override
    public LogicalPlan[] rewrite(LogicalPlan... inputs) throws SQLException {
        if (inputs.length != 0) {
            throw new SQLException("[RouteGen4]route: wrong number of inputs");
        }
        return null;
    }

    @Override
    public TableSet containsTables() {
        return tables;
    }

    @Override
    public List<SQLSelectItem> outputColumns() throws SQLException {
        if (this.select instanceof MySqlSelectQueryBlock) {
            MySqlSelectQueryBlock mySqlSelectQueryBlock = (MySqlSelectQueryBlock) this.select;
            return mySqlSelectQueryBlock.getSelectList();
        } else {
            throw new SQLFeatureNotSupportedException();
        }
    }


    // ========== ================

    public void setLimit(SQLLimit limit) {
        if (this.select instanceof MySqlSelectQueryBlock) {
            ((MySqlSelectQueryBlock) this.select).setLimit(limit);
        } else if (this.select instanceof SQLUnionQuery) {
            ((SQLUnionQuery) this.select).setLimit(limit);
        }
    }

    @Override
    public void setUpperLimit(SQLExpr count) {
        ((MySqlSelectQueryBlock) this.select).setLimit(new SQLLimit(count));
    }

    // public Boolean isLocal()

    public boolean isSingleShard() {
        switch (this.eroute.getRoutingParameters().getRouteOpcode()) {
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

    // prepareTheAST does minor fixups of the SELECT struct before producing the query string
    public void prepareTheAST() {
        VtRouteWireupFixUpAstVisitor fixUpAstVisitor = new VtRouteWireupFixUpAstVisitor(null);
        this.select.accept(fixUpAstVisitor);
    }
}
