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

package com.jd.jdbc.engine;

import com.jd.jdbc.IExecute;
import com.jd.jdbc.IExecute.ExecuteMultiShardResponse;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.visitor.VtVisitor;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.srvtopo.Resolver;
import com.jd.jdbc.vindexes.hash.BinaryHash;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;

import static com.jd.jdbc.common.Constant.DRIVER_PROPERTY_ROLE_KEY;

public abstract class BasePrimitiveEngine implements PrimitiveEngine {
    String keyspaceName;

    String tableName;

    String vindex;

    SQLStatement stmt;

    BinaryHash hs = new BinaryHash();

    VtVisitor visitor;

    public BasePrimitiveEngine(String keyspaceName, String tableName, String vindex, SQLStatement stmt) {
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
        this.vindex = vindex;
        this.stmt = stmt;
        visitor = new VtVisitor(vindex);
        stmt.accept(visitor);
    }

    @Override
    public String getKeyspaceName() {
        return keyspaceName;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public ExecuteMultiShardResponse execute(IContext ctx, final Vcursor cursor, final Map<String, Query.BindVariable> bindVariableMap, final boolean wantFields) throws SQLException {

        List<SQLExpr> routeValues = buildDestinationExpr();
        if (routeValues != null) {
            List<Destination> destinations = getDestination(routeValues);

            Resolver.ResolveDestinationResult resolveRet = cursor.resolveDestinations(keyspaceName, null, destinations);

            List<Query.BoundQuery> queries = Collections.nCopies(resolveRet.getResolvedShards().size(), Query.BoundQuery.newBuilder().setSql(stmt.toString()).build());
            boolean autocommit = resolveRet.getResolvedShards().size() == 1 && cursor.autocommitApproval();
            return cursor.executeMultiShard(resolveRet.getResolvedShards(), queries, true, autocommit);
        }

        Resolver.AllShardResult resolveRet = cursor.getAllShards(keyspaceName, (Topodata.TabletType) ctx.getContextValue(DRIVER_PROPERTY_ROLE_KEY));
        List<Query.BoundQuery> queries = Collections.nCopies(resolveRet.getResolvedShardList().size(), Query.BoundQuery.newBuilder().setSql(stmt.toString()).build());

        return cursor.executeMultiShard(resolveRet.getResolvedShardList(), queries, true, false);
    }

    @Override
    public IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, Query.BindVariable> bindVariableMap) throws SQLException {
        List<SQLExpr> routeValues = buildDestinationExpr();
        if (routeValues != null) {
            List<Destination> destinations = getDestination(routeValues);
            Resolver.ResolveDestinationResult resolveRet = vcursor.resolveDestinations(keyspaceName, null, destinations);
            List<Query.BoundQuery> queries = Collections.nCopies(resolveRet.getResolvedShards().size(), Query.BoundQuery.newBuilder().setSql(stmt.toString()).build());
            return new IExecute.ResolvedShardQuery(resolveRet.getResolvedShards(), queries);
        }

        Resolver.AllShardResult resolveRet = vcursor.getAllShards(keyspaceName, (Topodata.TabletType) ctx.getContextValue(DRIVER_PROPERTY_ROLE_KEY));
        List<Query.BoundQuery> queries = Collections.nCopies(resolveRet.getResolvedShardList().size(), Query.BoundQuery.newBuilder().setSql(stmt.toString()).build());
        return new IExecute.ResolvedShardQuery(resolveRet.getResolvedShardList(), queries);
    }

    List<SQLExpr> buildDestinationExpr() {
        return visitor.getRouteValues();
    }

    /**
     * @return
     */
    @Override
    public Boolean needsTransaction() {
        return true;
    }

    List<Destination> getDestination(final List<SQLExpr> columnValues) throws SQLException {
        VtValue[] values = new VtValue[columnValues.size()];
        for (int i = 0; i < values.length; i++) {
            values[i] = VtValue.newVtValue(columnValues.get(i));
        }
        return Arrays.asList(hs.map(values));
    }

    protected IdsDestinations getIdsDestinations(List<SQLExpr> columnValues) throws SQLException {
        if (null != columnValues && !columnValues.isEmpty()) {
            VtValue[] values = new VtValue[columnValues.size()];
            Query.Value[] ids = new Query.Value[columnValues.size()];

            for (int i = 0; i < columnValues.size(); i++) {
                ids[i] = VtValue.newVtValue(Query.Type.INT32, String.valueOf(i).getBytes()).toQueryValue();
                values[i] = VtValue.newVtValue(columnValues.get(i));
            }
            return new IdsDestinations(hs.map(values), ids);
        }
        return null;
    }

    @Data
    @AllArgsConstructor
    protected static class IdsDestinations {
        Destination[] destinations;

        Query.Value[] ids;
    }
}
