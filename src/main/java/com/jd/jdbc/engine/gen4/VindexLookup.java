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

package com.jd.jdbc.engine.gen4;

import com.jd.jdbc.IExecute;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.engine.Engine;
import com.jd.jdbc.engine.PrimitiveEngine;
import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.evalengine.EvalResult;
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.sqltypes.SqlTypes;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.vindexes.LookupPlanable;
import com.jd.jdbc.vindexes.VKeyspace;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VindexLookup implements PrimitiveEngine {

    Engine.RouteOpcode opcode;

    /**
     * The vindex to use to do the Map
     */
    LookupPlanable vindex;

    /**
     * Keyspace specifies the keyspace to send the query to.
     */
    VKeyspace keyspace;

    List<String> arguments;

    /**
     * Values specifies the vindex values to use for routing.
     */
    List<EvalEngine.Expr> values;

    /**
     * We fetch data in order to do the map from this primitive
     */
    PrimitiveEngine lookup;

    /**
     * This is the side that needs to be routed
     */
    RouteGen4Engine sendTo;

    public VindexLookup(Engine.RouteOpcode opcode, LookupPlanable vindex, VKeyspace keyspace,
                        List<EvalEngine.Expr> values, RouteGen4Engine sendTo, List<String> arguments,
                        PrimitiveEngine lookup) {
        this.opcode = opcode;
        this.vindex = vindex;
        this.keyspace = keyspace;
        this.values = values;
        this.sendTo = sendTo;
        this.arguments = arguments;
        this.lookup = lookup;
    }

    @Override
    public String getKeyspaceName() {
        return this.sendTo.getKeyspaceName();
    }

    @Override
    public String getTableName() {
        return this.sendTo.getTableName();
    }

    public VtResultSet getFields(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        return this.sendTo.getFields(vcursor, bindVariableMap);
    }

    @Override
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        VtValue[] ids = this.generateIds(vcursor, bindVariableMap);
        VtResultSet[] results = this.lookUpFunc(ctx, vcursor, ids);
        List<Destination> dest = this.mapVindexToDestination(ids, results, bindVariableMap);

        return this.sendTo.executeAfterLookup(ctx, vcursor, bindVariableMap, wantFields, ids, dest);
    }

    @Override
    public Boolean needsTransaction() {
        return this.sendTo.needsTransaction();
    }

    private VtValue[] generateIds(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        // vcursor.
        EvalEngine.ExpressionEnv env = new EvalEngine.ExpressionEnv(bindVariableMap);
        EvalEngine.Expr expr = this.values.get(0);
        EvalResult value = expr.evaluate(env);
        if (this.opcode == Engine.RouteOpcode.SelectEqual || this.opcode == Engine.RouteOpcode.SelectEqualUnique) {
            return new VtValue[] {value.value()};
        } else if (this.opcode == Engine.RouteOpcode.SelectIN) {
            // return res.resultValue()
            //TODO
        }

        return null;
    }

    private VtResultSet[] lookUpFunc(IContext ctx, Vcursor vcursor, VtValue[] ids) throws SQLException {
        if (ids[0].isIntegral() || this.vindex.AllowBatch()) {
            return this.executeBatch(ctx, vcursor, ids);
        }
        return this.executeNonBatch(ctx, vcursor, ids);
    }

    private VtResultSet[] executeNonBatch(IContext ctx, Vcursor vcursor, VtValue[] ids) throws SQLException {
        ArrayList<VtResultSet> results = new ArrayList<>(ids.length);
        for (VtValue id : ids) {
            Map<String, BindVariable> bindVariableMap = new HashMap<>();
            bindVariableMap.put(this.arguments.get(0), SqlTypes.valueBindVariable(id));
            IExecute.ExecuteMultiShardResponse executeMultiShardResponse = this.lookup.execute(ctx, vcursor, bindVariableMap, false);
            VtResultSet vtResultSet = (VtResultSet) executeMultiShardResponse.getVtRowList();
            results.add(vtResultSet);
        }
        return results.toArray(new VtResultSet[0]);
    }

    private VtResultSet[] executeBatch(IContext ctx, Vcursor vcursor, VtValue[] ids) throws SQLException {
        ArrayList<VtResultSet> results = new ArrayList<>(ids.length);
        Map<String, BindVariable> bindVariableMap = new HashMap<>();

        List<Query.Value> qv = new ArrayList<>(ids.length);

        for (VtValue id : ids) {
            qv.add(id.toQueryValue());
        }
        bindVariableMap.put(this.arguments.get(0), new BindVariable(qv, Query.Type.TUPLE));
        IExecute.ExecuteMultiShardResponse executeMultiShardResponse = this.lookup.execute(ctx, vcursor, bindVariableMap, false);
        VtResultSet vtResultSet = (VtResultSet) executeMultiShardResponse.getVtRowList();
        //TODO 结果处理 多列转多行
        results.add(vtResultSet);
        return results.toArray(new VtResultSet[0]);
    }

    private List<Destination> mapVindexToDestination(VtValue[] ids, VtResultSet[] results, Map<String, BindVariable> bindVariableMap) {
        Destination[] dest = this.vindex.MapResult(ids, results);
        if (this.opcode == Engine.RouteOpcode.SelectIN) {
            List<Query.Value> valuesList = new ArrayList<>(ids.length);
            for (VtValue id : ids) {
                valuesList.add(SqlTypes.vtValueToProto(id));
            }
            BindVariable valsBv = new BindVariable(valuesList, Query.Type.TUPLE);
            bindVariableMap.put(Engine.LIST_VAR_NAME, valsBv);
        }
        return Arrays.asList(dest);
    }
}
