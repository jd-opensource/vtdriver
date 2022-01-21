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
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.srvtopo.BindVariable;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Map;

/**
 * PrimitiveEngine is the building block of the engine execution plan. They form a tree structure, where the leaves typically
 * issue queries to one or more vttablet.
 * During execution, the Primitive's pass Result objects up the tree structure, until reaching the root,
 * and its result is passed to the client.
 */
public interface PrimitiveEngine {
    /**
     * @return
     */
    String getKeyspaceName();

    /**
     * @return
     */
    String getTableName();

    /**
     * @param ctx
     * @param vcursor
     * @param bindVariableMap
     * @param wantFields
     * @return
     * @throws Exception
     */
    ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException;

    /**
     * @param ctx
     * @param vcursor
     * @param wantFields
     * @return
     * @throws Exception
     */
    default List<ExecuteMultiShardResponse> batchExecute(IContext ctx, Vcursor vcursor, boolean wantFields) throws SQLException {
        throw new SQLFeatureNotSupportedException("unsupported multiquery");
    }

    /**
     * @param vtResultSet
     * @param bindVariable
     * @param wantFields
     * @return
     * @throws Exception
     */
    default ExecuteMultiShardResponse mergeResult(VtResultSet vtResultSet, Map<String, BindVariable> bindVariable, boolean wantFields) throws SQLException {
        throw new SQLFeatureNotSupportedException("unsupported multiquery");
    }

    /**
     * @param ctx
     * @param vcursor
     * @param bindValue
     * @return
     * @throws Exception
     */
    default IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindValue) throws SQLException {
        throw new SQLFeatureNotSupportedException("unsupported multiquery");
    }

    /**
     * @param ctx
     * @param vcursor
     * @param bindValue
     * @return
     * @throws Exception
     */
    default IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindValue, Map<String, String> switchTableMap) throws SQLException {
        throw new SQLFeatureNotSupportedException("unsupported multiquery");
    }

    default Boolean canResolveShardQuery() {
        return Boolean.FALSE;
    }

    /**
     * @param vcursor
     * @param bindValue
     * @param wantFields
     * @return
     * @throws Exception
     */
    default IExecute.VtStream streamExecute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindValue, boolean wantFields) throws SQLException {
        throw new SQLFeatureNotSupportedException("unsupported streamExecute");
    }

    /**
     * @param vcursor
     * @param bindVariableMap
     * @return
     */
    default VtResultSet getFields(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        throw new SQLFeatureNotSupportedException("unsupported getFields");
    }

    /**
     * @return
     */
    Boolean needsTransaction();

    default List<PrimitiveEngine> inputs() {
        return null;
    }
}
