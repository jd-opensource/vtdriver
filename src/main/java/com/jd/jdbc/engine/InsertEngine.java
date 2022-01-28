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

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.jd.jdbc.IExecute;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.evalengine.EvalEngine;
import com.jd.jdbc.key.Bytes;
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.key.DestinationAllShard;
import com.jd.jdbc.key.DestinationAnyShard;
import com.jd.jdbc.key.DestinationKeyspaceID;
import com.jd.jdbc.key.DestinationNone;
import com.jd.jdbc.planbuilder.InsertPlan;
import com.jd.jdbc.queryservice.util.RoleUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLInsertStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlInsertReplaceStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRestoreVisitor;
import com.jd.jdbc.sqltypes.SqlTypes;
import com.jd.jdbc.sqltypes.VtPlanValue;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.BoundQuery;
import com.jd.jdbc.srvtopo.ResolvedShard;
import com.jd.jdbc.srvtopo.Resolver;
import com.jd.jdbc.vindexes.VKeyspace;
import com.jd.jdbc.vindexes.hash.BinaryHash;
import io.netty.util.internal.StringUtil;
import io.vitess.proto.Query;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import vschema.Vschema;

/**
 * Insert represents the instructions to perform an insert operation.
 */
@Getter
@Setter
public class InsertEngine implements PrimitiveEngine {
    private static final SequenceCache SEQUENCE_CACHE = new SequenceCache();

    /**
     * Opcode is the execution opcode.
     */
    private Engine.InsertOpcode insertOpcode;

    /**
     * Keyspace specifies the keyspace to send the query to.
     */
    private VKeyspace keyspace;

    /**
     * TargetDestination specifies the destination to send the query to.
     */
    private Destination targetDestination;

    /**
     * Query specifies the query to be executed.
     * For InsertSharded plans, this value is unused,
     * and Prefix, Mid and Suffix are used instead.
     */
    private String query;

    private MySqlInsertReplaceStatement insertReplaceStmt;

    /**
     * VindexValues specifies values for all the vindex columns.
     * This is a three-dimensional data structure:
     * Insert.Values[i] represents the values to be inserted for the i'th colvindex (i < len(Insert.Table.ColumnVindexes))
     * Insert.Values[i].Values[j] represents values for the j'th column of the given colVindex (j < len(colVindex[i].Columns)
     * Insert.Values[i].Values[j].Values[k] represents the value pulled from row k for that column: (k < len(ins.rows))
     */
    private List<VtPlanValue> vindexValueList;

    /**
     * TableName specifies the table to send the query to.
     */
    private Vschema.Table table;

    private String tableName;

    /**
     * Generate is only set for inserts where a sequence must be generated.
     */
    private Generate generate;

    /**
     * Prefix, Mid and Suffix are for sharded insert plans.
     */
    private String prefix;

    private List<String> midList;

    private List<SQLInsertStatement.ValuesClause> midExprList;

    private String suffix;

    private List<SQLExpr> suffixExpr;

    /**
     * Option to override the standard behavior and allow a multi-shard insert
     * to use single round trip autocommit.
     * <p>
     * This is a clear violation of the SQL semantics since it means the statement
     * is not atomic in the presence of PK conflicts on one shard and not another.
     * However some application use cases would prefer that the statement partially
     * succeed in order to get the performance benefits of autocommit.
     */
    private boolean multiShardAutocommit;

    private long insertId;

    /**
     * creates an Insert for a Table.
     *
     * @param insertOpcode
     * @param keyspace
     * @param table
     */
    public InsertEngine(final Engine.InsertOpcode insertOpcode, final VKeyspace keyspace, final Vschema.Table table, final String tableName) {
        this.insertOpcode = insertOpcode;
        this.keyspace = keyspace;
        this.targetDestination = null;
        this.query = "";
        this.table = table;
        this.tableName = tableName;
        this.generate = null;
        this.prefix = "";
        this.midList = new ArrayList<>();
        this.midExprList = new ArrayList<>();
        this.suffix = "";
        this.multiShardAutocommit = false;
    }

    public boolean getMultiShardAutocommit() {
        return multiShardAutocommit;
    }

    /**
     * GetKeyspaceName specifies the Keyspace that this primitive routes to.
     *
     * @return
     */
    @Override
    public String getKeyspaceName() {
        return this.keyspace.getName();
    }

    /**
     * GetTableName specifies the table that this primitive routes to.
     *
     * @return
     */
    @Override
    public String getTableName() {
        if (!StringUtil.isNullOrEmpty(this.tableName)) {
            return this.tableName;
        }
        return "";
    }

    /**
     * Execute performs a non-streaming exec.
     *
     * @param ctx
     * @param vcursor
     * @param bindVariableMap
     * @param wantFields
     * @return
     * @throws Exception
     */
    @Override
    public IExecute.ExecuteMultiShardResponse execute(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        if (RoleUtils.notMaster(ctx)) {
            throw new SQLException("insert is not allowed for read only connection");
        }
        switch (this.insertOpcode) {
            case InsertUnsharded:
                return this.execInsertUnsharded(vcursor, bindVariableMap);
            case InsertSharded:
            case InsertShardedIgnore:
                return this.execInsertSharded(vcursor, bindVariableMap);
            case InsertByDestination:
                return this.execInsertByDestination(vcursor, bindVariableMap, this.targetDestination);
            default:
                throw new SQLException("unsupported query route: " + this.insertOpcode);
        }
    }

    @Override
    public IExecute.ExecuteMultiShardResponse mergeResult(VtResultSet vtResultSet, Map<String, BindVariable> bindVariableMap, boolean wantFields) throws SQLException {
        if (insertId != 0) {
            vtResultSet.setInsertID(insertId);
        }
        return new IExecute.ExecuteMultiShardResponse(vtResultSet).setUpdate();
    }

    public IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindValues, List<VtPlanValue> exVindexValueList,
                                                         List<SQLInsertStatement.ValuesClause> exMidExprList, String exPrefix, String exSuffix, List<SQLExpr> exSuffixExpr,
                                                         Map<String, String> switchTableMap) throws SQLException {
//        insertId = this.processGenerate(vcursor, bindValues);
        if (Engine.InsertOpcode.InsertUnsharded.equals(this.insertOpcode) || Engine.InsertOpcode.InsertByDestination.equals(this.insertOpcode)) {
            List<ResolvedShard> rsList = getResolvedUnsharded(vcursor);
            Engine.allowOnlyMaster(rsList);
            String charEncoding = vcursor.getCharEncoding();
            List<BoundQuery> queries = getUnshardQueries(bindValues, exMidExprList, exPrefix, exSuffix, exSuffixExpr, switchTableMap, charEncoding);
            return new IExecute.ResolvedShardQuery(rsList, queries);
        }
        if (Engine.InsertOpcode.InsertSharded.equals(this.insertOpcode) || Engine.InsertOpcode.InsertShardedIgnore.equals(this.insertOpcode)) {
            InsertShardedRouteResult insertShardedRouteResult = this.getInsertShardedRoute(vcursor, bindValues, exVindexValueList, exMidExprList, exPrefix, exSuffix, exSuffixExpr, switchTableMap);
            List<ResolvedShard> rss = insertShardedRouteResult.getResolvedShardList();
            List<BoundQuery> queries = insertShardedRouteResult.getBoundQueryList();
            Engine.allowOnlyMaster(rss);
            return new IExecute.ResolvedShardQuery(rss, queries);
        }
        throw new SQLException("unsupported query route: " + this.insertOpcode);
    }

    @Override
    public IExecute.ResolvedShardQuery resolveShardQuery(IContext ctx, Vcursor vcursor, Map<String, BindVariable> bindValues) throws SQLException {
        insertId = this.processGenerate(vcursor, bindValues);
        String charEncoding = vcursor.getCharEncoding();
        if (Engine.InsertOpcode.InsertUnsharded.equals(this.insertOpcode)) {
            List<ResolvedShard> rsList = getResolvedUnsharded(vcursor);
            Engine.allowOnlyMaster(rsList);
            List<BoundQuery> queries = Engine.getQueries(this.insertReplaceStmt, Lists.newArrayList(bindValues), charEncoding);
            return new IExecute.ResolvedShardQuery(rsList, queries);
        }
        if (Engine.InsertOpcode.InsertSharded.equals(this.insertOpcode) || Engine.InsertOpcode.InsertShardedIgnore.equals(this.insertOpcode)) {
            InsertShardedRouteResult insertShardedRouteResult = this.getInsertShardedRoute(vcursor, bindValues);
            List<ResolvedShard> rss = insertShardedRouteResult.getResolvedShardList();
            List<BoundQuery> queries = insertShardedRouteResult.getBoundQueryList();
            Engine.allowOnlyMaster(rss);
            return new IExecute.ResolvedShardQuery(rss, queries);
        }
        if (Engine.InsertOpcode.InsertByDestination.equals(this.insertOpcode)) {
            List<ResolvedShard> rsList = getResolvedDestinationShard(vcursor, this.targetDestination);
            List<BoundQuery> queries = Engine.getQueries(this.insertReplaceStmt, Lists.newArrayList(bindValues), charEncoding);
            return new IExecute.ResolvedShardQuery(rsList, queries);
        }
        throw new SQLException("unsupported query route: " + this.insertOpcode);
    }

    @Override
    public Boolean needsTransaction() {
        return true;
    }

    /**
     * @param vcursor
     * @param bindVariableMap
     * @return
     * @throws Exception
     */
    private IExecute.ExecuteMultiShardResponse execInsertUnsharded(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        long insertId = this.processGenerate(vcursor, bindVariableMap);
        List<ResolvedShard> rsList = getResolvedUnsharded(vcursor);
        Engine.allowOnlyMaster(rsList);
        IExecute.ExecuteMultiShardResponse executeMultiShardResponse = Engine.execShard(vcursor, this.insertReplaceStmt, bindVariableMap, rsList.get(0), true, true).setUpdate();
        VtResultSet vtResultSet = (VtResultSet) executeMultiShardResponse.getVtRowList();

        // If processGenerate generated new values, it supercedes
        // any ids that MySQL might have generated. If both generated
        // values, we don't return an error because this behavior
        // is required to support migration.
        if (insertId != 0) {
            vtResultSet.setInsertID(insertId);
        }
        return new IExecute.ExecuteMultiShardResponse(vtResultSet);
    }

    private IExecute.ExecuteMultiShardResponse execInsertSharded(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        long insertId = this.processGenerate(vcursor, bindVariableMap);
        InsertShardedRouteResult insertShardedRouteResult = this.getInsertShardedRoute(vcursor, bindVariableMap);
        List<ResolvedShard> rss = insertShardedRouteResult.getResolvedShardList();
        List<BoundQuery> queries = insertShardedRouteResult.getBoundQueryList();

        boolean autocommit = (rss.size() == 1 || this.multiShardAutocommit) && vcursor.autocommitApproval();
        Engine.allowOnlyMaster(rss);
        IExecute.ExecuteMultiShardResponse executeMultiShardResponse = vcursor.executeMultiShard(rss, queries, true, autocommit).setUpdate();
        VtResultSet vtResultSet = (VtResultSet) executeMultiShardResponse.getVtRowList();

        if (insertId != 0) {
            vtResultSet.setInsertID(insertId);
        }
        return new IExecute.ExecuteMultiShardResponse(vtResultSet);
    }

    private IExecute.ExecuteMultiShardResponse execInsertByDestination(Vcursor vcursor, Map<String, BindVariable> bindVariableMap, Destination destination) throws SQLException {
        long insertId = this.processGenerate(vcursor, bindVariableMap);
        List<ResolvedShard> rsList = getResolvedDestinationShard(vcursor, destination);
        IExecute.ExecuteMultiShardResponse executeMultiShardResponse = Engine.execShard(vcursor, this.insertReplaceStmt, bindVariableMap, rsList.get(0), true, true).setUpdate();
        VtResultSet vtResultSet = (VtResultSet) executeMultiShardResponse.getVtRowList();

        // If processGenerate generated new values, it supercedes
        // any ids that MySQL might have generated. If both generated
        // values, we don't return an error because this behavior
        // is required to support migration.
        if (insertId != 0) {
            vtResultSet.setInsertID(insertId);
        }
        return new IExecute.ExecuteMultiShardResponse(vtResultSet);
    }

    /**
     * processGenerate generates new values using a sequence if necessary.
     * If no value was generated, it returns 0. Values are generated only
     * for cases where none are supplied.
     *
     * @param vcursor
     * @param bindVariableMap
     * @return
     * @throws Exception
     */
    private Long processGenerate(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        Long insertId = 0L;
        if (this.generate == null) {
            return insertId;
        }

        // Scan input values to compute the number of values to generate, and
        // keep track of where they should be filled.
        List<VtValue> resolved = this.generate.getVtPlanValue().resolveList(bindVariableMap);
        int count = 0;
        for (VtValue val : resolved) {
            if (this.shouldGenerate(val)) {
                count++;
            }
        }

        // If generation is needed, generate the requested number of values (as one call).
        List<Long> sequences = null;
        if (count != 0) {
            Destination dst;
            if (!StringUtil.isNullOrEmpty(this.generate.getPinned())) {
                dst = new DestinationKeyspaceID(Bytes.decodeToByteArray(this.generate.getPinned()));
            } else {
                dst = new DestinationAnyShard();
            }
            Resolver.ResolveDestinationResult resolveDestinationResult = vcursor.resolveDestinations(this.generate.getKeyspace().getName(), null, Lists.newArrayList(dst));
            List<ResolvedShard> rss = resolveDestinationResult.getResolvedShards();
            if (rss.size() != 1) {
                throw new SQLException("processGenerate len(rss)=" + rss.size());
            }
            sequences = SEQUENCE_CACHE.getSequences(vcursor, rss.get(0), this.generate.getKeyspace().getName(), this.generate.getSequenceTableName(), count);
            insertId = sequences.get(0);
        }

        // Fill the holes where no value was supplied.
        for (int i = 0, j = 0; i < resolved.size(); i++) {
            VtValue v = resolved.get(i);
            if (shouldGenerate(v)) {
                bindVariableMap.put(Generate.SEQ_VAR_NAME.substring(1) + i, SqlTypes.int64BindVariable(sequences.get(j++)));
            } else {
                bindVariableMap.put(Generate.SEQ_VAR_NAME.substring(1) + i, SqlTypes.valueBindVariable(v));
            }
        }

        return insertId;
    }

    private InsertShardedRouteResult getInsertShardedRoute(Vcursor vcursor, Map<String, BindVariable> bindVariableMap, List<VtPlanValue> exVindexValueList,
                                                           List<SQLInsertStatement.ValuesClause> exMidExprList, String exPrefix, String exSuffix, List<SQLExpr> exSuffixExpr,
                                                           Map<String, String> switchTableMap) throws SQLException {
        // vindexRowsValues builds the values of all vindex columns.
        // the 3-d structure indexes are colVindex, row, col. Note that
        // ins.Values indexes are colVindex, col, row. So, the conversion
        // involves a transpose.
        // The reason we need to transpose is because all the Vindex APIs
        // require inputs in that format.
        List<List<List<VtValue>>> vindexRowsValues = new ArrayList<>();
        int rowCount = 0;
        for (int vIdx = 0; vIdx < exVindexValueList.size(); vIdx++) {
            VtPlanValue vColValues = exVindexValueList.get(vIdx);
            if (vColValues.getVtPlanValueList().size() != this.table.getColumnVindexes(vIdx).getColumnsList().size()) {
                throw new SQLException("BUG: supplied vindex column values don't match vschema: " + vColValues + " " + this.table.getColumnVindexes(vIdx).getColumnsList());
            }
            List<List<VtValue>> rowList = new ArrayList<>();
            for (int colIdx = 0; colIdx < vColValues.getVtPlanValueList().size(); colIdx++) {
                VtPlanValue colValues = vColValues.getVtPlanValueList().get(colIdx);
                List<VtValue> rowsResolvedValues = colValues.resolveList(bindVariableMap);
                // This is the first iteration: allocate for transpose.
                if (colIdx == 0) {
                    if (rowsResolvedValues.size() == 0) {
                        throw new SQLException("BUG: rowcount is zero for inserts: " + rowsResolvedValues);
                    }
                    if (rowCount == 0) {
                        rowCount = rowsResolvedValues.size();
                    }
                    if (rowCount != rowsResolvedValues.size()) {
                        throw new SQLException("BUG: uneven row values for inserts: " + rowCount + " " + rowsResolvedValues.size());
                    }
                }
                // Perform the transpose.
                for (VtValue rowsResolvedValue : rowsResolvedValues) {
                    rowList.add(Lists.newArrayList(rowsResolvedValue));
                }
            }
            vindexRowsValues.add(rowList);
        }

        // The output from the following 'process' functions is a list of
        // keyspace ids. For regular inserts, a failure to find a route
        // results in an error. For 'ignore' type inserts, the keyspace
        // id is returned as nil, which is used later to drop the corresponding rows.
        byte[][] keyspaceIds = this.processPrimary(vindexRowsValues.get(0));

        for (int vIdx = 1; vIdx < this.table.getColumnVindexesList().size(); vIdx++) {
            Vschema.ColumnVindex colVindex = this.table.getColumnVindexes(vIdx);
            this.processUnowned(vindexRowsValues.get(vIdx), colVindex, keyspaceIds);
        }

        // Build 3-d bindvars. Skip rows with nil keyspace ids in case
        // we're executing an insert ignore.
        for (int vIdx = 0; vIdx < this.table.getColumnVindexesList().size(); vIdx++) {
            Vschema.ColumnVindex colVindex = this.table.getColumnVindexes(vIdx);
            for (int rowNum = 0; rowNum < vindexRowsValues.get(vIdx).size(); rowNum++) {
                List<VtValue> rowColumnKeys = vindexRowsValues.get(vIdx).get(rowNum);
                if (keyspaceIds[rowNum] == null) {
                    // InsertShardedIgnore: skip the row.
                    continue;
                }
                for (int colIdx = 0; colIdx < rowColumnKeys.size(); colIdx++) {
                    VtValue vindexKey = rowColumnKeys.get(colIdx);
                    String col = colVindex.getColumns(colIdx);
                    String name = Engine.insertVarName(col, rowNum);
                    Integer colCorrespondingIndex = InsertPlan.findColumn(this.insertReplaceStmt, col);
                    String actualName = exMidExprList.get(rowNum).getValues().get(colCorrespondingIndex).toString();
                    if (actualName.contains(col) && !actualName.equals(name)) {
                        name = actualName.substring(1);
                    }

                    bindVariableMap.put(name, SqlTypes.valueBindVariable(vindexKey));
                }
            }
        }

        // We need to know the keyspace ids and the Mids associated with
        // each RSS.  So we pass the ksid indexes in as ids, and get them back
        // as values. We also skip nil KeyspaceIds, no need to resolve them.
        List<Query.Value> indexes = new ArrayList<>();
        List<Destination> destinations = new ArrayList<>();
        for (int i = 0; i < keyspaceIds.length; i++) {
            byte[] ksid = keyspaceIds[i];
            if (ksid != null) {
                indexes.add(Query.Value.newBuilder().setValue(ByteString.copyFrom(Long.toString(i).getBytes())).build());
                destinations.add(new DestinationKeyspaceID(ksid));
            }
        }
        if (destinations.isEmpty()) {
            // In this case, all we have is nil KeyspaceIds, we don't do
            // anything at all.
            return new InsertShardedRouteResult(Collections.emptyList(), Collections.emptyList());
        }

        Resolver.ResolveDestinationResult resolveDestinationResult = vcursor.resolveDestinations(this.keyspace.getName(), indexes, destinations);
        List<ResolvedShard> rss = resolveDestinationResult.getResolvedShards();
        List<List<Query.Value>> indexesPerRss = resolveDestinationResult.getValues();
        String charEncoding = vcursor.getCharEncoding();
        String newSuffix = getNewSuffix(bindVariableMap, exSuffix, exSuffixExpr, switchTableMap, charEncoding);

        List<BoundQuery> queries = new ArrayList<>();
        for (int i = 0; i < rss.size(); i++) {
            List<String> mids = new ArrayList<>();
            for (Query.Value indexValue : indexesPerRss.get(i)) {
                StringBuilder output = new StringBuilder();
                VtRestoreVisitor vtRestoreVisitor = new VtRestoreVisitor(output, bindVariableMap, charEncoding);
                long index = Long.parseLong(indexValue.getValue().toString(StandardCharsets.UTF_8));
                if (keyspaceIds[Math.toIntExact(index)] != null) {
                    SQLInsertStatement.ValuesClause valuesClause = exMidExprList.get(Math.toIntExact(index));
                    valuesClause.accept(vtRestoreVisitor);
                    if (vtRestoreVisitor.getException() != null) {
                        throw vtRestoreVisitor.getException();
                    }
                    mids.add(output.toString());
                }
            }
            String rewritten = exPrefix + String.join(",", mids) + newSuffix;
            queries.add(new BoundQuery(rewritten));
        }

        return new InsertShardedRouteResult(rss, queries);
    }

    /**
     * getInsertShardedRoute performs all the vindex related work
     * and returns a map of shard to queries.
     * Using the primary vindex, it computes the target keyspace ids.
     * For owned vindexes, it creates entries.
     * For unowned vindexes with no input values, it reverse maps.
     * For unowned vindexes with values, it validates.
     * If it's an IGNORE or ON DUPLICATE key insert, it drops unroutable rows.
     *
     * @param vcursor
     * @param bindVariableMap
     * @return
     * @throws Exception
     */
    private InsertShardedRouteResult getInsertShardedRoute(Vcursor vcursor, Map<String, BindVariable> bindVariableMap) throws SQLException {
        return this.getInsertShardedRoute(vcursor, bindVariableMap, this.vindexValueList, this.midExprList, this.prefix, this.suffix, this.suffixExpr, null);
    }

    /**
     * processPrimary maps the primary vindex values to the keyspace ids.
     *
     * @param vindexColumnsKeys
     * @return
     * @throws SQLException
     */
    private byte[][] processPrimary(List<List<VtValue>> vindexColumnsKeys) throws SQLException {
        List<VtValue> firstCols = new ArrayList<>();
        for (List<VtValue> val : vindexColumnsKeys) {
            firstCols.add(val.get(0));
        }
        Destination[] destinations = new BinaryHash().map(firstCols.toArray(new VtValue[0]));

        byte[][] keyspaceIds = new byte[destinations.length][];
        for (int i = 0; i < destinations.length; i++) {
            Destination destination = destinations[i];
            if (destination instanceof DestinationKeyspaceID) {
                // This is a single keyspace id, we're good.
                keyspaceIds[i] = ((DestinationKeyspaceID) destination).getValue();
            } else if (destination instanceof DestinationNone) {
                // No valid keyspace id, we may return an error.
                if (!Engine.InsertOpcode.InsertShardedIgnore.equals(this.insertOpcode)) {
                    throw new SQLException("could not map " + vindexColumnsKeys.get(0) + " to a keyspace id");
                }
            } else {
                throw new SQLException("could not map " + vindexColumnsKeys.get(0) + " to a unique keyspace id: " + destination);
            }
        }
        return keyspaceIds;
    }

    /**
     * processUnowned either reverse maps or validates the values for an unowned column.
     *
     * @param vindexColumnsKeys
     * @param colVindex
     * @param ksids
     * @throws SQLException
     */
    private void processUnowned(List<List<VtValue>> vindexColumnsKeys, Vschema.ColumnVindex colVindex, byte[][] ksids) throws SQLException {
        List<Integer> reverseIndexes = new ArrayList<>();
        List<byte[]> reverseKsids = new ArrayList<>();
        List<Integer> verifyIndexes = new ArrayList<>();
        List<List<VtValue>> verifyKeys = new ArrayList<>();
        List<byte[]> verifyKsids = new ArrayList<>();

        for (int rowNum = 0; rowNum < vindexColumnsKeys.size(); rowNum++) {
            List<VtValue> rowColumnKeys = vindexColumnsKeys.get(rowNum);
            // Right now, we only validate against the first column of a colvindex.
            if (ksids[rowNum] == null) {
                continue;
            }
            // Perform reverse map only for non-multi-column vindexes.
            if (rowColumnKeys.get(0).isNull()) {
                reverseIndexes.add(rowNum);
                reverseKsids.add(ksids[rowNum]);
            } else {
                verifyIndexes.add(rowNum);
                verifyKeys.add(rowColumnKeys);
                verifyKsids.add(ksids[rowNum]);
            }
        }

        // For cases where a value was not supplied, we reverse map it
        // from the keyspace id, if possible.
        if (!reverseKsids.isEmpty()) {
            VtValue[] reverseKeys = new BinaryHash().reverseMap(reverseKsids.toArray(new byte[0][]));
            for (int i = 0; i < reverseKeys.length; i++) {
                VtValue reverseKey = reverseKeys[i];
                // Fill the first column with the reverse-mapped value.
                vindexColumnsKeys.get(reverseIndexes.get(i)).set(0, reverseKey);
            }
        }

        if (!verifyKsids.isEmpty()) {
            // If values were supplied, we validate against keyspace id.
            Boolean[] verified = new BinaryHash().verify(verifyKeys.get(0).toArray(new VtValue[0]), verifyKsids.toArray(new byte[0][]));

            List<List<VtValue>> mismatchVindexKeys = new ArrayList<>();
            for (int i = 0; i < verified.length; i++) {
                Boolean v = verified[i];
                Integer rowNum = verifyIndexes.get(i);
                if (!v) {
                    if (!Engine.InsertOpcode.InsertShardedIgnore.equals(this.insertOpcode)) {
                        mismatchVindexKeys.add(vindexColumnsKeys.get(rowNum));
                        continue;
                    }
                    // InsertShardedIgnore: skip the row.
                    ksids[verifyIndexes.get(i)] = null;
                }
            }
            if (!mismatchVindexKeys.isEmpty()) {
                throw new SQLException("values " + mismatchVindexKeys + " for column " + colVindex.getColumnsList() + " does not map to keyspace ids");
            }
        }
    }

    /**
     * shouldGenerate determines if a sequence value should be generated for a given value
     *
     * @param vtValue
     * @return
     */
    private Boolean shouldGenerate(VtValue vtValue) {
        if (vtValue.isNull()) {
            return Boolean.TRUE;
        }

        // Unless the NO_AUTO_VALUE_ON_ZERO sql mode is active in mysql, it also
        // treats 0 as a value that should generate a new sequence.
        try {
            BigInteger n = EvalEngine.toUint64(vtValue);
            if (BigInteger.ZERO.equals(n)) {
                return Boolean.TRUE;
            }
        } catch (SQLException e) {
            return Boolean.FALSE;
        }
        return Boolean.FALSE;
    }

    private List<BoundQuery> getUnshardQueries(Map<String, BindVariable> bindValues, List<SQLInsertStatement.ValuesClause> exMidExprList, String exPrefix, String exSuffix,
                                               List<SQLExpr> exSuffixExpr, Map<String, String> switchTableMap, String charEncoding) throws SQLException {
        String newSuffix = getNewSuffix(bindValues, exSuffix, exSuffixExpr, switchTableMap, charEncoding);
        List<BoundQuery> queries = new ArrayList<>();
        List<String> mids = new ArrayList<>();
        for (SQLInsertStatement.ValuesClause valuesClause : exMidExprList) {
            StringBuilder output = new StringBuilder();
            VtRestoreVisitor vtRestoreVisitor = new VtRestoreVisitor(output, bindValues, charEncoding);
            valuesClause.accept(vtRestoreVisitor);
            if (vtRestoreVisitor.getException() != null) {
                throw vtRestoreVisitor.getException();
            }
            mids.add(output.toString());
        }
        String rewritten = exPrefix + String.join(",", mids) + newSuffix;
        queries.add(new BoundQuery(rewritten));
        return queries;
    }

    private String getNewSuffix(Map<String, BindVariable> bindVariableMap, String exSuffix, List<SQLExpr> exSuffixExpr, Map<String, String> switchTableMap, String charEncoding) throws SQLException {
        String newSuffix = exSuffix;
        if (!StringUtil.isNullOrEmpty(exSuffix)) {
            newSuffix = "on duplicate key update ";
            StringJoiner sj = new StringJoiner(", ", "", "");
            for (SQLExpr expr : exSuffixExpr) {
                StringBuilder output = new StringBuilder();
                VtRestoreVisitor vtRestoreVisitor = new VtRestoreVisitor(output, bindVariableMap, switchTableMap, charEncoding);
                expr.accept(vtRestoreVisitor);
                if (vtRestoreVisitor.getException() != null) {
                    throw vtRestoreVisitor.getException();
                }
                sj.add(output.toString());
            }
            newSuffix += sj.toString();
        }
        return newSuffix;
    }

    private List<ResolvedShard> getResolvedUnsharded(final Vcursor vcursor) throws SQLException {
        Resolver.ResolveDestinationResult resolveDestinationResult = vcursor.resolveDestinations(
            this.keyspace.getName(), null, Lists.newArrayList(new DestinationAllShard()));
        List<ResolvedShard> rsList = resolveDestinationResult.getResolvedShards();
        if (rsList.size() != 1) {
            throw new SQLException("Keyspace does not have exactly one shard: " + rsList);
        }
        return rsList;
    }

    private List<ResolvedShard> getResolvedDestinationShard(final Vcursor vcursor, final Destination destination) throws SQLException {
        Resolver.ResolveDestinationResult resolveDestinationResult = vcursor.resolveDestinations(this.keyspace.getName(), null, Lists.newArrayList(destination));
        List<ResolvedShard> rsList = resolveDestinationResult.getResolvedShards();
        if (rsList.size() != 1) {
            throw new SQLException("Keyspace does not have exactly one shard: " + rsList);
        }
        return rsList;
    }

    @Getter
    @AllArgsConstructor
    private static class InsertShardedRouteResult {
        private final List<ResolvedShard> resolvedShardList;

        private final List<BoundQuery> boundQueryList;
    }

    @Getter
    @AllArgsConstructor
    public static class Generate {
        public static final Integer SEQ_VAR_REFINDEX = -3;

        public static final String SEQ_VAR_NAME = ":__seq";

        private final VKeyspace keyspace;

        /**
         * Values are the supplied values for the column, which
         * will be stored as a list within the PlanValue. New
         * values will be generated based on how many were not
         * supplied (NULL).
         */
        private final VtPlanValue vtPlanValue;

        private final String sequenceTableName;

        private final String pinned;
    }
}
