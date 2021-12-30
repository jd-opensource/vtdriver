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
import com.jd.jdbc.engine.DMLEngine;
import com.jd.jdbc.engine.DMLOpcode;
import com.jd.jdbc.engine.DeleteEngine;
import com.jd.jdbc.engine.UpdateEngine;
import com.jd.jdbc.key.Bytes;
import com.jd.jdbc.key.DestinationKeyspaceID;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRemoveDbNameVisitor;
import com.jd.jdbc.sqltypes.VtPlanValue;
import com.jd.jdbc.vindexes.SingleColumn;
import com.jd.jdbc.vindexes.Vindex;
import com.jd.jdbc.vindexes.hash.Binary;
import com.jd.jdbc.vindexes.hash.BinaryHash;
import io.vitess.proto.Topodata;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import vschema.Vschema;

public class DMLPlan {

    public static DMLPlanResult buildDMLPlan(PrimitiveBuilder pb, RoutePlan rb, Vschema.Keyspace keyspace, DMLType dmlType, SQLStatement stmt,
                                             SQLExpr where, List<SQLObject> nodes, String defaltKeyspace) throws SQLException {
        DMLEngine edml;
        switch (dmlType) {
            case UPDATE:
                edml = new UpdateEngine();
                break;
            case DELETE:
                edml = new DeleteEngine();
                break;
            default:
                throw new RuntimeException("expect update or delete");
        }
        edml.setKeyspace(rb.getRouteEngine().getKeyspace());
        if (!edml.getKeyspace().getSharded()) {
            edml.setOpcode(DMLOpcode.Unsharded);
            // Generate query after all the analysis. Otherwise table name substitutions for
            // routed tables won't happen.
            edml.setSQLStatement(dmlFormat(stmt));
            return new DMLPlanResult(edml, null, "");
        }

        if (PlanBuilder.hasSubquery(stmt)) {
            throw new SQLFeatureNotSupportedException("unsupported: subqueries in sharded DML");
        }

        // Generate query after all the analysis. Otherwise table name substitutions for
        // routed tables won't happen.
        edml.setSQLStatement(dmlFormat(stmt));

        if (pb.getSymtab().getTables().size() != 1) {
            throw new SQLFeatureNotSupportedException("unsupported: multi-table " + dmlType + " statement in sharded keyspace");
        }

        for (Map.Entry<String, Symtab.Table> tval : pb.getSymtab().getTables().entrySet()) {
            // There is only one table.
            edml.setTableName(rb.getRouteEngine().getTableName());
            edml.setTable(tval.getValue().getVschemaTable());
        }

        if (!edml.getTable().getPinned().isEmpty()) {
            edml.setOpcode(DMLOpcode.ByDestination);
            edml.setTargetDestination(new DestinationKeyspaceID(Bytes.decodeToByteArray(edml.getTable().getPinned())));
            return new DMLPlanResult(edml, null, "");
        }

        DMLRoutingResult routingResult = getDMLRouting(where, edml.getTable(), keyspace);
        if (null != rb.getRouteEngine().getTargetDestination()) {
            if (rb.getRouteEngine().getTargetTabletType() != Topodata.TabletType.MASTER) {
                throw new SQLFeatureNotSupportedException("unsupported: " + dmlType + " statement with a replica or rdonly target");
            }
            edml.setOpcode(DMLOpcode.ByDestination);
            edml.setTargetDestination(rb.getRouteEngine().getTargetDestination());
            return new DMLPlanResult(edml, routingResult.ksidVindex, routingResult.ksidCol);
        }

        edml.setOpcode(routingResult.routingType);
        edml.setVindex(routingResult.vindex);
        edml.setVtPlanValueList(routingResult.values);
        return new DMLPlanResult(edml, routingResult.ksidVindex, routingResult.ksidCol);
    }

    /**
     * ExtractCommentDirectives parses the comment list for any execution directives of the form:
     * \/*vt+ OPTION_ONE=1 OPTION_TWO OPTION_THREE=abcd *\/
     * It returns the map of the directive values or nil if there aren't any.
     * <p>
     * private static Map<String, Object> extractCommentDirectives(final List<SQLCommentHint> comments) {
     * if (null == comments || comments.size() == 0) {
     * return null;
     * }
     * <p>
     * Map<String, Object> vals = new HashMap<>();
     * for (SQLCommentHint comment : comments) {
     * String commentStr = comment.getText();
     * if (!commentStr.substring(0, 5).equalsIgnoreCase(commentDirectivePreamble)) {
     * continue;
     * }
     * <p>
     * // Split on whitespace and ignore the first and last directive
     * // since they contain the comment start/end
     * String[] directives = commentStr.split("\\s");
     * for (int i = 1; i < directives.length - 1; i++) {
     * String directive = directives[i];
     * int equalSymbolIdx = directive.indexOf("=");
     * if (equalSymbolIdx < 0) {
     * vals.put(directive, true);
     * continue;
     * }
     * <p>
     * String strVal = directive.substring(equalSymbolIdx + 1);
     * directive = directive.substring(0, equalSymbolIdx);
     * vals.put(directive, strVal);
     * }
     * }
     * return vals;
     * }
     */

    private static String generateQuery(SQLStatement statement) {
        final StringBuffer sb = new StringBuffer();
        statement.output(sb);
        return sb.toString();
    }

    // getDMLRouting returns the vindex and values for the DML,
    // If it cannot find a unique vindex match, it returns an error.
    private static DMLRoutingResult getDMLRouting(SQLExpr where, Vschema.Table table, Vschema.Keyspace keyspace) throws SQLException {
        SingleColumn ksidVindex = null;
        String ksidCol = null;
        Map<String, Vindex> vindexes = new HashMap<>();
        for (Map.Entry<String, Vschema.Vindex> entry : keyspace.getVindexesMap().entrySet()) {
            if ("hash".equals(entry.getKey())) {
                vindexes.put(entry.getKey(), new BinaryHash());
            } else {
                vindexes.put(entry.getKey(), new Binary());
            }
        }
        for (Vschema.ColumnVindex index : table.getColumnVindexesList()) {
            Vindex vindex = vindexes.get(index.getName());
            if (!vindex.isUnique()) {
                continue;
            }
            if (!(vindex instanceof SingleColumn)) {
                continue;
            }
            SingleColumn single = (SingleColumn) vindex;
            if (null == ksidCol) {
                ksidCol = index.getColumnsCount() == 0 ? index.getColumn() : index.getColumns(0);
                ksidVindex = single;
            }
            if (null == where) {
                return new DMLRoutingResult(DMLOpcode.Scatter, ksidVindex, ksidCol, null, null);
            }

            PlanBuilder.MatchResult matchResult = PlanBuilder.getMatch(where, index.getColumnsCount() == 0 ? index.getColumn() : index.getColumns(0));
            if (matchResult.isOk()) {
                DMLOpcode opcode = DMLOpcode.Equal;
                if (matchResult.getPv().isList()) {
                    opcode = DMLOpcode.In;
                }

                return new DMLRoutingResult(opcode, ksidVindex, ksidCol, single, Lists.newArrayList(matchResult.getPv()));
            }
        }
        if (null == ksidVindex) {
            throw new SQLException("table without a primary vindex is not expected");
        }
        return new DMLRoutingResult(DMLOpcode.Scatter, ksidVindex, ksidCol, null, null);
    }

    private static SQLStatement dmlFormat(SQLStatement statement) {
        VtRemoveDbNameVisitor visitor = new VtRemoveDbNameVisitor();
        statement.accept(visitor);
        return statement;
    }

    public enum DMLType {
        UPDATE,
        DELETE
    }

    private static class DMLRoutingResult {
        DMLOpcode routingType;

        SingleColumn ksidVindex;

        String ksidCol;

        SingleColumn vindex;

        List<VtPlanValue> values;

        DMLRoutingResult(DMLOpcode routingType, SingleColumn ksidVindex, String ksidCol, SingleColumn vindex, List<VtPlanValue> values) {
            this.routingType = routingType;
            this.ksidVindex = ksidVindex;
            this.ksidCol = ksidCol;
            this.vindex = vindex;
            this.values = values;
        }
    }

    public static class DMLPlanResult {
        DMLEngine dmlEngine;

        SingleColumn ksidVindex;

        String ksidCol;

        public DMLPlanResult(DMLEngine dmlEngine, SingleColumn ksidVindex, String ksidCol) {
            this.dmlEngine = dmlEngine;
            this.ksidVindex = ksidVindex;
            this.ksidCol = ksidCol;
        }
    }

}
