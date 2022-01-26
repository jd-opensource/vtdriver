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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jd.BaseTest;
import com.jd.jdbc.VSchemaManager;
import com.jd.jdbc.engine.Plan;
import com.jd.jdbc.key.Destination;
import com.jd.jdbc.key.DestinationShard;
import com.jd.jdbc.planbuilder.vschema.AbstractTable;
import com.jd.jdbc.planbuilder.vschema.AutoIncrement;
import com.jd.jdbc.planbuilder.vschema.ColumnVindexesItem;
import com.jd.jdbc.planbuilder.vschema.ColumnsItem;
import com.jd.jdbc.planbuilder.vschema.Keyspaces;
import com.jd.jdbc.planbuilder.vschema.Main;
import com.jd.jdbc.planbuilder.vschema.PlanVschema;
import com.jd.jdbc.planbuilder.vschema.SecondUser;
import com.jd.jdbc.planbuilder.vschema.Tables;
import com.jd.jdbc.planbuilder.vschema.User;
import com.jd.jdbc.planbuilder.vschema.Vindexes;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLMethodInvokeExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLJoinTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectItem;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectQueryBlock;
import com.jd.jdbc.sqlparser.ast.statement.SQLSelectStatement;
import com.jd.jdbc.sqlparser.ast.statement.SQLSubqueryTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLTableSource;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQuery;
import com.jd.jdbc.sqlparser.ast.statement.SQLUnionQueryTableSource;
import com.jd.jdbc.sqlparser.dialect.mysql.BindVarNeeds;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlInsertReplaceStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.parser.MySqlLexer;
import com.jd.jdbc.sqlparser.parser.Lexer;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqlparser.utils.TableNameUtils;
import static com.jd.jdbc.vindexes.Vschema.TYPE_REFERENCE;
import io.vitess.proto.Query;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import vschema.Vschema;

import static com.jd.jdbc.vindexes.Vschema.TYPE_REFERENCE;

public class AbstractPlanTest extends BaseTest {

    protected static final Map<String, String> uniqueTables = new HashMap<>();

    private static final String START_OF_MULTI_LINE_COMMENT = "/*";

    private static final String END_OF_MULTI_LINE_COMMENT = "*/";

    static {
        Symtab.registSingleColumnVindex("user_index");
        Symtab.registSingleColumnVindex("kid_index");
        Symtab.registSingleColumnVindex("music_user_map");
        Symtab.registSingleColumnVindex("cola_map");
        Symtab.registSingleColumnVindex("colb_colc_map");
        Symtab.registSingleColumnVindex("cola_kid_map");
        Symtab.registSingleColumnVindex("email_user_map");
        Symtab.registSingleColumnVindex("address_user_map");
        Symtab.registSingleColumnVindex("hash_dup");
        Symtab.registSingleColumnVindex("vindex1");
        Symtab.registSingleColumnVindex("vindex2");
        Symtab.registSingleColumnVindex("hash_dup");
    }

    protected static Plan build(String query, VSchemaManager vm) throws Exception {
        Map<String, String> prop = parseComment(query);
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(query);
        for (Map.Entry<String, String> entry : prop.entrySet()) {
            stmt.putAttribute(entry.getKey(), entry.getValue());
        }
        Destination destination = null;
        String dest = extractShardPropFromStmt(stmt);
        if (dest != null) {
            destination = new DestinationShard(dest);
        }
        String defaultKeyspace = "";
        if (stmt instanceof SQLSelectStatement) {
            SQLSelectStatement selectStatement = (SQLSelectStatement) stmt;
            SQLSelectQuery selectQuery = selectStatement.getSelect().getQuery();
            if (selectQuery instanceof SQLUnionQuery) {
                String defaultKeyspaceLeft = processSelectQuery(((SQLUnionQuery) selectQuery).getLeft());
                String defaultKeyspaceRight = processSelectQuery(((SQLUnionQuery) selectQuery).getRight());
                if (!defaultKeyspaceLeft.equalsIgnoreCase(defaultKeyspaceRight)) {
                    throw new SQLException("Multi keyspaces");
                }
                defaultKeyspace = defaultKeyspaceLeft;
            } else {
                defaultKeyspace = processSelectQuery(selectQuery);
            }
        } else if (stmt instanceof MySqlInsertReplaceStatement) {
            SQLExprTableSource exprTableSource = ((MySqlInsertReplaceStatement) stmt).getTableSource();
            if (exprTableSource.getExpr() instanceof SQLPropertyExpr) {
                defaultKeyspace = ((SQLPropertyExpr) exprTableSource.getExpr()).getOwnernName();
            } else {
                String tableName = TableNameUtils.getTableSimpleName(exprTableSource);
                defaultKeyspace = uniqueTables.get(tableName.toLowerCase());
            }
        } else if (stmt instanceof MySqlDeleteStatement) {
            MySqlDeleteStatement deleteStatement = ((MySqlDeleteStatement) stmt);
            if (deleteStatement.getFrom() != null) {
                defaultKeyspace = processSQLTableSource(deleteStatement.getFrom());
            } else if (deleteStatement.getTableSource() != null) {
                defaultKeyspace = processSQLTableSource(deleteStatement.getTableSource());
            }
        } else if (stmt instanceof MySqlUpdateStatement) {
            MySqlUpdateStatement updateStatement = ((MySqlUpdateStatement) stmt);
            if (updateStatement.getFrom() != null) {
                defaultKeyspace = processSQLTableSource(updateStatement.getFrom());
            } else if (updateStatement.getTableSource() != null) {
                defaultKeyspace = processSQLTableSource(updateStatement.getTableSource());
            }
        }

        if (StringUtils.isEmpty(defaultKeyspace)) {
            if (query.equals("select c from t")) {
                defaultKeyspace = "user";
            } else {
                throw new SQLException("Not found defaultKeyspace");
            }
        }
        return PlanBuilder.buildFromStmt(stmt, vm, defaultKeyspace, new BindVarNeeds(), destination);
    }

    private static String processSelectQuery(SQLSelectQuery selectQuery) throws SQLException {
        String defaultKeyspace = "";
        if (selectQuery instanceof SQLUnionQuery) {
            String defaultKeyspaceLeft = processSelectQuery(((SQLUnionQuery) selectQuery).getLeft());
            String defaultKeyspaceRight = processSelectQuery(((SQLUnionQuery) selectQuery).getRight());
            if (!defaultKeyspaceLeft.equalsIgnoreCase(defaultKeyspaceRight)) {
                throw new SQLException("Multi keyspaces");
            }
            return defaultKeyspaceLeft;
        } else if (selectQuery instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock selectQueryBlock = (SQLSelectQueryBlock) selectQuery;

            for (SQLSelectItem selectItem : selectQueryBlock.getSelectList()) {
                if (selectItem.getExpr() instanceof SQLMethodInvokeExpr) {
                    defaultKeyspace = "main";
                    break;
                }
            }

            SQLTableSource tableSource = selectQueryBlock.getFrom();
            if (tableSource == null) {
                selectQueryBlock.setFrom(new SQLExprTableSource(new SQLIdentifierExpr("dual")));
                defaultKeyspace = "main";
            } else {
                defaultKeyspace = processSQLTableSource(tableSource);
            }
        }
        return defaultKeyspace;
    }

    private static String processSQLTableSource(SQLTableSource tableSource) throws SQLException {
        String defaultKeyspace = "";
        if (tableSource == null) {
            defaultKeyspace = "main";
        } else if (tableSource instanceof SQLExprTableSource) {
            defaultKeyspace = processExprTableSource((SQLExprTableSource) tableSource);
        } else if (tableSource instanceof SQLUnionQueryTableSource) {
            SQLUnionQuery unionQuery = ((SQLUnionQueryTableSource) tableSource).getUnion();
            defaultKeyspace = processSelectQuery(unionQuery);
        } else if (tableSource instanceof SQLJoinTableSource) {
            SQLJoinTableSource joinTableSource = (SQLJoinTableSource) tableSource;
            SQLTableSource leftTableSource = joinTableSource.getLeft();
            SQLTableSource rightTableSource = joinTableSource.getRight();

            if (leftTableSource instanceof SQLExprTableSource) {
                defaultKeyspace = processExprTableSource((SQLExprTableSource) leftTableSource);
            } else if (leftTableSource instanceof SQLSubqueryTableSource) {
                SQLTableSource from = ((SQLSubqueryTableSource) leftTableSource).getSelect().getQueryBlock().getFrom();
                defaultKeyspace = processExprTableSource((SQLExprTableSource) from);
            }
            if (StringUtils.isEmpty(defaultKeyspace)) {
                if (rightTableSource instanceof SQLExprTableSource) {
                    defaultKeyspace = processExprTableSource((SQLExprTableSource) rightTableSource);
                } else if (rightTableSource instanceof SQLSubqueryTableSource) {
                    SQLTableSource from = ((SQLSubqueryTableSource) rightTableSource).getSelect().getQueryBlock().getFrom();
                    defaultKeyspace = processExprTableSource((SQLExprTableSource) from);
                }
            }
        } else if (tableSource instanceof SQLSubqueryTableSource) {
            SQLTableSource subqueryTableSource = ((SQLSubqueryTableSource) tableSource).getSelect().getQueryBlock().getFrom();

            if (subqueryTableSource instanceof SQLExprTableSource) {
                defaultKeyspace = processExprTableSource((SQLExprTableSource) subqueryTableSource);
            } else if (subqueryTableSource instanceof SQLJoinTableSource) {
                SQLJoinTableSource joinTableSource = (SQLJoinTableSource) subqueryTableSource;
                SQLTableSource leftTableSource = joinTableSource.getLeft();
                SQLTableSource rightTableSource = joinTableSource.getRight();

                if (leftTableSource instanceof SQLExprTableSource) {
                    defaultKeyspace = processExprTableSource((SQLExprTableSource) leftTableSource);
                } else if (leftTableSource instanceof SQLSubqueryTableSource) {
                    SQLTableSource from = ((SQLSubqueryTableSource) leftTableSource).getSelect().getQueryBlock().getFrom();
                    defaultKeyspace = processExprTableSource((SQLExprTableSource) from);
                }
                if (StringUtils.isEmpty(defaultKeyspace)) {
                    if (rightTableSource instanceof SQLExprTableSource) {
                        defaultKeyspace = processExprTableSource((SQLExprTableSource) rightTableSource);
                    } else if (rightTableSource instanceof SQLSubqueryTableSource) {
                        SQLTableSource from = ((SQLSubqueryTableSource) rightTableSource).getSelect().getQueryBlock().getFrom();
                        defaultKeyspace = processExprTableSource((SQLExprTableSource) from);
                    }
                }
            }
        }

        return defaultKeyspace;
    }

    private static String processExprTableSource(SQLExprTableSource exprTableSource) throws SQLException {
        String defaultKeyspace;
        if (exprTableSource.getExpr() instanceof SQLPropertyExpr) {
            defaultKeyspace = ((SQLPropertyExpr) exprTableSource.getExpr()).getOwnernName();
            if ("information_schema".equalsIgnoreCase(defaultKeyspace)) {
                defaultKeyspace = "main";
            }
        } else {
            String tableName = TableNameUtils.getTableSimpleName(exprTableSource);
            if ("unsharded_no_metadata".equalsIgnoreCase(tableName) || "dual".equalsIgnoreCase(tableName)) {
                defaultKeyspace = "main";
            } else {
                defaultKeyspace = uniqueTables.get(tableName.toLowerCase());
            }
        }
        return defaultKeyspace;
    }

    protected static Map<String, String> parseComment(String sql) {
        //only support parse comment from the beginning of sql statement.
        Lexer lexer = new MySqlLexer(sql);
        lexer.setKeepComments(true);
        lexer.nextToken();

        Map<String, String> prop = new HashMap<>(16, 1);

        List<String> comments = lexer.readAndResetComments();
        if (comments == null || comments.isEmpty()) {
            return prop;
        }

        for (String comment : comments) {
            if (comment.startsWith(START_OF_MULTI_LINE_COMMENT) && comment.endsWith(END_OF_MULTI_LINE_COMMENT)) {
                comment = comment.substring(2, comment.length() - 2);
            }

            String[] commentContents = comment.split(",");
            for (String content : commentContents) {
                int operatorIedx = content.indexOf("=");
                if (operatorIedx == -1 || operatorIedx == 0 || operatorIedx == content.length() - 1) {
                    continue;
                }
                String leftExpr = content.substring(0, operatorIedx).trim();
                String rightExpr = content.substring(operatorIedx + 1).trim();
                prop.put(leftExpr, rightExpr);
            }
        }

        return prop;
    }

    protected static String extractShardPropFromStmt(final SQLStatement stmt) {
        Map<String, Object> attributes = stmt.getAttributes();
        if (!attributes.containsKey("shard")) {
            return null;
        }
        Object shardObject = attributes.get("shard");
        if (!(shardObject instanceof String)) {
            return null;
        }
        String shard = (String) shardObject;
        return shard;
    }

    protected VSchemaManager loadSchema(String filename) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(filename), StandardCharsets.UTF_8);
        StringBuilder sb = new StringBuilder();
        lines.forEach(sb::append);

        PlanVschema planVschema = new ObjectMapper().readValue(sb.toString(), PlanVschema.class);
        return new VSchemaManager(this.buildkeyspaces(planVschema.getKeyspaces()));
    }

    private Map<String, Vschema.Keyspace> buildkeyspaces(Keyspaces keyspaces) {
        Map<String, Vschema.Keyspace> ksMap = new HashMap<>();

        User user = keyspaces.getUser();
        if (user != null) {
            ksMap.put("user", Vschema.Keyspace.newBuilder()
                .putAllVindexes(this.buildVschemaVindex(user.getVindexes()))
                .putAllTables(this.buildVschemaTables(user))
                .setSharded(user.isSharded())
                .build());
        }

        SecondUser secondUser = keyspaces.getSecondUser();
        if (secondUser != null) {
            ksMap.put("secondUser", Vschema.Keyspace.newBuilder()
                .putAllVindexes(this.buildVschemaVindex(secondUser.getVindexes()))
                .putAllTables(this.buildVschemaTables(secondUser))
                .setSharded(secondUser.isSharded())
                .build());
        }

        Main main = keyspaces.getMain();
        if (main != null) {
            ksMap.put("main", Vschema.Keyspace.newBuilder()
                .putAllTables(this.buildVschemaTables(main))
                .build());
        }

        for (Map.Entry<String, Vschema.Keyspace> keyspaceEntry : ksMap.entrySet()) {
            String keyspaceName = keyspaceEntry.getKey();
            Vschema.Keyspace keyspace = keyspaceEntry.getValue();
            for (String tableName : keyspace.getTablesMap().keySet()) {
                uniqueTables.put(tableName, keyspaceName);
            }
        }

        return ksMap;
    }

    private Map<String, Vschema.Vindex> buildVschemaVindex(Vindexes vindexes) {
        Map<String, Vschema.Vindex> vindexMap = new HashMap<>();
        Vschema.Vindex vindex = Vschema.Vindex.newBuilder()
            .setType(vindexes.getHash().getType())
            .build();
        vindexMap.put("hash", vindex);
        return vindexMap;
    }

    private Map<String, Vschema.Table> buildVschemaTables(Object obj) {
        Map<String, Vschema.Table> tableMap = new HashMap<>();
        if (obj instanceof User) {
            Tables tables = ((User) obj).getTables();
            if (tables.getSeq() != null) {
                tableMap.put("seq", this.buildVschemaTable(tables.getSeq()));
            }
            if (tables.getUser() != null) {
                tableMap.put("user", this.buildVschemaTable(tables.getUser()));
            }
            if (tables.getUserMetadata() != null) {
                tableMap.put("user_metadata", this.buildVschemaTable(tables.getUserMetadata()));
            }
            if (tables.getUserExtra() != null) {
                tableMap.put("user_extra", this.buildVschemaTable(tables.getUserExtra()));
            }
            if (tables.getMusic() != null) {
                tableMap.put("music", this.buildVschemaTable(tables.getMusic()));
            }
            if (tables.getAuthoritative() != null) {
                tableMap.put("authoritative", this.buildVschemaTable(tables.getAuthoritative()));
            }
            if (tables.getSamecolvin() != null) {
                tableMap.put("samecolvin", this.buildVschemaTable(tables.getSamecolvin()));
            }
            if (tables.getMulticolvin() != null) {
                tableMap.put("multicolvin", this.buildVschemaTable(tables.getMulticolvin()));
            }
            if (tables.getOverlapVindex() != null) {
                tableMap.put("overlap_vindex", this.buildVschemaTable(tables.getOverlapVindex()));
            }
            if (tables.getMusicExtra() != null) {
                tableMap.put("music_extra", this.buildVschemaTable(tables.getMusicExtra()));
            }
            if (tables.getPinTest() != null) {
                tableMap.put("pin_test", this.buildVschemaTable(tables.getPinTest()));
            }
            if (tables.getWeirdName() != null) {
                tableMap.put("weird`name", this.buildVschemaTable(tables.getWeirdName()));
            }
            if (tables.getTest() != null) {
                tableMap.put("test", this.buildVschemaTable(tables.getTest()));
            }
        } else if (obj instanceof Main) {
            Tables tables = ((Main) obj).getTables();
            tableMap.put("m1", this.buildVschemaTable(tables.getM1()));
            tableMap.put("foo", this.buildVschemaTable(tables.getFoo()));
            tableMap.put("unsharded", this.buildVschemaTable(tables.getUnsharded()));
            tableMap.put("unsharded_a", this.buildVschemaTable(tables.getUnshardedA()));
            tableMap.put("unsharded_b", this.buildVschemaTable(tables.getUnshardedB()));
            tableMap.put("unsharded_auto", this.buildVschemaTable(tables.getUnshardedAuto()));
            tableMap.put("unsharded_authoritative", this.buildVschemaTable(tables.getUnshardedAuthoritative()));
            tableMap.put("seq", this.buildVschemaTable(tables.getSeq()));
        }
        tableMap.put("dual", Vschema.Table.newBuilder().setType(TYPE_REFERENCE).build());
        return tableMap;
    }

    private Vschema.Table buildVschemaTable(AbstractTable abstractTable) {
        if (abstractTable == null) {
            return null;
        }
        Vschema.Table.Builder tableBuilder = Vschema.Table.newBuilder();

        AutoIncrement autoIncrement = abstractTable.getAutoIncrement();
        if (autoIncrement != null) {
            tableBuilder.setAutoIncrement(this.buildAutoIncrement(autoIncrement));
        }

        List<ColumnsItem> columnsItemList = abstractTable.getColumns();
        if (columnsItemList != null && !columnsItemList.isEmpty()) {
            tableBuilder.addAllColumns(this.buildColumns(columnsItemList));
        }

        List<ColumnVindexesItem> columnVindexesItemList = abstractTable.getColumnVindexes();
        if (columnVindexesItemList != null && !columnVindexesItemList.isEmpty()) {
            tableBuilder.addAllColumnVindexes(this.buildColumnVindexes(columnVindexesItemList));
        }

        tableBuilder.setColumnListAuthoritative(abstractTable.isColumnListAuthoritative());

        String type = "";
        if (StringUtils.isNotEmpty(abstractTable.getType())) {
            type = abstractTable.getType();
        }
        tableBuilder.setType(type);

        String pinned = "";
        if (StringUtils.isNotEmpty(abstractTable.getPinned())) {
            pinned = abstractTable.getPinned();
        }
        tableBuilder.setPinned(pinned);

        return tableBuilder.build();
    }

    private Vschema.AutoIncrement buildAutoIncrement(AutoIncrement autoIncrement) {
        return Vschema.AutoIncrement.newBuilder()
            .setSequence(autoIncrement.getSequence())
            .setColumn(autoIncrement.getColumn())
            .build();
    }

    private List<Vschema.Column> buildColumns(List<ColumnsItem> columnsItemList) {
        List<Vschema.Column> columnList = new ArrayList<>();
        for (ColumnsItem columnsItem : columnsItemList) {
            Vschema.Column.Builder columnBuilder = Vschema.Column.newBuilder();
            columnBuilder.setName(StringUtils.nullToEmpty(columnsItem.getName()));
            Query.Type type = Query.Type.NULL_TYPE;
            if (StringUtils.isNotEmpty(columnsItem.getType())) {
                type = Query.Type.valueOf(columnsItem.getType());
            }
            columnBuilder.setType(type);
            columnList.add(columnBuilder.build());
        }
        return columnList;
    }

    private List<Vschema.ColumnVindex> buildColumnVindexes(List<ColumnVindexesItem> columnVindexesItemList) {
        List<Vschema.ColumnVindex> columnVindexList = new ArrayList<>();
        for (ColumnVindexesItem columnVindexesItem : columnVindexesItemList) {
            Vschema.ColumnVindex columnVindex = Vschema.ColumnVindex.newBuilder()
                .setColumn(StringUtils.nullToEmpty(columnVindexesItem.getColumn()))
                .setName(StringUtils.nullToEmpty(columnVindexesItem.getName()))
                .build();
            columnVindexList.add(columnVindex);
        }
        return columnVindexList;
    }
}
