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

package com.jd.jdbc.planbuilder.semantics;

import com.jd.jdbc.sqlparser.ast.SQLObject;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqlparser.utils.TableNameUtils;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Data;

@Data
public class Scope {
    private Scope parent;

    private SQLObject stmt;

    private List<TableInfo> tables;

    private boolean isUnion;

    private Map<String, TableSet> joinUsing;

    public Scope(Scope parent) {
        this.parent = parent;
        this.joinUsing = new HashMap<>(16);
        this.tables = new ArrayList<>(10);
    }

    public void addTable(TableInfo info) throws SQLException {
        SQLExprTableSource name = info.name();
        String tblName = name.getAlias();
        if (StringUtils.isEmpty(tblName)) {
            tblName = TableNameUtils.getTableSimpleName(name);
        }
        for (TableInfo table : this.tables) {
            SQLExprTableSource tableName = table.name();

            String simpleName = tableName.getAlias();
            if (StringUtils.isEmpty(simpleName)) {
                simpleName = TableNameUtils.getTableSimpleName(tableName);
            }
            if (Objects.equals(simpleName, tblName)) {
                throw new SQLException("Not unique table/alias: '" + tblName + "'" + "==='" + simpleName + "'");
            }
        }
        this.tables.add(info);
    }

    public Map<TableSet, Map<String, TableSet>> prepareUsingMap() {
        Map<TableSet, Map<String, TableSet>> result = new HashMap<>(this.getJoinUsing().size());
        for (Map.Entry<String, TableSet> entry : this.getJoinUsing().entrySet()) {
            TableSet tss = entry.getValue();
            for (TableSet ts : tss.constituents()) {
                Map<String, TableSet> m = result.get(ts);
                if (m == null) {
                    m = new HashMap<>(this.getJoinUsing().size());
                }
                m.put(entry.getKey(), tss);
                result.put(ts, m);
            }
        }
        return result;
    }
}
