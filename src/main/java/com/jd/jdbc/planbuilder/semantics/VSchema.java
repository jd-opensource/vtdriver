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

import com.jd.jdbc.VSchemaManager;
import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import com.jd.jdbc.sqlparser.utils.TableNameUtils;
import com.jd.jdbc.vindexes.VKeyspace;
import java.sql.SQLException;
import lombok.Getter;
import vschema.Vschema;

/**
 * VSchema defines the interface for this package to fetch
 * info about tables.
 */
@Getter
public class VSchema implements SchemaInformation {

    private String defaultKeyspace;

    private VSchemaManager vSchemaManager;

    public VSchema(String defaultKeyspace, VSchemaManager vSchemaManager) {
        this.defaultKeyspace = defaultKeyspace;
        this.vSchemaManager = vSchemaManager;
    }

    /**
     * PlannerWarning records warning created during planning.
     *
     * @return
     */
    public void plannerWarning(String message) {
        return;
    }

    @Override
    public SchemaInformationContext findTableOrVindex(SQLExprTableSource tableSource) throws SQLException {
        Vschema.Keyspace keyspace = vSchemaManager.getKeyspace(defaultKeyspace);
        Vschema.Table table = vSchemaManager.getTable(defaultKeyspace, TableNameUtils.getTableSimpleName(tableSource));
        return new SchemaInformationContext(table, null);
    }

    public VKeyspace anyKeyspace() throws SQLException {
        return getVschemaKeyspace();
    }

    public VKeyspace getVschemaKeyspace() throws SQLException {
        Vschema.Keyspace ks = vSchemaManager.getKeyspace(defaultKeyspace);
        return new VKeyspace(this.defaultKeyspace, ks.getSharded());
    }
}
