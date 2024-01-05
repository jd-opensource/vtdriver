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

import com.jd.jdbc.sqlparser.ast.statement.SQLExprTableSource;
import java.sql.SQLException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import vschema.Vschema;

/**
 * SchemaInformation is used tp provide table information from Vschema.
 */
public interface SchemaInformation {

    SchemaInformationContext findTableOrVindex(SQLExprTableSource tableSource) throws SQLException;

    @Getter
    @AllArgsConstructor
    class SchemaInformationContext {
        @Setter
        private Vschema.Table table;

        private Vschema.ColumnVindex vindex;
    }
}
