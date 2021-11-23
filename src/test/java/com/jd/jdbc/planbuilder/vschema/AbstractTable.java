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

package com.jd.jdbc.planbuilder.vschema;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public abstract class AbstractTable {
    @JsonProperty("pinned")
    private String pinned;

    @JsonProperty("type")
    private String type;

    @JsonProperty("auto_increment")
    private AutoIncrement autoIncrement;

    @JsonProperty("columns")
    private List<ColumnsItem> columns;

    @JsonProperty("column_vindexes")
    private List<ColumnVindexesItem> columnVindexes;

    @JsonProperty("column_list_authoritative")
    private boolean columnListAuthoritative;

    public String getPinned() {
        return pinned;
    }

    public String getType() {
        return type;
    }

    public AutoIncrement getAutoIncrement() {
        return autoIncrement;
    }

    public List<ColumnsItem> getColumns() {
        return columns;
    }

    public List<ColumnVindexesItem> getColumnVindexes() {
        return columnVindexes;
    }

    public boolean isColumnListAuthoritative() {
        return columnListAuthoritative;
    }
}
