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

package com.jd.jdbc.srvtopo;

import java.util.Map;
import lombok.Data;

/**
 * BoundQuery is a query with its bind variables
 */
@Data
public class BoundQuery {
    // sql is the SQL query to execute
    private String sql;

    // bind_variables is a map of all bind variables to expand in the query.
    // nil values are not allowed. Use NULL_TYPE to express a NULL value.
    private Map<String, BindVariable> bindVariablesMap;

    public BoundQuery(String sql, Map<String, BindVariable> bindVariablesMap) {
        this.sql = sql;
        this.bindVariablesMap = bindVariablesMap;
    }

    public BoundQuery(String sql) {
        this.sql = sql;
    }
}