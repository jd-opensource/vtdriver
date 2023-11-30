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

package com.jd.jdbc.planbuilder.gen4;


import lombok.Getter;

// TableName represents a table  name.
// Qualifier, if specified, represents a database or keyspace.
// TableName is a value struct whose fields are case sensitive.
// This means two TableName vars can be compared for equality
// and a TableName can also be used as key in a map.
@Getter
public class TableName {
    private String name;

    private String qualifier;

    public TableName(String qualifier, String tableName) {
        this.qualifier = qualifier;
        this.name = tableName;
    }

    public String getDbAndTableName() {
        return qualifier + ".`" + name + "`";
    }
}
