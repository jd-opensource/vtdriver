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

import io.vitess.proto.Query;
import java.util.List;
import lombok.Data;

/**
 * BindVariable represents a single bind variable in a Query.
 */
@Data
public class BindVariable {
    public static final BindVariable NULL_BIND_VARIABLE = new BindVariable(Query.Type.NULL_TYPE);

    private byte[] value;

    private Query.Type type;

    // values are set if type is TUPLE.
    private List<Query.Value> valuesList;

    private BindVariable(Query.Type type) {
        this.type = type;
    }

    public BindVariable(byte[] value, Query.Type type) {
        this.value = value;
        this.type = type;
    }

    public BindVariable(List<Query.Value> valuesList, Query.Type type) {
        this.type = type;
        this.valuesList = valuesList;
    }
}