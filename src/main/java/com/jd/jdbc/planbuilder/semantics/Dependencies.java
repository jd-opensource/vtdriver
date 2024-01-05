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

import io.vitess.proto.Query;
import java.sql.SQLException;

/**
 * these types are used to go over dependencies provided by multiple
 * tables and figure out bindings and/or errors by merging dependencies together
 */
public interface Dependencies {

    boolean empty();

    Dependency get() throws SQLException;

    Dependencies merge(Dependencies other, boolean allowMulti);

    static Certain createCertain(TableSet direct, TableSet recursive, Type qt) {
        Certain c = new Certain(direct, recursive);
        if (qt != null && qt.getType() != Query.Type.NULL_TYPE) {
            c.setTyp(qt);
        }
        return c;
    }

    static Uncertain createUncertain(TableSet direct, TableSet recursive) {
        return new Uncertain(direct, recursive);
    }
}
