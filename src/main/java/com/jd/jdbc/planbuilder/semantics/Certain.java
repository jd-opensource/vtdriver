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

import java.sql.SQLException;
import java.util.Objects;

public class Certain extends Dependency implements Dependencies {
    private SQLException err;

    public Certain(TableSet direct, TableSet recursive) {
        super(direct, recursive);
    }

    @Override
    public boolean empty() {
        return false;
    }

    @Override
    public Dependency get() throws SQLException {
        if (err != null) {
            throw err;
        }
        return this;
    }

    @Override
    public Dependencies merge(Dependencies d, boolean allowMulti) {
        if (d instanceof Certain) {
            if (Objects.equals(((Certain) d).getRecursive(), super.getRecursive())) {
                return this;
            }
            this.getDirect().mergeInPlace(((Certain) d).getDirect());
            this.getRecursive().mergeInPlace(((Certain) d).getRecursive());
            if (!allowMulti) {
                err = new Exception.AmbiguousException("ambiguous");
            }
            return this;
        }
        return this;
    }
}
