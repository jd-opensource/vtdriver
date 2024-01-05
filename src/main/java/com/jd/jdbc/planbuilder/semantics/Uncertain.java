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
import lombok.Getter;

public class Uncertain extends Dependency implements Dependencies {
    @Getter
    private boolean fail;

    public Uncertain(TableSet direct, TableSet recursive) {
        super(direct, recursive);
        this.fail = false;
    }

    @Override
    public boolean empty() {
        return false;
    }

    @Override
    public Dependency get() throws SQLException {
        if (fail) {
            throw new Exception.AmbiguousException("ambiguous");
        }
        return this;
    }

    @Override
    public Dependencies merge(Dependencies d, boolean allowMulti) {
        if (d instanceof Uncertain) {
            // 这里是用!=还是equals
            if (((Uncertain) d).getRecursive() != super.getRecursive()) {
                this.fail = true;
            }
            return this;
        }
        if (d instanceof Certain) {
            return d;
        }
        return this;
    }
}
