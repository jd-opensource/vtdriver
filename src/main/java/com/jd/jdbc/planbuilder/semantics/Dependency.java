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

import lombok.Getter;
import lombok.Setter;

public class Dependency {
    @Getter
    private TableSet direct;

    @Getter
    private TableSet recursive;

    @Setter
    @Getter
    private Type typ;

    public Dependency(TableSet direct, TableSet recursive) {
        this.direct = direct;
        this.recursive = recursive;
    }

    public Dependency() {
        this.direct = new TableSet(0, null);
        this.recursive = new TableSet(0, null);
    }
}
