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

package com.jd.jdbc.vindexes;

import lombok.Data;

@Data
public class VKeyspace {

    private String name;

    private Boolean sharded;

    public VKeyspace(String name) {
        this.name = name;
    }

    public VKeyspace(String name, Boolean sharded) {
        this.name = name;
        this.sharded = sharded;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof VKeyspace) {
            return this.name == ((VKeyspace) obj).name && this.sharded == ((VKeyspace) obj).sharded;
        }
        return false;
    }
}
