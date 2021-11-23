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

package com.jd.jdbc.engine;

public enum DMLOpcode {
    // Unsharded is for routing a dml statement
    // to an unsharded keyspace.
    Unsharded(0),
    // Equal is for routing an dml statement to a single shard.
    // Requires: A Vindex, and a single Value.
    Equal(1),
    // In is for routing an dml statement to a multi shard.
    // Requires: A Vindex, and a multi Values.
    In(2),
    // Scatter is for routing a scattered dml statement.
    Scatter(3),
    // ByDestination is to route explicitly to a given target destination.
    // Is used when the query explicitly sets a target destination:
    // in the clause e.g: UPDATE `keyspace[-]`.x1 SET foo=1
    ByDestination(4);

    private final int code;

    DMLOpcode(int code) {
        this.code = code;
    }
}