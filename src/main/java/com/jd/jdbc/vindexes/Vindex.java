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

/**
 * Vindex defines the interface required to register a vindex.
 */
public interface Vindex {

    /**
     * Cost is used by planbuilder to prioritize vindexes.
     * The cost can be 0 if the id is basically a keyspace id.
     * The cost can be 1 if the id can be hashed to a keyspace id.
     * The cost can be 2 or above if the id needs to be looked up
     * from an external data source. These guidelines are subject
     * to change in the future.
     *
     * @return Integer
     */
    Integer cost();

    /**
     * IsUnique returns true if the Vindex is unique.
     * A Unique Vindex is allowed to return non-unique values like
     * a keyrange. This is in situations where the vindex does not
     * have enough information to map to a keyspace id. If so, such
     * a vindex cannot be primary.
     *
     * @return Boolean
     */
    Boolean isUnique();

    /**
     * NeedsVCursor returns true if the Vindex makes calls into the
     * VCursor. Such vindexes cannot be used by vreplication.
     *
     * @return Boolean
     */
    Boolean needsVcursor();
}
