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

package com.jd.jdbc.topo;

public enum TopoExceptionCode {
    /**
     * UNKNOWN_CODE
     */
    UNKNOWN_CODE(-1),
    /**
     * NODE_EXISTS
     */
    NODE_EXISTS(0),
    /**
     * NO_NODE
     */
    NO_NODE(1),
    /**
     * NODE_NOT_EMPTY
     */
    NODE_NOT_EMPTY(2),
    /**
     * TIMEOUT
     */
    TIMEOUT(3),
    /**
     * INTERRUPTED
     */
    INTERRUPTED(4),
    /**
     * BAD_VERSION
     */
    BAD_VERSION(5),
    /**
     * PARTIAL_RESULT
     */
    PARTIAL_RESULT(6),
    /**
     * NO_UPDATE_NEEDED
     */
    NO_UPDATE_NEEDED(7),
    /**
     * NO_IMPLEMENTATION
     */
    NO_IMPLEMENTATION(8);

    int errorCode;

    TopoExceptionCode(int errorCode) {
        this.errorCode = errorCode;
    }
}
