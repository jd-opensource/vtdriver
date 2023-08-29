/*
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

package com.jd.jdbc.session;

public enum TransactionMode {
    /**
     * <pre>
     * UNSPECIFIED uses the transaction mode set by the VTGate flag 'transaction_mode'.
     * </pre>
     *
     * <code>UNSPECIFIED = 0;</code>
     */
    UNSPECIFIED(0),
    /**
     * <pre>
     * SINGLE disallows distributed transactions.
     * </pre>
     *
     * <code>SINGLE = 1;</code>
     */
    SINGLE(1),
    /**
     * <pre>
     * MULTI allows distributed transactions with best effort commit.
     * </pre>
     *
     * <code>MULTI = 2;</code>
     */
    MULTI(2),
    /**
     * <pre>
     * TWOPC is for distributed transactions with atomic commits.
     * </pre>
     *
     * <code>TWOPC = 3;</code>
     */
    TWOPC(3),
    UNRECOGNIZED(-1);

    private final int value;

    TransactionMode(int value) {
        this.value = value;
    }
}