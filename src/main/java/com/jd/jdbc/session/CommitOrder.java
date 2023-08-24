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

public enum CommitOrder {
    /**
     * <pre>
     * NORMAL is the default commit order.
     * </pre>
     *
     * <code>NORMAL = 0;</code>
     */
    NORMAL(0),
    /**
     * <pre>
     * PRE is used to designate pre_sessions.
     * </pre>
     *
     * <code>PRE = 1;</code>
     */
    PRE(1),
    /**
     * <pre>
     * POST is used to designate post_sessions.
     * </pre>
     *
     * <code>POST = 2;</code>
     */
    POST(2),
    /**
     * <pre>
     * AUTOCOMMIT is used to run the statement as autocommitted transaction.
     * </pre>
     *
     * <code>AUTOCOMMIT = 3;</code>
     */
    AUTOCOMMIT(3),
    UNRECOGNIZED(-1);

    private final int value;

    CommitOrder(int value) {
        this.value = value;
    }

}