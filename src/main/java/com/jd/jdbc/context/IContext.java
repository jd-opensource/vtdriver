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

package com.jd.jdbc.context;

import java.util.Date;

public interface IContext extends AutoCloseable {
    /**
     * indicate if the context has been done.
     * side effect: context and derived contexts may be canceled.
     */
    boolean isDone();

    /**
     * get the reason for cancel.
     */
    String error();

    /**
     * cancel context with reason.
     * side effect: all derived contexts will be canceled as well.
     */
    void cancel(String reason);

    /**
     * get context value by key. if the key is not found in current context, search it up to root context.
     */
    Object getContextValue(Object key);

    /**
     * set context value by key.
     */
    void setContextValue(Object key, Object value);

    /**
     * get the deadline. return null if not applicable.
     */
    Date getDeadline();

    /**
     * invoke cancel.
     */
    @Override
    void close();
}
