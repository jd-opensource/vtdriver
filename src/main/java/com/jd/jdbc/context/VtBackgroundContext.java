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

public class VtBackgroundContext extends VtContext {
    public VtBackgroundContext() {
    }

    @Override
    protected void addChild(IContext child) {

    }

    @Override
    protected void removeChild(IContext child) {

    }

    @Override
    public boolean isDone() {
        return false;
    }

    /**
     * get the reason for cancel.
     */
    @Override
    public String error() {
        return null;
    }

    /**
     * cancel context with reason.
     * side effect: all derived contexts will be canceled as well.
     *
     * @param reason
     */
    @Override
    public void cancel(String reason) {

    }

    @Override
    public Object getContextValue(Object key) {
        return null;
    }

    /**
     * set context value by key.
     *
     * @param key
     * @param value
     */
    @Override
    public void setContextValue(Object key, Object value) {

    }

    @Override
    public Date getDeadline() {
        return null;
    }
}
