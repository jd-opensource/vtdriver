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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class VtContext implements IContext {
    public VtContext() {
    }

    public static IContext background() {
        return new VtBackgroundContext();
    }

    public static IContext withCancel(IContext parent) {
        return new VtCancelContext(parent, new HashMap<>());
    }

    public static IContext withCancel(IContext parent, Map<Object, Object> values) {
        return new VtCancelContext(parent, values);
    }

    public static IContext withDeadline(IContext parent, Date deadline) {
        return new VtDeadlineContext(parent, deadline, new HashMap<>(16));
    }

    public static IContext withDeadline(IContext parent, Date deadline, Map<Object, Object> values) {
        return new VtDeadlineContext(parent, deadline, values);
    }

    public static IContext withDeadline(IContext parent, long later, TimeUnit unit) {
        Date deadline = new Date(System.currentTimeMillis() + unit.toMillis(later));
        return new VtDeadlineContext(parent, deadline, new HashMap<>(16));
    }

    public static IContext withDeadline(IContext parent, long later, TimeUnit unit, Map<Object, Object> values) {
        Date deadline = new Date(System.currentTimeMillis() + unit.toMillis(later));
        return new VtDeadlineContext(parent, deadline, values);
    }

    protected void addChild(IContext child) {
    }

    protected void removeChild(IContext child) {
    }

    protected void cancelFromParent(String reason) {
    }

    @Override
    public void close() {
        cancel("closed");
    }
}
