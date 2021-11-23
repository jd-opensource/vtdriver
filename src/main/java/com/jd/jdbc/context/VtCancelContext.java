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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class VtCancelContext extends VtContext {
    protected VtContext parent;

    protected Set<VtContext> children;

    protected Map<Object, Object> contextValues;

    protected volatile Date deadline;

    protected boolean isDone;

    protected volatile String error;

    protected Lock lock = new ReentrantLock();

    public VtCancelContext(IContext parent, Map<Object, Object> contextValues) {
        this.parent = (VtContext) parent;
        /**
         Note: this is the only way of creating thread-safe Set in Java.
         https://javatutorial.net/java-concurrenthashset-example
         **/
        ConcurrentHashMap<VtContext, Integer> concurrentHashMap = new ConcurrentHashMap();
        this.children = ConcurrentHashMap.newKeySet();
        this.contextValues = contextValues;
        this.deadline = parent.getDeadline();
        this.parent.addChild(this);
        this.isDone = parent.isDone();
        this.error = parent.error();
    }

    /**
     * cancel context with reason.
     * side effect: all derived contexts will be canceled as well.
     */
    @Override
    public void cancel(String reason) {
        lock.lock();
        try {
            if (this.isDone) {
                return;
            }
            propagate(false, reason);
            this.error = reason;
            this.isDone = true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * indicate if the context has been done.
     * side effect: context and derived contexts may be canceled.
     */
    @Override
    public boolean isDone() {
        lock.lock();
        try {
            if (this.isDone) {
                return true;
            }
            if (deadline != null && !deadline.after(new Date())) {
                String reason = "dead line";
                propagate(false, reason);
                this.error = reason;
                this.isDone = true;
            }
            return this.isDone;
        } finally {
            lock.unlock();
        }
    }

    /**
     * get the reason for cancel.
     */
    @Override
    public String error() {
        return this.error;
    }


    /**
     * get context value by key. if the key is not found in current context, search it up to root context.
     */
    @Override
    public Object getContextValue(Object key) {
        Object contextValue = contextValues.get(key);
        if (contextValue == null) {
            contextValue = parent.getContextValue(key);
        }
        return contextValue;
    }

    /**
     * set context value by key.
     */
    @Override
    public void setContextValue(Object key, Object value) {
        contextValues.put(key, value);
    }

    /**
     * get the deadline. return null if not applicable.
     */
    @Override
    public Date getDeadline() {
        return this.deadline;
    }

    @Override
    protected void addChild(IContext child) {
        lock.lock();
        try {
            if (!this.isDone()) {
                this.children.add((VtContext) child);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void removeChild(IContext child) {
        this.children.remove(child);
    }

    @Override
    protected void cancelFromParent(String reason) {
        lock.lock();
        try {
            if (this.isDone) {
                return;
            }
            propagate(true, reason);
            this.error = reason;
            this.isDone = true;
        } finally {
            lock.unlock();
        }
    }

    protected void propagate(boolean fromParent, String reason) {
        if (!fromParent) {
            this.parent.removeChild(this);
        }
        for (VtContext child : this.children) {
            child.cancelFromParent(reason);
        }
        this.children.clear();
    }
}
