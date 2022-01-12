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

package com.jd.jdbc.concurrency;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.jd.jdbc.context.VtContextConstant.BEGIN_EXECUTION_CANCELLED;
import static com.jd.jdbc.context.VtContextConstant.EXECUTION_CANCELLED;
import static com.jd.jdbc.context.VtContextConstant.STREAM_EXECUTION_CANCELLED;

public class AllErrorRecorder implements ErrorRecorder {
    private final ConcurrentLinkedQueue<Exception> exceptionQueue = new ConcurrentLinkedQueue<>();

    private SQLException canceledException;

    /**
     * @param e
     */
    @Override
    public void recordError(Exception e) {
        if (e == null) {
            return;
        }
        if (e instanceof SQLException && e.getMessage() != null
            && (e.getMessage().contains(EXECUTION_CANCELLED) || e.getMessage().contains(STREAM_EXECUTION_CANCELLED) || e.getMessage().contains(BEGIN_EXECUTION_CANCELLED))) {
            canceledException = (SQLException) e;
            return;
        }
        exceptionQueue.add(e);
    }

    /**
     * @return
     */
    @Override
    public Boolean hasErrors() {
        return !exceptionQueue.isEmpty() || canceledException != null;
    }

    /**
     * @return
     */
    @Override
    public SQLException error() {
        return this.aggrError(
            exceptionQueue -> new SQLException(String.join(",", exceptionQueue.stream().map(Throwable::getMessage).collect(Collectors.toCollection(() -> new ArrayList<>(exceptionQueue.size()))))));
    }

    /**
     * @param function
     * @return
     */
    public SQLException aggrError(Function<ConcurrentLinkedQueue<Exception>, SQLException> function) {
        if (exceptionQueue.isEmpty()) {
            return null;
        }
        return function.apply(exceptionQueue);
    }

    /**
     * @return
     */
    public List<String> errorStrings() {
        if (exceptionQueue.isEmpty()) {
            return null;
        }
        return exceptionQueue.stream().map(Throwable::getMessage).collect(Collectors.toList());
    }

    /**
     * @return
     */
    public ConcurrentLinkedQueue<Exception> getErrors() {
        return this.exceptionQueue;
    }

    public void throwException() throws SQLException {
        if (!exceptionQueue.isEmpty()) {
            // throw allErrors.error();
            Exception exception = this.getErrors().peek();
            if (exception instanceof SQLException) {
                throw (SQLException) exception;
            } else {
                throw new SQLException(exception);
            }
        } else if (canceledException != null) {
            throw canceledException;
        }
    }
}
