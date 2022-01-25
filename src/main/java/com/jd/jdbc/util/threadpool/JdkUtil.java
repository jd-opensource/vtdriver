/*
Copyright 2021 JD Project Authors.

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

package com.jd.jdbc.util.threadpool;

public class JdkUtil {
    private static int queryExecutorCoreSize;

    private static final int availableProcessors = Runtime.getRuntime().availableProcessors();

    private static final int MAX_QUERY_EXECUTOR_CORE_SIZE = 32;

    private static final int MIN_QUERY_EXECUTOR_CORE_SIZE = 8;

    static {
        initQueryExecutorCorePoolSize();
    }

    /**
     * Set the default number of core threads in the thread pool
     * 1. The number of {@link #availableProcessors} < {@link #MIN_QUERY_EXECUTOR_CORE_SIZE}, the number of {@link #queryExecutorCoreSize} is set to {@link #MIN_QUERY_EXECUTOR_CORE_SIZE}
     * 2. The number of {@link #availableProcessors} > {@link #MAX_QUERY_EXECUTOR_CORE_SIZE}, the number of {@link #queryExecutorCoreSize} is set to {@link #MAX_QUERY_EXECUTOR_CORE_SIZE}
     * 3. The number of core threads is set to the number of {@link #availableProcessors}
     */
    private static void initQueryExecutorCorePoolSize() {
        queryExecutorCoreSize = availableProcessors < MIN_QUERY_EXECUTOR_CORE_SIZE ? MIN_QUERY_EXECUTOR_CORE_SIZE : Math.min(availableProcessors, MAX_QUERY_EXECUTOR_CORE_SIZE);
    }

    public static int getQueryExecutorCorePoolSize() {
        return queryExecutorCoreSize;
    }
}
