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

package com.jd.jdbc.util.threadpool.impl;

import com.jd.jdbc.monitor.ThreadPoolCollector;
import com.jd.jdbc.util.threadpool.AbstractVtExecutorService;
import com.jd.jdbc.util.threadpool.VtRejectedExecutionHandler;
import com.jd.jdbc.util.threadpool.VtThreadFactoryBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VtDaemonExecutorService extends AbstractVtExecutorService {
    private static final String QUERY_TASK_NAME_FORMAT = "DaemonTask-";

    private volatile static ExecutorService executorService;

    public static void initialize(Integer corePoolSize, Integer maximumPoolSize, Long rejectedExecutionTimeoutMillis) {
        if (executorService == null) {
            synchronized (VtDaemonExecutorService.class) {
                if (executorService == null) {
                    if (corePoolSize == null || corePoolSize <= 0) {
                        corePoolSize = DEFAULT_DAEMON_CORE_POOL_SIZE;
                    }
                    if (maximumPoolSize == null || maximumPoolSize <= 0 || maximumPoolSize < corePoolSize) {
                        maximumPoolSize = DEFAULT_DAEMON_MAXIMUM_POOL_SIZE;
                        if (maximumPoolSize < corePoolSize) {
                            maximumPoolSize += corePoolSize;
                        }
                    }
                    if (rejectedExecutionTimeoutMillis == null || rejectedExecutionTimeoutMillis <= 0) {
                        rejectedExecutionTimeoutMillis = DEFAULT_REJECTED_EXECUTION_TIMEOUT_MILLIS;
                    }
                    executorService = new ThreadPoolExecutor(
                        corePoolSize,
                        maximumPoolSize,
                        DEFAULT_KEEP_ALIVE_TIME_MILLIS,
                        TimeUnit.MILLISECONDS,
                        new SynchronousQueue<>(),
                        VtThreadFactoryBuilder.build(QUERY_TASK_NAME_FORMAT),
                        new VtRejectedExecutionHandler(QUERY_TASK_NAME_FORMAT, rejectedExecutionTimeoutMillis));
                    ThreadPoolCollector.getInstance().add(QUERY_TASK_NAME_FORMAT, (ThreadPoolExecutor) executorService);
                }
            }
        }
    }

    public static void execute(Runnable command) {
        executorService.execute(command);
    }
}
