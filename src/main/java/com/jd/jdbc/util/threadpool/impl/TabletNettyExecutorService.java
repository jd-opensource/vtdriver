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
import com.jd.jdbc.util.threadpool.VtThreadFactoryBuilder;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TabletNettyExecutorService extends AbstractVtExecutorService {
    private static final String QUERY_TASK_NAME_FORMAT = "Netty-ExecutorService-";

    private static final ThreadPoolExecutor NETTY_EXECUTOR_SERVICE;

    static {
        NETTY_EXECUTOR_SERVICE = new ThreadPoolExecutor(
            DEFAULT_QUERY_CORE_POOL_SIZE,
            DEFAULT_QUERY_MAXIMUM_POOL_SIZE,
            DEFAULT_KEEP_ALIVE_TIME_MILLIS,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(DEFAULT_QUEUE_SIZE),
            VtThreadFactoryBuilder.build(QUERY_TASK_NAME_FORMAT));
        NETTY_EXECUTOR_SERVICE.prestartAllCoreThreads();
        ThreadPoolCollector.getInstance().add(QUERY_TASK_NAME_FORMAT, NETTY_EXECUTOR_SERVICE);
    }

    public static ThreadPoolExecutor getNettyExecutorService() {
        return NETTY_EXECUTOR_SERVICE;
    }

    private void shutdown() {
        if (NETTY_EXECUTOR_SERVICE.isShutdown()) {
            return;
        }
        try {
            NETTY_EXECUTOR_SERVICE.shutdownNow();
        } catch (Exception ignore) {

        }
    }
}
