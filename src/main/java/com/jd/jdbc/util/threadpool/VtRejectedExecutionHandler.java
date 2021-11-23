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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class VtRejectedExecutionHandler implements RejectedExecutionHandler {
    private static final Log LOGGER = LogFactory.getLog(VtRejectedExecutionHandler.class);

    private static final String THREAD_POOL_NAME_SUFFIX = "ThreadPool";

    private static final long TEN_MINUTES_MILLS = 10 * 60 * 1000;

    private static final Semaphore GUARD = new Semaphore(1);

    private static final String OS_WIN_PREFIX = "win";

    private static final String USER_HOME_KEY = "user.home";

    private static final String OS_NAME_KEY = "os.name";

    private static final String WIN_DATETIME_FORMAT = "yyyy-MM-dd_HH-mm-ss";

    private static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd_HH:mm:ss";

    private static volatile long lastPrintTime = 0;

    private final String threadPoolName;

    private final Long timeout;

    public VtRejectedExecutionHandler(final String threadPoolName, final Long timeout) {
        this.threadPoolName = threadPoolName + THREAD_POOL_NAME_SUFFIX;
        this.timeout = timeout;
    }

    @Override
    public void rejectedExecution(final Runnable r, final ThreadPoolExecutor executor) {
        try {
            executor.getQueue().offer(r, this.timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOGGER.warn("Interrupted", e);
            throw new RejectedExecutionException("Interrupted", e);
        }
        String msg = String.format("%s - Pool Size: %d (active: %d, core: %d, max: %d, largest: %d)," +
                " Task: %d (completed: %d), " +
                "Executor status:(isShutdown:%s, isTerminated:%s, isTerminating:%s)!",
            this.threadPoolName, executor.getPoolSize(), executor.getActiveCount(), executor.getCorePoolSize(),
            executor.getMaximumPoolSize(), executor.getLargestPoolSize(), executor.getTaskCount(),
            executor.getCompletedTaskCount(), executor.isShutdown(), executor.isTerminated(),
            executor.isTerminating());
        LOGGER.warn(msg);
        dumpJvmStack();
        throw new RejectedExecutionException(String.format("Time out %d(ms)! %s", this.timeout, msg));
    }

    private void dumpJvmStack() {
        long now = System.currentTimeMillis();

        // dump every 10 minutes
        if (now - lastPrintTime < TEN_MINUTES_MILLS) {
            return;
        }

        if (!GUARD.tryAcquire()) {
            return;
        }

        ThreadPoolExecutor pool = new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder()
                .setNameFormat("DumpJvmStack-%d")
                .build()
        );
        pool.execute(() -> {
            String dumpPath = System.getProperty(USER_HOME_KEY);
            String os = System.getProperty(OS_NAME_KEY).toLowerCase();

            // window system don't support ":" in file name
            SimpleDateFormat sdf;
            if (os.contains(OS_WIN_PREFIX)) {
                sdf = new SimpleDateFormat(WIN_DATETIME_FORMAT);
            } else {
                sdf = new SimpleDateFormat(DEFAULT_DATETIME_FORMAT);
            }

            String dateStr = sdf.format(new Date());
            //try-with-resources
            try (FileOutputStream jStackStream = new FileOutputStream(
                new File(dumpPath, "VtDriver_JStack.log" + "." + dateStr))) {
                JvmUtil.jstack(jStackStream);
            } catch (Throwable t) {
                LOGGER.error("dump jStack error", t);
            } finally {
                GUARD.release();
            }
            lastPrintTime = System.currentTimeMillis();
        });
        // must shutdown thread pool, if not will lead to OOM
        pool.shutdown();
    }
}
