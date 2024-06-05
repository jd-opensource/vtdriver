/*
Copyright 2024 JD Project Authors.

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

package com.jd.jdbc.util;

import com.jd.jdbc.util.threadpool.VtThreadFactoryBuilder;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ScheduledManager {

    private ScheduledThreadPoolExecutor scheduledExecutor;

    private TimeUnit timeUnit;

    public ScheduledManager() {
        scheduledExecutor = new ScheduledThreadPoolExecutor(1, new VtThreadFactoryBuilder.DefaultThreadFactory("default-schedule", true));
        scheduledExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        scheduledExecutor.setRemoveOnCancelPolicy(true);
        timeUnit = TimeUnit.MINUTES;
    }

    public ScheduledManager(String threadName, int poolSize, TimeUnit timeInternal) {
        scheduledExecutor = new ScheduledThreadPoolExecutor(poolSize, new VtThreadFactoryBuilder.DefaultThreadFactory(threadName, true));
        scheduledExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        scheduledExecutor.setRemoveOnCancelPolicy(true);
        timeUnit = timeInternal;
    }

    public void close() {
        scheduledExecutor.shutdownNow();
        try {
            int tryAgain = 3;
            while (tryAgain > 0 && !scheduledExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                tryAgain--;
            }
        } catch (InterruptedException e) {
            // We're shutting down anyway, so just ignore.
        }
    }
}
