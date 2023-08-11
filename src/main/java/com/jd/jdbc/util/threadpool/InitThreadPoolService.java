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

import com.jd.jdbc.sqlparser.utils.Utils;
import com.jd.jdbc.util.threadpool.impl.VtHealthCheckExecutorService;
import com.jd.jdbc.util.threadpool.impl.VtQueryExecutorService;
import java.util.Properties;

public class InitThreadPoolService {

    private static InitThreadPoolService instance = new InitThreadPoolService();

    private InitThreadPoolService() { }

    public static InitThreadPoolService getInstance() {
        return instance;
    }

    private static Integer queryCorePoolSize;

    private static Integer queryMaximumPoolSize;

    private static Integer queryQueueSize;

    private static Long queryRejectedExecutionTimeoutMillis;

    private static Integer healthCheckCorePoolSize;

    private static Integer healthCheckMaximumPoolSize;

    private static Integer healthCheckQueueSize;

    private static Long healthCheckRejectedExecutionTimeoutMillis;

    static {
        Properties prop = System.getProperties();
        queryCorePoolSize = Utils.getInteger(prop, "queryCoreSize");
        queryMaximumPoolSize = Utils.getInteger(prop, "queryMaximumSize");
        queryQueueSize = Utils.getInteger(prop, "queryQueueSize");
        queryRejectedExecutionTimeoutMillis = Utils.getLong(prop, "queryRejectedTimeout");
        healthCheckCorePoolSize = Utils.getInteger(prop, "healthCheckCoreSize");
        healthCheckMaximumPoolSize = Utils.getInteger(prop, "healthCheckMaximumSize");
        healthCheckQueueSize = Utils.getInteger(prop, "healthCheckQueueSize");
        healthCheckRejectedExecutionTimeoutMillis = Utils.getLong(prop, "healthCheckRejectedTimeout");
        VtQueryExecutorService.initialize(queryCorePoolSize, queryMaximumPoolSize, queryQueueSize, queryRejectedExecutionTimeoutMillis);
        VtHealthCheckExecutorService.initialize(healthCheckCorePoolSize, healthCheckMaximumPoolSize, healthCheckQueueSize, healthCheckRejectedExecutionTimeoutMillis);
    }

}
