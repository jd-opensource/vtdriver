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

import com.jd.jdbc.sqlparser.utils.StringUtils;

public class JdkUtil {
    private static int queryExecutorCoreSize;

    private static final int availableProcessors = Runtime.getRuntime().availableProcessors();

    private static final int MAX_QUERY_EXECUTOR_CORE_SIZE = 32;

    private static final int MIN_QUERY_EXECUTOR_CORE_SIZE = 8;

    private static final int DEFAULT_QUERY_EXECUTOR_COLE_SIZE = 8;

    private static final String applicationCurrentJdkVersion = System.getProperty("java.version");

    private static final int[] catGetContainerCoreJdkArray = new int[] {1, 8, 0, 131};

    static {
        initQueryExecutorCorePoolSize();
    }

    /**
     * If the current application is deployed in Docker, after jdk1.8.0_131,
     * the number of Docker cores can be obtained by Runtime.getRuntime().availableProcessors()
     */
    private static boolean canGetContainerCoreByJdk() {
        if (StringUtils.isEmpty(applicationCurrentJdkVersion) || !applicationCurrentJdkVersion.contains(".")) {
            return false;
        }
        String[] applicationCurrentJdkArray = applicationCurrentJdkVersion.split("\\.|_");
        int count = Math.min(applicationCurrentJdkArray.length, catGetContainerCoreJdkArray.length);
        try {
            for (int i = 0; i < count; i++) {
                int applicationCurrentJdk = Integer.parseInt(applicationCurrentJdkArray[i]);
                if (catGetContainerCoreJdkArray[i] == applicationCurrentJdk) {
                    continue;
                }
                return catGetContainerCoreJdkArray[i] < applicationCurrentJdk;
            }
            return applicationCurrentJdkArray.length == catGetContainerCoreJdkArray.length;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * Set the default number of core threads in the thread pool
     * If the number of Docker cores cannot be obtained by the JDK version or JDK version number resolution failed,
     * the number of core threads is set to MIN_QUERY_COLE_SIZE.
     * If the number of Docker cores can be obtained by the JDK version:
     * 1. The number of Docker cores < {@link #MIN_QUERY_EXECUTOR_CORE_SIZE}, the number of core threads is set to {@link #MIN_QUERY_EXECUTOR_CORE_SIZE}
     * 2. The number of Docker cores > {@link #MAX_QUERY_EXECUTOR_CORE_SIZE}, the number of core threads is set to {@link #MAX_QUERY_EXECUTOR_CORE_SIZE}
     * 3. The number of core threads is set to the number of Docker cores
     */
    private static void initQueryExecutorCorePoolSize() {
        if (canGetContainerCoreByJdk()) {
            if (availableProcessors < MIN_QUERY_EXECUTOR_CORE_SIZE) {
                queryExecutorCoreSize = MIN_QUERY_EXECUTOR_CORE_SIZE;
            } else {
                queryExecutorCoreSize = Math.min(availableProcessors, MAX_QUERY_EXECUTOR_CORE_SIZE);
            }
        } else {
            queryExecutorCoreSize = DEFAULT_QUERY_EXECUTOR_COLE_SIZE;
        }
    }

    public static int getQueryExecutorCorePoolSize() {
        return queryExecutorCoreSize;
    }
}
