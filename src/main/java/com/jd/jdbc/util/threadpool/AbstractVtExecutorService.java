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

public abstract class AbstractVtExecutorService {
    public static final Integer DEFAULT_CORE_POOL_SIZE = Integer.min(10, Runtime.getRuntime().availableProcessors());

    public static final Integer DEFAULT_MAXIMUM_POOL_SIZE = DEFAULT_CORE_POOL_SIZE * 10;

    public static final Long DEFAULT_KEEP_ALIVE_TIME_MILLIS = 60 * 1000L;

    public static final Integer DEFAULT_QUEUE_SIZE = 1000;

    public static final Long DEFAULT_REJECTED_EXECUTION_TIMEOUT_MILLIS = 3 * 1000L;
}
