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

package com.jd.jdbc.monitor;


import com.jd.jdbc.util.threadpool.VtRejectedExecutionHandler;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;

public final class ThreadPoolCollector extends Collector {

    private static final List<String> LABEL_NAMES = Collections.singletonList("threadPool");

    private static final Map<String, ThreadPoolExecutor> THREAD_POOL_EXECUTOR_MAP = new ConcurrentHashMap<>();

    private static final ThreadPoolCollector threadPoolCollector = new ThreadPoolCollector();

    private ThreadPoolCollector() {
    }

    public static ThreadPoolCollector getInstance() {
        return threadPoolCollector;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        return Arrays.asList(
            createGauge("thread_pool_core_size", "thread.pool.core.size", ThreadPoolExecutor::getCorePoolSize),
            createGauge("thread_pool_largest_size", "thread.pool.largest.size", ThreadPoolExecutor::getLargestPoolSize),
            createGauge("thread_pool_max_size", "thread.pool.max.size", ThreadPoolExecutor::getMaximumPoolSize),
            createGauge("thread_pool_active_size", "thread.pool.active.size", ThreadPoolExecutor::getActiveCount),
            createGauge("thread_pool_thread_count", "thread.pool.thread.count", ThreadPoolExecutor::getPoolSize),
            createGauge("thread_pool_queue_size", "thread.pool.queue.size", e -> e.getQueue().size()),
            createGauge("thread_pool_queue_remainingCapacity", "thread.pool.queue.remainingCapacity", e -> e.getQueue().remainingCapacity()),
            createGauge("thread_pool_vtrejected_handler_timeout", "thread.pool.vtrejected.handler.timeout", e -> {
                if (!(e.getRejectedExecutionHandler() instanceof VtRejectedExecutionHandler)) {
                    return 0;
                }
                Long timeout = ((VtRejectedExecutionHandler) e.getRejectedExecutionHandler()).getTimeout();
                return Math.toIntExact(timeout);
            }));
    }

    public void add(final String name, final ThreadPoolExecutor executor) {
        THREAD_POOL_EXECUTOR_MAP.put(name, executor);
    }

    public void remove(final String name) {
        THREAD_POOL_EXECUTOR_MAP.remove(name);
    }

    private GaugeMetricFamily createGauge(final String metric, final String help,
                                          final Function<ThreadPoolExecutor, Integer> metricValueFunction) {
        GaugeMetricFamily metricFamily = new GaugeMetricFamily(metric, help, LABEL_NAMES);
        THREAD_POOL_EXECUTOR_MAP.forEach((k, v) -> metricFamily.addMetric(
            Collections.singletonList(k),
            metricValueFunction.apply(v)
        ));
        return metricFamily;
    }
}
