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

package com.jd.jdbc.threadpool;

import com.jd.jdbc.monitor.ThreadPoolCollector;
import com.jd.jdbc.util.threadpool.InitThreadPoolService;
import io.prometheus.client.Collector;
import java.util.List;
import static junit.framework.TestCase.fail;
import org.junit.Test;
import testsuite.TestSuite;

public class InitConnectionPoolParams extends TestSuite {

    @Test
    public void testInitThreadPoolParams() {
        String queryCoreSize = "5";
        String queryMaximumSize = "101";
        String queryQueueSize = "1001";
        String queryRejectedTimeout = "3001";
        System.setProperty("queryCoreSize", queryCoreSize);
        System.setProperty("queryMaximumSize", queryMaximumSize);
        System.setProperty("queryQueueSize", queryQueueSize);
        System.setProperty("queryRejectedTimeout", queryRejectedTimeout);
        System.setProperty("healthCheckCoreSize", queryCoreSize);
        System.setProperty("healthCheckMaximumSize", queryMaximumSize);
        System.setProperty("healthCheckQueueSize", queryQueueSize);
        System.setProperty("healthCheckRejectedTimeout", queryRejectedTimeout);
        InitThreadPoolService.getInstance();
        int num = 0;
        ThreadPoolCollector t = ThreadPoolCollector.getInstance();
        List<Collector.MetricFamilySamples> collects = t.collect();
        for (Collector.MetricFamilySamples c : collects) {
            if (c.name.equals("thread_pool_core_size")) {
                List<Collector.MetricFamilySamples.Sample> samples = c.samples;
                for (Collector.MetricFamilySamples.Sample s : samples) {
                    if (s.labelValues.get(0).equals("QueryTask-") || s.labelValues.get(0).equals("HealthCheckTask-")) {
                        if (s.value == Double.parseDouble(queryCoreSize)) {
                            System.out.println(s.labelValues + ":thread_pool_core_size " + s.value);
                            num++;
                        } else {
                            fail(s.labelValues + ":thread_pool_core_size " + s.value);
                        }
                    }
                }
            } else if (c.name.equals("thread_pool_max_size")) {
                List<Collector.MetricFamilySamples.Sample> samples = c.samples;
                for (Collector.MetricFamilySamples.Sample s : samples) {
                    if (s.labelValues.get(0).equals("QueryTask-") || s.labelValues.get(0).equals("HealthCheckTask-")) {
                        if (s.value == Double.parseDouble(queryMaximumSize)) {
                            System.out.println(s.labelValues + ":thread_pool_max_size " + s.value);
                            num++;
                        } else {
                            fail(s.labelValues + ":thread_pool_max_size " + s.value);
                        }
                    }
                }
            } else if (c.name.equals("thread_pool_queue_remainingCapacity")) {
                List<Collector.MetricFamilySamples.Sample> samples = c.samples;
                for (Collector.MetricFamilySamples.Sample s : samples) {
                    if (s.labelValues.get(0).equals("QueryTask-") || s.labelValues.get(0).equals("HealthCheckTask-")) {
                        if (s.value == Double.parseDouble(queryQueueSize)) {
                            System.out.println(s.labelValues + ":thread_pool_queue_remainingCapacity " + s.value);
                            num++;
                        } else {
                            fail(s.labelValues + ":thread_pool_queue_remainingCapacity " + s.value);
                        }
                    }
                }
            } else if (c.name.equals("thread_pool_vtrejected_handler_timeout")) {
                List<Collector.MetricFamilySamples.Sample> samples = c.samples;
                for (Collector.MetricFamilySamples.Sample s : samples) {
                    if (s.labelValues.get(0).equals("QueryTask-") || s.labelValues.get(0).equals("HealthCheckTask-")) {
                        if (s.value == Double.parseDouble(queryRejectedTimeout)) {
                            System.out.println(s.labelValues + ":thread_pool_vtrejected_handler_timeout " + s.value);
                            num++;
                        } else {
                            fail(s.labelValues + ":thread_pool_vtrejected_handler_timeout " + s.value);
                        }
                    }
                }
            }
        }
        if (num != 8) {
            fail("testInitThreadPoolParams is [FAIL]");
        }
    }

}
