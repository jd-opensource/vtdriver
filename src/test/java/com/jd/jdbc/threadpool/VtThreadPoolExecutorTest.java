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

import com.jd.jdbc.util.threadpool.AbstractVtExecutorService;
import com.jd.jdbc.util.threadpool.impl.VtQueryExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import testsuite.TestSuite;

public class VtThreadPoolExecutorTest extends TestSuite {

    @Test
    public void testRejectedExecution() {
        printInfo("Test VtDriver thread pool executor rejected execution!");
        VtQueryExecutorService.initialize(null, null, null, null);
        int max = AbstractVtExecutorService.DEFAULT_MAXIMUM_POOL_SIZE + AbstractVtExecutorService.DEFAULT_QUEUE_SIZE;
        for (int i = 0; i < max; i++) {
            VtQueryExecutorService.execute(() -> {
                try {
                    TimeUnit.SECONDS.sleep(11);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }

        boolean beAbandoned = false;
        try {
            VtQueryExecutorService.execute(() -> System.out.println("10s after, I will be abandoned -_-!"));
        } catch (Exception e) {
            Assert.assertTrue(printFail("[FAIL.1]"), e instanceof RejectedExecutionException);
            beAbandoned = true;
        }

        if (beAbandoned) {
            printOk(Boolean.TRUE);
        } else {
            System.out.println(printFail("[FAIL.2]"));
        }
    }
}
