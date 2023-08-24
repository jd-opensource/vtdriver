/*
Copyright 2021 JD Project Authors. Licensed under Apache-2.0.

Copyright 2019 The Vitess Authors.

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

package com.jd.jdbc.topo.discovery;

import com.jd.jdbc.context.IContext;
import com.jd.jdbc.context.VtContext;
import com.jd.jdbc.discovery.HealthCheck;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class TestHealthCheck {

    private final HealthCheck hc = HealthCheck.INSTANCE;

    @Ignore
    @Test
    public void waitForAllServingTabletsTimeoutTest() throws Exception {
        IContext ctx = VtContext.withCancel(VtContext.background());
        List<Query.Target> targetList = new CopyOnWriteArrayList<>();
        targetList.add(Query.Target.newBuilder()
            .setCell("aa")
            .setKeyspace("vtdriver")
            .setShard("-80")
            .setTabletType(Topodata.TabletType.MASTER)
            .build());

        ExecutorService fixedThreadPool = new ThreadPoolExecutor(1, 1,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());
        FutureTask<Boolean> task = new FutureTask<>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws SQLException {
                try {
                    hc.waitForAllServingTablets(VtContext.withDeadline(ctx, 30, TimeUnit.SECONDS), targetList);
                    return false;
                } catch (SQLException e) {
                    return true;
                }
            }
        });

        fixedThreadPool.submit(task);
        Thread.sleep(35000);
        if (task.isDone()) {
            Assert.assertTrue(task.get());
        } else {
            Assert.fail();
        }
    }
}
