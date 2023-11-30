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

package com.jd.jdbc.planbuilder;

import com.jd.jdbc.VSchemaManager;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;

public class PlanPerformanceTest extends AbstractPlanTest {

    private VSchemaManager vm = null;

    @Before
    public void init() throws IOException {
        vm = loadSchema("src/test/resources/plan/plan_schema.json");
    }

    @Test
    public void testOnce() throws Exception {
        long sTime = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            PlanTest.build("select a, b, count(*) from user group by a order by b limit 10", vm, true);
        }
        long eTime = System.currentTimeMillis();

        System.out.println("总耗时: " + (eTime - sTime) + "(ms)");
    }
}
