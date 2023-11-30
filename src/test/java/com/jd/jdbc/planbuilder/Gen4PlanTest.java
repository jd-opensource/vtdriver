/*
Copyright 2023 JD Project Authors. Licensed under Apache-2.0.

Copyright 2022 The Vitess Authors.

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

import java.io.IOException;
import org.junit.Test;

public class Gen4PlanTest extends PlanTest {
    @Test
    public void testOne() throws IOException {
        g4AssertTestFile("src/test/resources/plan/one_cases.txt", vm, 0);
        testFile("src/test/resources/plan/one_cases.txt", vm, 0);
    }

    @Test
    public void g4TestPlan() throws IOException {
        g4AssertTestFile("src/test/resources/plan/filter_cases.txt", vm, 0);
        g4AssertTestFile("src/test/resources/plan/aggr_cases.txt", vm, 0);
        g4AssertTestFile("src/test/resources/plan/postprocess_cases.txt", vm, 0);
        g4AssertTestFile("src/test/resources/plan/from_cases.txt", vm, 0);
        g4AssertTestFile("src/test/resources/plan/memory_sort_cases.txt", vm, 0);
    }
}
