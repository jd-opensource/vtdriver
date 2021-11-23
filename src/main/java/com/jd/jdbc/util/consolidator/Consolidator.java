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

package com.jd.jdbc.util.consolidator;

import com.jd.jdbc.Executor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Consolidator {
    private static volatile Consolidator singletonInstance;

    private final Map<String, ConsolidatorResult> consolidatorMap = new ConcurrentHashMap<>();

    private Consolidator() {
    }

    public static Consolidator getInstance() {
        if (singletonInstance == null) {
            synchronized (Executor.class) {
                if (singletonInstance == null) {
                    singletonInstance = new Consolidator();
                }
            }
        }
        return singletonInstance;
    }

    /**
     * if exist ,need return
     *
     * @return
     */
    public ConsolidatorResult putIfAbsent(final String key, final ConsolidatorResult result) {
        return consolidatorMap.putIfAbsent(key, result);
    }

    public ConsolidatorResult get(final String key) {
        return consolidatorMap.get(key);
    }

    public void replace(final String key, final ConsolidatorResult result) {
        consolidatorMap.replace(key, result);
    }

    public void remove(final String key) {
        consolidatorMap.remove(key);
    }
}
