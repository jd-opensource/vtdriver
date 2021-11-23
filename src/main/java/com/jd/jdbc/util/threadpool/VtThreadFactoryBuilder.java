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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ThreadFactory;

public final class VtThreadFactoryBuilder {

    private static final String NAME_FORMAT_PREFIX = "VtDriver-";

    private static final String DEFAULT_EXECUTOR_NAME_FORMAT = NAME_FORMAT_PREFIX + "%d";

    private VtThreadFactoryBuilder() {
    }

    /**
     * Build default VtDriver thread factory
     *
     * @return default VtDriver thread factory
     */
    public static ThreadFactory build() {
        return build(DEFAULT_EXECUTOR_NAME_FORMAT);
    }

    /**
     * Build VtDriver thread factory
     *
     * @param nameFormat thread name format
     * @return VtDriver thread factory
     */
    public static ThreadFactory build(final String nameFormat) {
        return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(NAME_FORMAT_PREFIX + nameFormat + "%d").build();
    }
}
