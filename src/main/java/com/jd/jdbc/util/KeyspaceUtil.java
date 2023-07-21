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

package com.jd.jdbc.util;

import static com.jd.jdbc.common.Constant.DEFAULT_DATABASE_PREFIX;
import io.netty.util.internal.StringUtil;

public class KeyspaceUtil {
    public static String getLogicSchema(String tableCat) {
        if (StringUtil.isNullOrEmpty(tableCat)) {
            return tableCat;
        }

        if (tableCat.startsWith(DEFAULT_DATABASE_PREFIX)) {
            tableCat = tableCat.substring(3);
        }
        return tableCat;
    }

    public static String getRealSchema(String tableCat) {
        if (StringUtil.isNullOrEmpty(tableCat)) {
            return tableCat;
        }
        return DEFAULT_DATABASE_PREFIX + tableCat;
    }
}