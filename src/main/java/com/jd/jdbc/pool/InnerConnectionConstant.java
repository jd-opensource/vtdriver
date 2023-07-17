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

package com.jd.jdbc.pool;

public class InnerConnectionConstant {

    private static final String PREFIX = "vt";

    public static final String MINIMUM_IDLE = PREFIX + "MinimumIdle";

    public static final String MAXIMUM_POOL_SIZE = PREFIX + "MaximumPoolSize";

    public static final String CONNECTION_TIMEOUT = PREFIX + "ConnectionTimeout";

    public static final String IDLE_TIMEOUT = PREFIX + "IdleTimeout";

    public static final String MAX_LIFETIME = PREFIX + "MaxLifetime";

    public static final String VALIDATION_TIMEOUT = PREFIX + "ValidationTimeout";

    public static final String CONNECTION_INIT_SQL = PREFIX + "ConnectionInitSql";

    public static final String CONNECTION_TEST_QUERY = PREFIX + "ConnectionTestQuery";
}
