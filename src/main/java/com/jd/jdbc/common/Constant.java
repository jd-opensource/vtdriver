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

package com.jd.jdbc.common;

public class Constant {
    public static final String DRIVER_NAME = "vtdriver";

    public static final int DRIVER_MAJOR_VERSION = 1;

    public static final int DRIVER_MINOR_VERSION = 0;

    public static final String DEFAULT_DATABASE_PREFIX = "vt_";

    public static final String DRIVER_PROPERTY_ROLE_KEY = "role";

    public static final String DRIVER_PROPERTY_ROLE_RW = "rw";

    public static final String DRIVER_PROPERTY_ROLE_RR = "rr";

    public static final String DRIVER_PROPERTY_SCHEMA = "schema";

    public static final String DRIVER_PROPERTY_QUERY_CONSOLIDATOR = "queryConsolidator";

    public static final String MYSQL_PROTOCOL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";

    public static final String DEFAULT_SPLIT_TABLE_CONFIG_PATH = "vtdriver-split-table.yml";

}
