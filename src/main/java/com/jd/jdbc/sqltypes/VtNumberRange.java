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

package com.jd.jdbc.sqltypes;

import java.math.BigInteger;

public class VtNumberRange {
    public static final int INT8_MIN = Byte.MIN_VALUE;

    public static final int INT8_MAX = Byte.MAX_VALUE;

    public static final int UINT8_MIN = 0;

    public static final int UINT8_MAX = 255;

    public static final int INT16_MIN = Short.MIN_VALUE;

    public static final int INT16_MAX = Short.MAX_VALUE;

    public static final int UINT16_MIN = 0;

    public static final int UINT16_MAX = 65535;

    public static final int INT24_MIN = -8388608;

    public static final int INT24_MAX = 8388607;

    public static final int UINT24_MIN = 0;

    public static final int UINT24_MAX = 16777215;

    public static final int INT32_MIN = Integer.MIN_VALUE;

    public static final int INT32_MAX = Integer.MAX_VALUE;

    public static final int UINT32_MIN = 0;

    public static final long UINT32_MAX = 4294967295L;

    public static final long INT64_MIN = Long.MIN_VALUE;

    public static final long INT64_MAX = Long.MAX_VALUE;

    public static final long UINT64_MIN = 0;

    public static final BigInteger UINT64_MAX = new BigInteger("18446744073709551615");
}
