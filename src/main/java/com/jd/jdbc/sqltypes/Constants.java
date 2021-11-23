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

package com.jd.jdbc.sqltypes;

import java.math.BigDecimal;
import java.math.BigInteger;

public class Constants {

    public static final BigInteger BIG_INTEGER_MIN_INTEGER_VALUE = BigInteger.valueOf(-2147483648L);

    public static final BigInteger BIG_INTEGER_MAX_INTEGER_VALUE = BigInteger.valueOf(2147483647L);

    public static final BigInteger BIG_INTEGER_MIN_LONG_VALUE = BigInteger.valueOf(-9223372036854775808L);

    public static final BigInteger BIG_INTEGER_MAX_LONG_VALUE = BigInteger.valueOf(9223372036854775807L);

    public static final BigDecimal BIG_DECIMAL_MIN_INTEGER_VALUE = BigDecimal.valueOf(-2147483648L);

    public static final BigDecimal BIG_DECIMAL_MAX_INTEGER_VALUE = BigDecimal.valueOf(2147483647L);

    public static final BigDecimal BIG_DECIMAL_MIN_LONG_VALUE = BigDecimal.valueOf(-9223372036854775808L);

    public static final BigDecimal BIG_DECIMAL_MAX_LONG_VALUE = BigDecimal.valueOf(9223372036854775807L);

    private Constants() {
    }
}
