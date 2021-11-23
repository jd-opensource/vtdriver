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

package com.jd.jdbc.vindexes.hash;

import com.jd.jdbc.sqltypes.VtValue;
import io.vitess.proto.Query;
import java.math.BigDecimal;
import java.math.BigInteger;

public class Arithmetic {

    public static BigInteger toUint64(VtValue v) throws Exception {
        Numeric num = newIntegralNumeric(v);
        switch (num.typ.getNumber()) {
            case 265:
                if (num.ival < 0) {
                    throw new Exception("value lt 0");
                }
                return BigInteger.valueOf(num.ival);
            case 778:
                return num.uval;
        }
        throw new Exception("convert fail");
    }

    // newIntegralNumeric parses a value and produces an Int64 or Uint64.
    private static Numeric newIntegralNumeric(VtValue v) throws Exception {
        String str = v.toString();
        if (v.isSigned()) {
            long ival = Long.parseLong(str, 10);
            return new Numeric(Query.Type.INT64, ival, null, null);
        } else if (v.isUnsigned()) {
            long uval = Long.parseUnsignedLong(str, 10);
            return new Numeric(Query.Type.UINT64, null, BigInteger.valueOf(uval), null);
        }

        long ival;
        try {
            ival = Long.parseLong(str, 10);
            return new Numeric(Query.Type.INT64, ival, null, null);
        } catch (Exception e) {
            BigInteger uval = new BigInteger(str);
            return new Numeric(Query.Type.UINT64, null, uval, null);
        }
    }

    private static class Numeric {
        private final Query.Type typ;

        private final Long ival;

        private final BigInteger uval;

        private final BigDecimal fval;

        public Numeric(Query.Type typ, Long ival, BigInteger uval, BigDecimal fval) {
            this.typ = typ;
            this.ival = ival;
            this.uval = uval;
            this.fval = fval;
        }
    }

}
