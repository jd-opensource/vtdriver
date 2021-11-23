package com.jd.jdbc.vindexes.hash;

import com.jd.jdbc.sqltypes.VtValue;
import java.math.BigInteger;

public interface VtValueIndex {
    BigInteger getIndex(VtValue id);
}
