package com.jd.jdbc.vindexes.hash;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.jd.jdbc.sqltypes.VtValue;
import java.math.BigInteger;

public class MurmurVtValueIndex implements VtValueIndex {
    @Override
    public BigInteger getIndex(VtValue vtValue) {
        BigInteger bi;
        if (vtValue.isSigned()) {
            bi = new BigInteger(vtValue.toString());
        } else {
            try {
                bi = Arithmetic.toUint64(vtValue);
            } catch (Exception e) {
                HashFunction hashFunction = Hashing.murmur3_128();
                HashCode hashCode = hashFunction.hashBytes(vtValue.raw() == null ? "".getBytes() : vtValue.toString().trim().toLowerCase().getBytes());
                bi = BigInteger.valueOf(hashCode.asLong());
            }
        }
        return bi;
    }
}