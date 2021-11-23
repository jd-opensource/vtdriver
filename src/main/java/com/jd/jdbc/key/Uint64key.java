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

package com.jd.jdbc.key;

import com.google.common.primitives.UnsignedLong;
import com.hierynomus.protocol.commons.buffer.Buffer;
import com.hierynomus.protocol.commons.buffer.Endian;
import java.math.BigInteger;

/**
 * Uint64Key definitions
 */
public final class Uint64key {

    // Uint64Key is a uint64 that can be converted into a KeyspaceId.
    public static String string(long longValue) {
        final String binaryString = Long.toBinaryString(longValue);
        final UnsignedLong unsignedLong = UnsignedLong.valueOf(binaryString, 2);
        return unsignedLong.toString();
    }

    // Bytes returns the keyspace id (as bytes) associated with a Uint64Key.
    public static byte[] bytes(BigInteger i) throws Buffer.BufferException {
        byte[] aa = new byte[8];
        aa[0] = i.shiftRight(56).byteValue();
        aa[1] = i.shiftRight(48).byteValue();
        aa[2] = i.shiftRight(40).byteValue();
        aa[3] = i.shiftRight(32).byteValue();
        aa[4] = i.shiftRight(24).byteValue();
        aa[5] = i.shiftRight(16).byteValue();
        aa[6] = i.shiftRight(8).byteValue();
        aa[7] = i.byteValue();
        return aa;
    }

    public static byte[] bytes(BigInteger i, Endian endiannes) throws Buffer.BufferException {
        byte[] aa = new byte[8];
        Buffer.PlainBuffer plainBuffer = new Buffer.PlainBuffer(8, endiannes);
        plainBuffer.putRawBytes(new byte[] {((byte) i.shiftRight(56).intValue()), ((byte) i.shiftRight(48).intValue()), ((byte) i.shiftRight(40).intValue()), ((byte) i.shiftRight(32).intValue()),
            ((byte) i.shiftRight(24).intValue()), ((byte) i.shiftRight(16).intValue()), ((byte) i.shiftRight(8).intValue()), ((byte) i.intValue())});
        plainBuffer.readRawBytes(aa);
        return aa;
    }

    public static BigInteger uint64(byte[] b) {
        //_ = b[7] // bounds check hint to compiler; see golang.org/issue/14808
        int b7 = b[7] & 0xff;
        int b6 = b[6] & 0xff;
        int b5 = b[5] & 0xff;
        int b4 = b[4] & 0xff;
        int b3 = b[3] & 0xff;
        int b2 = b[2] & 0xff;
        int b1 = b[1] & 0xff;
        int b0 = b[0] & 0xff;
        BigInteger bi7 = new BigInteger(Integer.toString(b7));
        BigInteger bi6 = new BigInteger(Integer.toString(b6));
        bi6 = bi6.shiftLeft(8);
        BigInteger bi5 = new BigInteger(Integer.toString(b5));
        bi5 = bi5.shiftLeft(16);
        BigInteger bi4 = new BigInteger(Integer.toString(b4));
        bi4 = bi4.shiftLeft(24);
        BigInteger bi3 = new BigInteger(Integer.toString(b3));
        bi3 = bi3.shiftLeft(32);
        BigInteger bi2 = new BigInteger(Integer.toString(b2));
        bi2 = bi2.shiftLeft(40);
        BigInteger bi1 = new BigInteger(Integer.toString(b1));
        bi1 = bi1.shiftLeft(48);
        BigInteger bi0 = new BigInteger(Integer.toString(b0));
        bi0 = bi0.shiftLeft(56);

        return bi7.or(bi6).or(bi5).or(bi4).or(bi3).or(bi2).or(bi1).or(bi0);
    }
}
