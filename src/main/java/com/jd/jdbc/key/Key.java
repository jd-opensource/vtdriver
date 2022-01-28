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

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import com.hierynomus.protocol.commons.buffer.Buffer;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import io.vitess.proto.Topodata.KeyRange;
import io.vitess.proto.Topodata.KeyspaceIdType;
import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

public class Key {

    public static final Map<KeyRange, byte[]> keyRangeStartCache = new ConcurrentHashMap<>(16, 1);

    public static final Map<KeyRange, byte[]> keyRangeEndCache = new ConcurrentHashMap<>(16, 1);

    private final static Log log = LogFactory.getLog(Key.class);

    //
    // KeyspaceIdType helper methods
    //

    // ParseKeyspaceIDType parses the keyspace id type into the enum
    public static KeyspaceIdType parseKeyspaceIDType(String param) {
        if (param.equals("")) {
            return KeyspaceIdType.UNSET;
        }
        KeyspaceIdType value = KeyspaceIdType.UNSET;
        switch (param.toUpperCase()) {
            case "UINT64":
                value = KeyspaceIdType.UINT64;
            case "BYTES":
                value = KeyspaceIdType.BYTES;
            case "UNSET":
                value = KeyspaceIdType.UNSET;
        }

        return value;
    }
    //
    // KeyRange helper methods
    //

    // EvenShardsKeyRange returns a key range definition for a shard at index "i",
    // assuming range based sharding with "n" equal-width shards in total.
    // i starts at 0.
    //
    // Example: (1, 2) returns the second out of two shards in total i.e. "80-".
    //
    // This function must not be used in the Vitess code base because Vitess also
    // supports shards with different widths. In that case, the output of this
    // function would be wrong.
    //
    // Note: start and end values have trailing zero bytes omitted.
    // For example, "80-" has only the first byte (0x80) set.
    // We do this to produce the same KeyRange objects as ParseKeyRangeParts() does.
    // Because it's using the Go hex methods, it's omitting trailing zero bytes as
    // well.

    public static KeyRange evenShardsKeyRange(long i, long n) {
        if (n <= 0) {
            //return nil, fmt.Errorf("the shard count must be > 0: %v", n)
            return null;
        }
        if (i >= n) {
            return null;
            //return nil, fmt.Errorf("the index of the shard must be less than the total number of shards: %v < %v", i, n)
        }
        if ((n & (n - 1)) != 0) {
            return null;
            //return nil, fmt.Errorf("the shard count must be a power of two: %v", n)
        }

        // Determine the number of bytes which are required to represent any
        // KeyRange start or end for the given n.
        // This is required to trim the returned values to the same length e.g.
        // (256, 512) should return 8000-8080 as shard key range.
        int minBytes = 0;
        for (long nn = (n - 1); nn > 0; nn >>= 8) {
            minBytes++;
        }


        // Note: The byte value is empty if start or end is the min or the max
        // respectively.
        byte[] startBytes = {};
        byte[] endBytes = {};

        if (n > 0) {
            BigInteger bi = new BigInteger("18446744073709551615");
            BigInteger val = new BigInteger(String.valueOf(n));
            BigInteger width = bi.divide(val);
            width = width.add(new BigInteger("1"));
            BigInteger start = width.multiply(new BigInteger(String.valueOf(i)));
            BigInteger end = start.add(width);

            if (end.compareTo(new BigInteger("18446744073709551616")) == 0
                || end.compareTo(new BigInteger("18446744073709551616")) == 1) {
                end = new BigInteger("0");
            }
            try {
                byte[] sb = Uint64key.bytes(start);
                //byte[] sb=sbuf.readRawBytes();
                if ((sb.length) >= minBytes) {
                    startBytes = new byte[minBytes];
                    System.arraycopy(sb, 0, startBytes, 0, minBytes);
                } else {
                    startBytes = new byte[sb.length];
                    System.arraycopy(sb, 0, startBytes, 0, sb.length);
                }
            } catch (Buffer.BufferException e) {
                throw new RuntimeException("Key buffer copy fail", e);
            }

            try {
                byte[] eb = Uint64key.bytes(end);
                if ((eb.length) >= minBytes) {
                    endBytes = new byte[minBytes];
                    System.arraycopy(eb, 0, endBytes, 0, minBytes);
                } else {
                    endBytes = new byte[eb.length];
                    System.arraycopy(eb, 0, endBytes, 0, eb.length);
                }
            } catch (Buffer.BufferException e) {
                throw new RuntimeException("Key buffer copy fail", e);
            }
            if (start.compareTo(new BigInteger("0")) == 0) {
                byte[] tp = {};
                startBytes = tp;
            }
            if (end.compareTo(new BigInteger("0")) == 0) {
                // Always set the end except for the last shard. In that case, the
                // end value (2^64) flows over and is the same as 0.
                byte[] tp = {};
                endBytes = tp;
            }
        }


        KeyRange kr = KeyRange.newBuilder()
            .setStart(ByteString.copyFrom(startBytes))
            .setEnd(ByteString.copyFrom(endBytes)).build();

        return kr;
    }

    // KeyRangeContains returns true if the provided id is in the keyrange.
    public static boolean keyRangeContains(KeyRange kr, byte[] id) {
        if (kr == null) {
            return true;
        }

        byte[] startCache = keyRangeStartCache.get(kr);
        if (startCache == null || startCache.length == 0) {
            startCache = kr.getStart().toByteArray();
            keyRangeStartCache.put(kr, startCache);
        }

        byte[] endCache = keyRangeEndCache.get(kr);
        if (endCache == null || endCache.length == 0) {
            endCache = kr.getEnd().toByteArray();
            keyRangeEndCache.put(kr, endCache);
        }

        return Bytes.compare(startCache, id) <= 0 && ((kr.getEnd()).size() == 0 || Bytes.compare(id, endCache) < 0);
    }

    // ParseKeyRangeParts parses a start and end hex values and build a proto KeyRange
    public static KeyRange parseKeyRangeParts(String start, String end) {
        byte[] s = {};
        try {
            s = Hex.decodeHex(start.toCharArray());
        } catch (DecoderException e) {
            log.error("Key.parseKeyRangeParts failed", e);
            return null;
        }


        byte[] e = {};
        try {
            e = Hex.decodeHex(end.toCharArray());
        } catch (DecoderException ex) {
            log.error("Key.parseKeyRangeParts.Hex.decodeHex failed", ex);
            return null;
        }
        KeyRange kr = KeyRange.newBuilder()
            .setStart(ByteString.copyFrom(s))
            .setEnd(ByteString.copyFrom(e)).build();
        return kr;
    }


    // KeyRangeString prints a topodatapb.KeyRange
    public static String keyRangeString(KeyRange k) {
        if (k == null) {
            return "<null>";
        }
        return Hex.encodeHexString(k.getStart().toByteArray()) + "-" + Hex.encodeHexString(k.getEnd().toByteArray());
    }

    // KeyRangeIsPartial returns true if the KeyRange does not cover the entire space.
    public static Boolean KeyRangeIsPartial(KeyRange kr) {
        if (kr == null) {
            return false;
        }
        return !(kr.getStart().size() == 0 && kr.getEnd().size() == 0);
    }

    // KeyRangeEqual returns true if both key ranges cover the same area
    public static Boolean keyRangeEqual(KeyRange left, KeyRange right) {
        if (left == null) {
            return right == null || (right.getStart().size() == 0 && right.getEnd().size() == 0);
        }
        if (right == null) {
            return left.getStart().size() == 0 && left.getEnd().size() == 0;
        }
        return Bytes.equal(left.getStart().toByteArray(), right.getStart().toByteArray()) &&
            Bytes.equal(left.getEnd().toByteArray(), right.getEnd().toByteArray());
    }

    // KeyRangeStartEqual returns true if both key ranges have the same start
    public static Boolean keyRangeStartEqual(KeyRange left, KeyRange right) {
        if (left == null) {
            return right == null || right.getStart().size() == 0;
        }
        if (right == null) {
            return left.getStart().size() == 0;
        }
        return Bytes.equal(left.getStart().toByteArray(), right.getStart().toByteArray());
    }

    // KeyRangeEndEqual returns true if both key ranges have the same end
    public static boolean keyRangeEndEqual(KeyRange left, KeyRange right) {
        if (left == null) {
            return right == null || right.getEnd().size() == 0;
        }
        if (right == null) {
            return (left.getEnd().size()) == 0;
        }
        return Bytes.equal(left.getEnd().toByteArray(), right.getEnd().toByteArray());
    }

    // For more info on the following functions, see:
    // See: http://stackoverflow.com/questions/4879315/what-is-a-tidy-algorithm-to-find-overlapping-intervals
    // two segments defined as (a,b) and (c,d) (with a<b and c<d):
    // intersects = (b > c) && (a < d)
    // overlap = min(b, d) - max(c, a)

    // KeyRangesIntersect returns true if some Keyspace values exist in both ranges.
    public static boolean keyRangesIntersect(KeyRange first, KeyRange second) {
        if (first == null || second == null) {
            return true;
        }
        return (first.getEnd().size() == 0 || Bytes.compare(second.getStart().toByteArray(), first.getEnd().toByteArray()) < 0) &&
            (second.getEnd().size() == 0 || Bytes.compare(first.getStart().toByteArray(), second.getEnd().toByteArray()) < 0);
    }


    // KeyRangesOverlap returns the overlap between two KeyRanges.
    // They need to overlap, otherwise an error is returned.
    public static KeyRange keyRangesOverlap(KeyRange first, KeyRange second) {
        if (!keyRangesIntersect(first, second)) {
            log.info(String.format("KeyRanges %s and %s don't overlap", first, second));
            return null;
        }
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }
        // compute max(c,a) and min(b,d)
        // start with (a,b)
        KeyRange result = first;
        KeyRange.Builder builder = KeyRange.newBuilder();
        // if c > a, then use c
        if (Bytes.compare(second.getStart().toByteArray(), first.getStart().toByteArray()) > 0) {
            try {
                TextFormat.merge(result.toString(), builder);
            } catch (TextFormat.ParseException e) {
                log.error(e.getMessage(), e);
            }
            result = builder.setStart(second.getStart()).build();
        }
        // if b is maxed out, or
        // (d is not maxed out and d < b)
        //                           ^ valid test as neither b nor d are max
        // then use d
        if (first.getEnd().size() == 0 || (second.getEnd().size() != 0 && Bytes.compare(second.getEnd().toByteArray(), first.getEnd().toByteArray()) < 0)) {
            try {
                TextFormat.merge(result.toString(), builder);
            } catch (TextFormat.ParseException e) {
                log.error("result.parse failed, result is " + result, e);
            }
            result = builder.setEnd(second.getEnd()).build();
        }
        return result;
    }

    // KeyRangeIncludes returns true if the first provided KeyRange, big,
    // contains the second KeyRange, small. If they intersect, but small
    // spills out, this returns false.
    public static boolean keyRangeIncludes(KeyRange big, KeyRange small) {
        if (big == null) {
            // The outside one covers everything, we're good.
            return true;
        }
        if (small == null) {
            // The smaller one covers everything, better have the
            // bigger one also cover everything.
            return big.getStart().size() == 0 && big.getEnd().size() == 0;
        }
        // Now we check small.Start >= big.Start, and small.End <= big.End
        if (big.getStart().size() != 0 && Bytes.compare(small.getStart().toByteArray(), big.getStart().toByteArray()) < 0) {
            return false;
        }
        return big.getEnd().size() == 0 || (small.getEnd().size() != 0 && Bytes.compare(small.getEnd().toByteArray(), big.getEnd().toByteArray()) <= 0);
    }

    // ParseShardingSpec parses a string that describes a sharding
    // specification. a-b-c-d will be parsed as a-b, b-c, c-d. The empty
    // string may serve both as the start and end of the keyspace: -a-b-
    // will be parsed as start-a, a-b, b-end.
    public static KeyRange[] parseShardingSpec(String spec) {
        String[] parts = Strings.split(spec, "-");
        if (parts.length == 1) {
            log.info("malformed spec: doesn't define a range: " + spec);
            return null;
        }
        String old = parts[0];
        KeyRange[] ranges = new KeyRange[parts.length - 1];

        int i = 0;
        for (int j = 1; j < parts.length; j++) {
            String p = parts[j];
            if (p.equals("") && i != (parts.length - 2)) {
                log.info("malformed spec: MinKey/MaxKey cannot be in the middle of the spec: " + spec);
                return null;
            }
            if (!p.equals("") && p.compareTo(old) == -1) {
                //throw new RuntimeException(System.out.format("malformed spec: shard limits should be in order: %s", spec).toString());
                log.info("malformed spec: shard limits should be in order: " + spec);
                return null;
            }

            byte[] s = {};
            try {
                s = Hex.decodeHex(old.toCharArray());
            } catch (DecoderException ex) {
                log.error(ex.getMessage(), ex);
                return null;
            }
            if (s == null) {
                return null;
            }
            if (s.length == 0) {
                s = null;
            }

            byte[] e = {};
            try {
                e = Hex.decodeHex(p.toCharArray());
            } catch (DecoderException ex) {
                log.error("Hex.decodeHex(p) fail, p is " + p, ex);
                return null;
            }
            if (e.length == 0) {
                e = null;
            }
            KeyRange.Builder bd = KeyRange.newBuilder();
            if (s != null) {
                bd.setStart(ByteString.copyFrom(s));
            }
            if (e != null) {
                bd.setEnd(ByteString.copyFrom(e));
            }
            ranges[i] = bd.build();
            i++;
            old = p;
        }
        return ranges;
    }
}
