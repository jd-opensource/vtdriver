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

package com.jd.jdbc.shards.hash;

import com.jd.jdbc.key.Destination;
import com.jd.jdbc.key.DestinationKeyspaceID;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.vindexes.hash.BinaryHash;
import io.vitess.proto.Query;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VtConnHashTest {

    public byte[][] buildKeyspaceIds(Collection<VtValue> values) throws Exception {
        BinaryHash binaryHash = new BinaryHash();
        VtValue[] ids = new VtValue[values.size()];
        ids = values.toArray(ids);
        Destination[] destinations = binaryHash.map(ids);
        List<byte[]> results = new ArrayList<>();
        for (Destination destination : destinations) {
            results.add(((DestinationKeyspaceID) destination).getValue());
        }
        byte[][] result = new byte[results.size()][];
        return results.toArray(result);
    }

    @Test
    public void hash() {
        System.out.println((1 << 31) - 1);
        try {
            Map<VtValue, String> keyShards = new HashMap<>();
//            VtValue vtValue = VtValue.newVtValue(Query.Type.TEXT, "\t\r\n\\abcd1231abcd".getBytes()).copy();
//            VtValue vtValue1 = VtValue.newVtValue(Query.Type.TEXT, "18446744073709551615".getBytes()).copy();
//            VtValue vtValue2 = VtValue.newVtValue(Query.Type.TEXT, " 0-0asfasdfasf。。。。".getBytes()).copy();
//            VtValue vtValue3 = VtValue.newVtValue(Query.Type.TEXT, "你好世界，の".getBytes()).copy();
//            VtValue vtValue4 = VtValue.newVtValue(Query.Type.INT64, String.valueOf(0).getBytes()).copy();
//            VtValue vtValue5 = VtValue.newVtValue(Query.Type.INT64, String.valueOf(Long.MIN_VALUE).getBytes()).copy();
//            VtValue vtValue6= VtValue.newVtValue(Query.Type.TEXT, "!@#$%^&*()_+QWERTYUIOP{}ASDFGHJKL:'ZXCVBNM<>?".getBytes()).copy();
//            VtValue vtValue7= VtValue.newVtValue(Query.Type.TEXT, "18446744073709551620".getBytes()).copy();
            VtValue vtValue8 = VtValue.newVtValue(Query.Type.TIMESTAMP, "2023-05-15 02:39:26.602".getBytes()).copy();
//            keyShards.put(vtValue, "196 193 192 168 108 236 4 27");
//            keyShards.put(vtValue1, "53 85 80 178 21 14 36 81");
//            keyShards.put(vtValue2, "21 232 21 182 168 102 52 110");
//            keyShards.put(vtValue3, "170 169 41 44 13 235 46 205");
//            keyShards.put(vtValue4, "140 166 77 233 193 177 35 167");
//            keyShards.put(vtValue5, "149 248 165 229 221 49 217 0");
//            keyShards.put(vtValue6, "70 139 27 226 119 148 118 57");
//            keyShards.put(vtValue7, "44 112 54 208 203 110 69 154");
            keyShards.put(vtValue8, "68 104 115 235 9 58 76 63");
            Set<VtValue> values = keyShards.keySet();
            VtValue[] vtValues = new VtValue[values.size()];
            byte[][] keyspaceIds = buildKeyspaceIds(values);
            values.toArray(vtValues);
            int i = 0;
            for (byte[] keyspaceId : keyspaceIds) {
                String ksid = printByteArray(keyspaceId, "ksid");
                System.out.println(ksid);
                assertEquals(ksid, keyShards.get(vtValues[i]));
                i++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String printByteArray(byte[] bytes, String size) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            if (i > 0) {
                sb.append(" ");
            }
            int bi = bytes[i];
            if (bytes[i] < 0) {
                bi = bytes[i] & 0xff;
            }
            sb.append(bi);
        }
        return sb.toString();
    }

}
