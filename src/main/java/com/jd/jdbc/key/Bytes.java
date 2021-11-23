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

public final class Bytes {

    // Compare returns an integer comparing two byte slices lexicographically.
    // The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
    // A nil argument is equivalent to an empty slice.
    public static int compare(byte[] a, byte[] b) {
        if (null == a || a.length == 0) {
            return -1;
        }
        if (null == b || b.length == 0) {
            return 1;
        }
        int min = Math.min(a.length, b.length);
        for (int i = 0; i < min; i++) {
            int n = a[i] & 0xff;
            int m = b[i] & 0xff;
            if (n > m) {
                return 1;
            } else if (n < m) {
                return -1;
            }
        }
        return 0;
    }

    // Equal reports whether a and b
    // are the same length and contain the same bytes.
    // A nil argument is equivalent to an empty slice.
    public static boolean equal(byte[] a, byte[] b) {
        if (a == null || b == null || a.length != b.length) {
            return false;
        }
        for (int i = 0; i < a.length; i++) {
            int n = a[i] & 0xff;
            int m = b[i] & 0xff;
            if (n != m) {
                return false;
            }
        }
        return true;
    }

    public static byte[] decodeToByteArray(String str) {
        char[] charArray = str.toCharArray();
        byte[] arr = new byte[charArray.length];
        for (int i = 0; i < charArray.length; i++) {
            arr[i] = (byte) Integer.parseInt(String.valueOf(charArray[i]));
        }
        return arr;
    }
}
