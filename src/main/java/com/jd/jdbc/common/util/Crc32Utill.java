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

package com.jd.jdbc.common.util;

import java.util.zip.CRC32;

public class Crc32Utill {

    private Crc32Utill() {
    }

    public static long checksumByCrc32(byte[] b) {
        CRC32 crc32 = new CRC32();
        crc32.update(b);
        return crc32.getValue();
    }

    public static long checksumByCrc32(int b) {
        CRC32 crc32 = new CRC32();
        crc32.update(b);
        return crc32.getValue();
    }

}
