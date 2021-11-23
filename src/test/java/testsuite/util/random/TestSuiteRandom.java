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

package testsuite.util.random;

import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqltypes.VtNumberRange;
import com.vdurmont.emoji.Emoji;
import com.vdurmont.emoji.EmojiLoader;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.jeasy.random.EasyRandom;

public class TestSuiteRandom extends RandomStringUtils {
    private static final Log LOGGER = LogFactory.getLog(TestSuiteRandom.class);

    private static final Random RANDOM = new Random();

    private static final EasyRandom easyRandom = new EasyRandom();

    private static List<Emoji> emojiList = new ArrayList<>();

    private static int emojiListSize;

    static {
        try {
            InputStream is = TestSuiteRandom.class.getClassLoader().getResourceAsStream("emojis.json");
            if (is != null) {
                emojiList = EmojiLoader.loadEmojis(is);
                emojiListSize = emojiList.size();
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public static Stream<String> streamRandomAlphabetic(final int length, final int streamSize) {
        return Stream.generate(() -> RandomStringUtils.randomAlphabetic(length)).limit(streamSize);
    }

    public static Stream<String> streamRandomAlphanumeric(final int length, final int streamSize) {
        return Stream.generate(() -> RandomStringUtils.randomAlphanumeric(length)).limit(streamSize);
    }

    public static Stream<String> streamRandomAscii(final int length, final int streamSize) {
        return Stream.generate(() -> RandomStringUtils.randomAscii(length)).limit(streamSize);
    }

    public static Stream<Emoji> streamRandomEmoji(final int streamSize) {
        return Stream.generate(() -> emojiList.get(getRandomInt())).limit(streamSize);
    }

    public static Stream<Timestamp> streamTimestamp(final int streamSize) {
        return easyRandom.objects(Timestamp.class, streamSize);
    }

    public static Stream<Date> streamDate(final int streamSize) {
        return easyRandom.objects(Date.class, streamSize);
    }

    public static Stream<Time> streamTime(final int streamSize) {
        return easyRandom.objects(Time.class, streamSize);
    }

    public static IntStream streamSignedInt(final int streamSize) {
        return RANDOM.ints(streamSize, VtNumberRange.INT32_MIN, VtNumberRange.INT32_MAX);
    }

    public static LongStream streamUnsignedInt(final int streamSize) {
        return RANDOM.longs(streamSize, 0L, VtNumberRange.UINT32_MAX);
    }

    public static LongStream streamSignedBigInt(final int streamSize) {
        return RANDOM.longs(streamSize, VtNumberRange.INT64_MIN, VtNumberRange.INT64_MAX);
    }

    public static Stream<BigInteger> streamUnsignedBigInt(final int streamSize) {
        return Stream.generate(() -> new BigInteger(VtNumberRange.UINT64_MAX.bitLength(), new Random())).limit(streamSize);
    }

    public static Stream<String> streamChineseCharacter(final int length, final int streamSize) {
        return Stream.generate(() -> RandomStringUtils.random(length, 0x4E00, 0x9FA5, false, false)).limit(streamSize);
    }

    private static int getRandomInt() {
        return (int) (1 + RANDOM.nextDouble() * (emojiListSize - 1));
    }
}
