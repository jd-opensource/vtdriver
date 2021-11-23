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

package com.jd.jdbc.cache;

import com.jd.jdbc.util.cache.lrucache.LRUCache;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LRUCacheTest {

    @Test
    public void testInitialState() {
        LRUCache<CacheValue> cache = new LRUCache<>(5);

        assertEquals(0L, cache.size().longValue());
        assertEquals(5L, cache.capacity().longValue());
    }

    @Test
    public void testSetInsertsValue() {
        LRUCache<CacheValue> cache = new LRUCache<>(100);
        CacheValue cacheValue = new CacheValue("ABC");
        String key = "key";
        cache.set(key, cacheValue);
        CacheValue actualValue = cache.get(key);

        assertEquals(cacheValue, actualValue);
    }

    @Test
    public void testGetValueWithMultipleTypes() {
        LRUCache<CacheValue> cache = new LRUCache<>(100);
        CacheValue cacheValue = new CacheValue("ABC");
        String key = new String("key".getBytes());
        cache.set(key, cacheValue);
        CacheValue actualValue = cache.get(key);

        assertEquals(cacheValue, actualValue);
    }

    @Test
    public void testSetUpdatesSize() {
        LRUCache<CacheValue> cache = new LRUCache<>(100);
        CacheValue cacheValue = new CacheValue("ABC");
        String key = "key";
        IntStream.range(0, 50).forEach(i -> cache.set(key, cacheValue));

        assertEquals(1L, cache.size().longValue());
        assertEquals(100L, cache.capacity().longValue());

        IntStream.range(0, 50).forEach(i -> cache.set(key + i, cacheValue));

        assertEquals(51L, cache.size().longValue());
        assertEquals(100L, cache.capacity().longValue());
    }

    @Test
    public void testSetWithOldKeyUpdatesValue() {
        LRUCache<CacheValue> cache = new LRUCache<>(100);
        CacheValue firstValue = new CacheValue("12345");
        String key = "key1";
        cache.set(key, firstValue);
        CacheValue secondValue = new CacheValue("54321");
        cache.set(key, secondValue);
        CacheValue cacheValue = cache.get(key);

        Assert.assertNotEquals(cacheValue, firstValue);
        assertEquals(cacheValue, secondValue);
    }

    @Test
    public void testGetNonExistent() {
        LRUCache<CacheValue> cache = new LRUCache<>(100);
        CacheValue nobody = cache.get("nobody");

        Assert.assertNull(nobody);
    }

    @Test
    public void testClear() {
        LRUCache<CacheValue> cache = new LRUCache<>(100);
        CacheValue v1 = new CacheValue("1");
        cache.set("k1", v1);
        cache.clear();

        assertEquals(0L, cache.size().longValue());
    }

    @Test
    public void testEvicted() {
        int size = 3;
        LRUCache<CacheValue> cache = new LRUCache<>(size);
        cache.set("k1", new CacheValue("v1"));
        cache.set("k2", new CacheValue("v2"));
        cache.set("k3", new CacheValue("v3"));

        assertEquals("LRUCache[[key='k3', value=v3], [key='k2', value=v2], [key='k1', value=v1]]", cache.toString());

        cache.get("k3");
        cache.get("k2");
        cache.get("k1");

        assertEquals("LRUCache[[key='k1', value=v1], [key='k2', value=v2], [key='k3', value=v3]]", cache.toString());

        cache.set("k4", new CacheValue("v4"));

        CacheValue v3 = cache.get("k3");
        assertNull(v3);
        assertEquals("LRUCache[[key='k4', value=v4], [key='k1', value=v1], [key='k2', value=v2]]", cache.toString());
    }

    @Test
    public void testConcurrency() throws InterruptedException {
        LRUCache<CacheValue> cache = new LRUCache<>(1);
        CacheValue v1 = new CacheValue("v1");
        cache.set("k1", v1);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            if (cache.get("k1") != null) {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Assert.assertNull(cache.get("k1"));
                Assert.assertNotEquals(v1, cache.get("k1"));
            }
        });

        executorService.submit(() -> cache.set("k2", new CacheValue("v2")));

        executorService.shutdown();

        TimeUnit.SECONDS.sleep(2);
    }

    private static class CacheValue {
        private final Object value;

        public CacheValue(Object value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CacheValue)) {
                return false;
            }
            return value.equals(((CacheValue) o).value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }
}
