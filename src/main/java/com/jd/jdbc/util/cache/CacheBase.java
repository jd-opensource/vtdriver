package com.jd.jdbc.util.cache;

public interface CacheBase<T> {

    /**
     * @param key
     * @return
     */
    T get(String key);

    /**
     * @param key
     * @param value
     */
    void set(String key, T value);

    /**
     * @return
     */
    Integer size();

    /**
     * @return
     */
    Integer capacity();

    /**
     *
     */
    void clear();
}
