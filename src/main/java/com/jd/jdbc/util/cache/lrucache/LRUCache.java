package com.jd.jdbc.util.cache.lrucache;

import com.jd.jdbc.util.cache.CacheBase;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.locks.ReentrantLock;

public class LRUCache<T> implements CacheBase<T> {

    private final Map<String, Node<T>> cacheMap;

    private final Integer capacity;

    private final ReentrantLock lock;

    private Node<T> head;

    public LRUCache(Integer capacity) {
        this.capacity = capacity;
        this.cacheMap = new HashMap<>();
        this.lock = new ReentrantLock();
    }

    @Override
    public T get(String key) {
        lock.lock();
        try {
            if (!cacheMap.containsKey(key)) {
                return null;
            }
            Node<T> node = this.cacheMap.get(key);

            updateCacheNodeLocked(node);
            return node.value;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void set(String key, T value) {
        lock.lock();
        try {
            if (cacheMap.containsKey(key)) {
                Node<T> existNode = cacheMap.get(key);
                updateCacheNodeLocked(existNode, value);
            } else {
                addNew(key, value);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Integer size() {
        lock.lock();
        try {
            return this.cacheMap.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Integer capacity() {
        lock.lock();
        try {
            return this.capacity;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            this.cacheMap.clear();

            while (head != null) {
                Node<T> node = head;
                head = head.next;
                node.next = null;
                node.pre = null;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        lock.lock();
        try {
            StringJoiner joiner = new StringJoiner(", ", LRUCache.class.getSimpleName() + "[", "]");
            if (!cacheMap.isEmpty() && head != null) {
                Node<T> currentNode = head;
                for (int i = 0; i < cacheMap.size(); i++) {
                    if (currentNode.next != null) {
                        joiner.add(currentNode.toString());
                        currentNode = currentNode.next;
                    } else {
                        joiner.add(currentNode.toString());
                    }
                }
            }
            return joiner.toString();
        } finally {
            lock.unlock();
        }
    }

    /**
     * @param node
     */
    private void updateCacheNodeLocked(Node<T> node) {
        this.removeNode(node);
        addAsHead(node);
    }

    /**
     * @param node
     * @param value
     */
    private void updateCacheNodeLocked(Node<T> node, T value) {
        this.removeNode(node);
        node.value = value;
        addAsHead(node);
    }

    /**
     * @param key
     * @param value
     */
    private void addNew(String key, T value) {
        Node<T> newNode = new Node<>(key, value);
        this.addAsHead(newNode);
        while (cacheMap.size() > this.capacity) {
            removeNode(head.pre);
        }
    }

    /**
     * @param node
     */
    private void addAsHead(Node<T> node) {
        if (head == null) {
            node.next = node;
            node.pre = node;
        } else {
            node.next = head;
            node.pre = head.pre;
            head.pre.next = node;
            head.pre = node;
        }

        cacheMap.put(node.key, node);
        head = node;
    }

    /**
     * @param node
     */
    private void removeNode(Node<T> node) {
        if (node.pre != null) {
            node.pre.next = node.next;
        }

        if (node.next != null) {
            node.next.pre = node.pre;
        }

        if (node == head) {
            if (cacheMap.size() > 1) {
                head = head.next;
            } else {
                head = null;
            }
        }
        node.next = null;
        node.pre = null;

        cacheMap.remove(node.key);
    }

    /**
     * @param <T>
     */
    private static class Node<T> {
        private T value;

        private String key;

        private Node<T> pre;

        private Node<T> next;

        public Node() {
        }

        public Node(String key, T value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", "[", "]")
                .add("key='" + key + "'")
                .add("value=" + value)
                .toString();
        }
    }
}
