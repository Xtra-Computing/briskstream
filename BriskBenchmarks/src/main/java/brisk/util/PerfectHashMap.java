package brisk.util;

import java.util.*;

/**
 * <p><code>PerfectHashmap</code> is optimized for fast <code>get</code> and <code>contains</code> operations.
 * It is required that the set of keys is finite, known in advance and the hashcodes for the keys are collision free.</p>
 * <p>Only use this implementation if your application is latency sensitive.</p>
 *
 * @param <K> Type parameter for keys.
 * @param <V> Type parameter for values.
 * @author Nicholas Poul Schultz-Mller
 */
public class PerfectHashMap<K, V> implements Map<K, V> {

    private final Object[] keys;
    private final Object[] values;

    private PerfectHashMap(Object[] keys, Object[] values) {
        this.keys = keys;
        this.values = values;
    }

    /**
     * Calculates the index given the key and the size (of the map).
     *
     * @param key  The key to find the index for.
     * @param size The size of the map. Must be a power of 2.
     * @return The <code>size</code> least significant bits of the <code>key</code>'s hashcode.
     */
    private static int indexFor(Object key, int size) {
        return key.hashCode() & (size - 1);
    }

    public static <K, V> Builder<K, V> newBuilder() {
        return new Builder<K, V>();
    }

    public boolean containsKey(Object key) {
        return keys[indexFor(key)] != null;
    }

    public boolean containsValue(Object value) {
        for (Object v : values) {
            if (v == null && value == null) {
                return true;
            }
            if (v != null && v.equals(value)) {
                return true;
            }
        }
        return false;
    }

    public void clear() {
        for (int i = 0, n = values.length; i < n; ++i) {
            keys[i] = null;
            values[i] = null;
        }
    }

    @SuppressWarnings("unchecked")
    public V get(Object key) {
        return (V) values[indexFor(key)];
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public Set<Entry<K, V>> entrySet() {
        Set<Entry<K, V>> result = new HashSet<Entry<K, V>>(keys.length);

        for (final Object key : keys) {
            Entry<K, V> entry = new Entry<K, V>() {
                @SuppressWarnings("unchecked")
                public K getKey() {
                    return (K) key;
                }

                @SuppressWarnings("unchecked")
                public V getValue() {
                    return (V) PerfectHashMap.this.values[PerfectHashMap.this.indexFor(key)];
                }

                @SuppressWarnings("unchecked")
                public V setValue(V value) {
                    int index = PerfectHashMap.this.indexFor(key);

                    V oldValue = (V) PerfectHashMap.this.values[index];

                    PerfectHashMap.this.values[index] = value;

                    return oldValue;
                }
            };

            result.add(entry);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public Set<K> keySet() {
        Set<K> keySet = new HashSet<K>();

        for (Object key : keys) {
            if (key != null) {
                keySet.add((K) key);
            }
        }

        return keySet;
    }

    public Collection<V> values() {
        List<V> values = new ArrayList<V>();

        for (Object key : keys) {
            if (key != null) {
                values.add(get(key));
            }
        }

        return values;
    }

    @SuppressWarnings("unchecked")
    public V put(K key, V value) {
        int index = indexFor(key);

        Object oldKey = keys[index];
        Object oldValue = values[index];

        if (oldKey != null && !oldKey.equals(key)) {
            throw new IllegalArgumentException("Cannot put key '" + key + "' because it's a new key.");
        }

        keys[index] = key;
        values[index] = value;

        return (V) oldValue;
    }

    public void putAll(Map<? extends K, ? extends V> map) {
        for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        int index = indexFor(key);

        Object o = values[index];

        keys[index] = null;
        values[index] = null;

        return (o == null) ? null : (V) o;
    }

    public int size() {
        int size = 0;

        for (Object o : keys) {
            if (o != null) {
                ++size;
            }
        }

        return size;
    }

    private int indexFor(Object key) {
        return indexFor(key, keys.length);
    }

    public static class Builder<K, V> {
        private final Map<K, V> map = new HashMap<K, V>();

        private Builder() {
        }

        public Builder<K, V> add(K key, V value) {
            map.put(key, value);

            return this;
        }

        public Builder<K, V> addAll(Map<K, V> map) {
            for (Entry<K, V> entry : map.entrySet()) {
                add(entry.getKey(), entry.getValue());
            }

            return this;
        }

        public <K1 extends K, V1 extends V> Map<K1, V1> build() {
            if (map.size() == 0) {
                return new PerfectHashMap<K1, V1>(new Object[0], new Object[0]);
            }

            assertCollisionFree();

            int size = findSize();

            Object[] keys = new Object[size];
            Object[] values = new Object[size];

            for (Entry<K, V> entry : map.entrySet()) {
                int index = indexFor(entry.getKey(), size);

                keys[index] = entry.getKey();
                values[index] = entry.getValue();
            }

            return new PerfectHashMap<K1, V1>(keys, values);
        }

        public float fillrate() {
            if (map.size() == 0) {
                return 0;
            }

            int size = findSize();

            return ((float) map.size()) / size;
        }

        private int findSize() {
            Set<K> keys = map.keySet();

            // Find a power of 2 capacity greater than map.size().
            int size = 1;

            while (hasCollisions(size, keys)) {
                size <<= 1;
            }

            return size;
        }

        private void assertCollisionFree() {
            final Set<Integer> hashCodes = new HashSet<Integer>();

            for (K key : map.keySet()) {
                int hashCode = key.hashCode();

                if (hashCodes.contains(hashCode)) {
                    throw new IllegalArgumentException("HashCode collision.");
                }

                hashCodes.add(hashCode);
            }
        }

        private boolean hasCollisions(int size, Set<K> keys) {
            final BitSet usedIndexes = new BitSet(size);

            for (K key : keys) {
                int index = indexFor(key, size);

                if (usedIndexes.get(index)) {
                    return true;
                }

                usedIndexes.set(index);
            }

            return false;
        }
    }
}




