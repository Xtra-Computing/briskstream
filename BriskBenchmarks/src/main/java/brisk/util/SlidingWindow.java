package brisk.util;

import java.io.Serializable;

/**
 * Created by shuhaozhang on 21/11/16.
 */
public class SlidingWindow<V> implements Serializable {
    private static final long serialVersionUID = 7L;
    private final V[] storage;
    private int ct = 0;

    public SlidingWindow(int size) {
        storage = (V[]) new Object[size];
    }

    public boolean build() {
        return get(storage.length - 1) != null;
    }

    public V get(int i) {
        return storage[i];
    }

    public void put(V item) {
        storage[ct % storage.length] = item;
        ct++;
    }

}
