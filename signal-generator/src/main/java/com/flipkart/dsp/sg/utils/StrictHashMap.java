package com.flipkart.dsp.sg.utils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 */


/**
 * A <i>strict</i> version of {@link LinkedHashMap} which throws an exception if you try to:
 * <ul>
 * <li><code>get</code> a non-existent key</li>
 * <li><code>put</code> into an existent key</li>
 * </ul>
 */
public class StrictHashMap<K, V> extends LinkedHashMap<K, V> {
    public static class StrictHashMapException extends IllegalArgumentException {

        public StrictHashMapException(String s) {
            super(s);
        }
    }

    public StrictHashMap() {
        super();
    }

    /**
     * Similar to {@link LinkedHashMap#get(Object)}, but throws exception if you try to get mapped value for a non-existent key
     */
    @Override
    public V get(Object key) {
        V v = super.get(key);
        if (v == null)
            throw new StrictHashMapException("Key " + key + " is not present");
        else
            return v;
    }


    /**
     * Similar to {@link LinkedHashMap#put(K, V)}, but throws exception if you try to get mapped value for a non-existent key
     */
    @Override
    public V put(K key, V value) {
        if (containsKey(key)) {
            throw new StrictHashMapException("Key " + key + " is already present");
        }
        return super.put(key, value);
    }

    public List<V> getValues() {
        return new ArrayList<>(super.values());
    }
}
