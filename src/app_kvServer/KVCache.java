package app_kvServer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A KV cache implementation, based on LinkedHashMap.
 *
 * The cache has a fixed maximum number of elements (cacheSize).
 * If the cache is full and another entry is added, according to the policy
 * an entry has to be dropped.
 *
 * This class is thread-safe. All methods of this class are synchronized.
 */
public class KVCache<K, V> {

    private LinkedHashMap<K,V> map;
    final int cacheSize;
    CachePolicy policy;


    /**
     * Creates a new LRU or FIFO or LFU cache according to the cache replacing policy.
     * @param cacheSize the maximum number of entries that will be kept in this cache.
     */
    public KVCache (final int cacheSize, String Policy) {

        this.cacheSize = cacheSize;
        policy = CachePolicy.valueOf(Policy.toUpperCase());

        switch (policy) {
            case LRU:
                map = new LinkedHashMap<K,V>(cacheSize +1, 1F, true) {
                    // (an anonymous inner class)
                    private static final long serialVersionUID = 1;
                    @Override protected boolean removeEldestEntry (Map.Entry<K,V> eldest) {
                        return size() > KVCache.this.cacheSize; }};
                break;
            case FIFO:
                map = new LinkedHashMap<K,V>(cacheSize +1, 1F, false) {
                    // (an anonymous inner class)
                    private static final long serialVersionUID = 1;
                    @Override protected boolean removeEldestEntry (Map.Entry<K,V> eldest) {
                        return size() > KVCache.this.cacheSize; }};
                break;
            case LFU:
                break;
            default:
                System.out.println("No such Cache replacement policy.");
                break;

        }
    }

    /**
     * Retrieves an entry from the cache.
     * e.g. If we use LRU policy: The retrieved entry becomes the MRU (most recently used) entry.
     * @param key the key whose the associated value is to be returned by the function.
     * @return    the value for this key, or null if no value with this key exists in the cache.
     */
    public synchronized V get (K key) {
        return map.get(key);
    }

    /**
     * Adds an entry to the cache.
     * The new entry becomes the most recently used (MRU) entry.
     * If an entry with the specified key already exists in the cache, it is replaced by the new entry.
     * If the cache is full, the least recently used (LRU) entry is removed from the cache.
     * @param key    the key with which the specified value.
     * @param value  a value, associated with the specified key.
     */
    public synchronized void put (K key, V value) {
        map.put (key, value);
    }


    /**
     * Returns a Collection that contains a copy of all cache entries.
     * @return a Collection with a copy of the cache content.
     */
    public synchronized Collection<Map.Entry<K,V>> getAll() {
        return new ArrayList<Map.Entry<K,V>>(map.entrySet());
    }



    // Test routine for the KVCache class.
    public static void main (String[] args) {

        KVCache<String,String> c = new KVCache<String, String>(3, "LRU");
        c.put ("1", "one");                           // 1
        c.put ("2", "two");                           // 2 1
        c.put ("3", "three");                         // 3 2 1
        c.put ("4", "four");                          // 4 3 2


        if (c.get("2") == null) throw new Error();    // 2 4 3
        c.put ("5", "five");                          // 5 2 4
        c.put ("4", "second four");                   // 4 5 2

        // Verify cache content.
        if (!c.get("4").equals("second four")) throw new Error();
        if (!c.get("5").equals("five"))        throw new Error();
        //if (!c.get("2").equals("two"))
        // List cache content.
        for (Map.Entry<String, String> e : c.getAll())
            System.out.println (e.getKey() + " : " + e.getValue());

    }

}
