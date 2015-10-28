package app_kvServer;

import java.util.*;

/**
 * A KV cache implementation, based on LinkedHashMap.
 *
 * The cache has a fixed maximum number of elements (cacheSize).
 * If the cache is full and another entry is added, according to the policy
 * an entry has to be dropped.
 *
 * This class is thread-safe. All methods of this class are synchronized.
 */
public class KVCache {

    private LinkedHashMap<String,String> map;
    private LFUCache lfu;
    final int cacheSize;
    CachePolicy policy;


    /**
     * Creates a new LRU or FIFO or LFU cache according to the cache replacing policy.
     * @param cacheSize the maximum number of entries that will be kept in this cache.
     */
    public KVCache (final int cacheSize, String Policy) {

        this.cacheSize = cacheSize;
        switch (policy = CachePolicy.valueOf(Policy)) {
            case LRU:
                map = new LinkedHashMap<String, String>(cacheSize +1, 1F, true) {
                    // (an anonymous inner class)
                    private static final long serialVersionUID = 1;
                    @Override protected boolean removeEldestEntry (Map.Entry<String, String> eldest) {
                        return size() > KVCache.this.cacheSize; }};
                break;
            case FIFO:
                map = new LinkedHashMap<String, String>(cacheSize +1, 1F, false) {
                    // (an anonymous inner class)
                    private static final long serialVersionUID = 1;
                    @Override protected boolean removeEldestEntry (Map.Entry<String, String> eldest) {
                        return size() > KVCache.this.cacheSize; }};
                break;
            case LFU:
                lfu = new LFUCache(cacheSize);
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
    public synchronized String get (String key) {

        if (policy == CachePolicy.LFU)
            return lfu.getCacheEntry(key);
        else
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
    public synchronized void put (String key, String value) {

        if (policy == CachePolicy.LFU) {
            lfu.addCacheEntry(key, value);
        }
        else
            map.put (key, value);
    }

    public void prettyPrintCache() {

        if (policy == CachePolicy.LFU)
            lfu.printLFUCache();
        else {
            Set set = map.entrySet();
            Iterator i = set.iterator();
            // Display elements
            while(i.hasNext()) {
                Map.Entry me = (Map.Entry)i.next();
                System.out.print(me.getKey() + ": ");
                System.out.println(me.getValue());
            }
        }

    }


    // Test routine for the KVCache class.
    public static void main (String[] args) {

        KVCache c = new KVCache(3, "LFU");
        c.put ("1", "one");
        c.put ("2", "two");
        c.put ("3", "three");
        c.put ("4", "four");

        if (c.get("2") == null) throw new Error();
        c.put ("5", "five");
        c.put ("4", "second four");

        c.prettyPrintCache();
    }

}
