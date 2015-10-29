package app_kvServer;

import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import helpers.StorageException;
import org.apache.log4j.Logger;

import java.util.*;

import static app_kvServer.CachePolicy.*;

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
    private KVPersistenceEngine persistence;
    final Integer cacheSize;
    CachePolicy policy;
    private static Logger logger = Logger.getLogger(KVCache.class);



    /* IMPORTANT NOTE:
     *  From http://docs.oracle.com/javase/7/docs/api/java/util/LinkedHashMap.html
     *  Note that this implementation is not synchronized.
     *  If multiple threads access a linked hash map concurrently, and at least one of the threads modifies the
     *  map structurally, it must be synchronized externally. This is typically accomplished by synchronizing on
     *  some object that naturally encapsulates the map. If no such object exists, the map should be "wrapped" using
     *  the Collections.synchronizedMap method. This is best done at creation time, to prevent accidental
     *  unsynchronized access to the map:
     *          Map m = Collections.synchronizedMap(new LinkedHashMap(...));
     *  TODO: Should we consider that method??
     * /





    /**
     * Creates a new LRU or FIFO or LFU cache according to the cache replacing policy.
     * @param cacheSize the maximum number of entries that will be kept in this cache.
     */
    public KVCache (final int cacheSize, String Policy) throws StorageException {

        this.cacheSize = cacheSize;
        switch (policy = valueOf(Policy)) {
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
                lfu = new LFUCache(cacheSize, persistence);
                break;
            default:
                System.out.println("No such Cache replacement policy.");
                break;

        }
        this.persistence = new KVPersistenceEngine();

    }

    /**
     * Retrieves an entry from the cache.
     * e.g. If we use LRU policy: The retrieved entry becomes the MRU (most recently used) entry.
     * @param key the key whose the associated value is to be returned by the function.
     * @return    the value for this key, or null if no value with this key exists in the cache.
     */
    public synchronized KVMessageImpl get (String key) {

        if (policy == LFU) {
            // LFU does the job
            return lfu.getCacheEntry(key); // Just forward the request to the other cache
        }
        else {
            // "This" does the job
            if (map.containsKey(key)) {
                // Cache has the key
                return new KVMessageImpl(key, map.get(key), KVMessage.StatusType.GET_SUCCESS);
            }
            else {
                // Cache miss.... Forward request to KVPersistenceEngine.
                KVMessageImpl result = persistence.get(key);
                if (result.getStatus().equals(KVMessage.StatusType.GET_SUCCESS)) {
                    // Key found in persistence file. Put it in cache too.
                    if (!isFull()) {
                        map.put(key, result.getValue());
                    }
                    else {
                        // Find victim, write it to persistence and
                        String victimKey = findVictimKey();

                        // Write victim to persistence
                        // Then delete it from cache and add new (k,v)
                        if (!victimKey.isEmpty()) {
                            String victimValue = map.get(victimKey);
                            map.remove(victimKey);
                            map.put(key, result.getValue());
                            persistence.put(victimKey, victimValue);
                        }
                        else {
                            logger.error("Couldn't find cache victim");
                            return new KVMessageImpl("", "", KVMessage.StatusType.GET_ERROR);
                        }
                    }
                }
                else {
                    result = new KVMessageImpl(key, "", KVMessage.StatusType.GET_ERROR );
                }

                return result;

            }

        }


    }

    /**
     * Adds an entry to the cache.
     * The new entry becomes the most recently used (MRU) entry.
     * If an entry with the specified key already exists in the cache, it is replaced by the new entry.
     * If the cache is full, the least recently used (LRU) entry is removed from the cache.
     * @param key    the key with which the specified value.
     * @param value  a value, associated with the specified key.
     */
    public synchronized KVMessageImpl put (String key, String value) {

        // TODO: Delete is missing (if key is "")

        if (policy == LFU) {
            return lfu.addCacheEntry(key, value); // Just forward the request to the other cache
        }
        else {
            // "This" does the job
            if (value.equals("")) {
                map.remove(key);
                return persistence.remove(key);
            }
            else {
                if (map.containsKey(key)) {
                    map.replace(key, value);
                    //return persistence.put(key, value); // Write-through policy
                    return new KVMessageImpl(key, value, KVMessage.StatusType.PUT_SUCCESS);
                } else {
                    // Cache miss.... Forward request to KVPersistenceEngine.
                    KVMessageImpl result = persistence.put(key, value);
                    if (result.getStatus().equals(KVMessage.StatusType.PUT_SUCCESS) || result.getStatus().equals(KVMessage.StatusType.PUT_UPDATE)) {
                        // Key was written in persistence file. Put it in cache too.
                        // Or key found and updated in persistence file. Put it in cache too. :-)

                        // The rest for Write-allocate policy
                        if (!isFull()) {
                            map.put(key, value);
                        } else {
                            // Find victim, write it to persistence and
                            String victimKey = findVictimKey();

                            // Write victim to persistence
                            // Then delete it from cache and add new (k,v)
                            if (!key.isEmpty()) {
                                String victimValue = map.get(victimKey);
                                map.remove(victimKey);
                                map.put(key, result.getValue());
                                persistence.put(victimKey, victimValue);
                            } else {
                                logger.error("Couldn't find cache victim");
                                return new KVMessageImpl("", "", KVMessage.StatusType.PUT_ERROR);
                            }
                        }
                    } else {
                        logger.error("Error while putting value to persistence");
                    }

                    return result;

                }
            }
        }
    }

    private String findVictimKey() {
        // TODO: Implement victim strategies. Is that enough? Do we need switch?
        String victimKey = new String();
        Iterator it = map.entrySet().iterator();
        Map.Entry pair = (Map.Entry)it.next();
        victimKey = pair.getKey().toString();
        /*switch (policy) {
            case FIFO:
                 // Dummy Same algorithm as above
                break;
            case LRU:
                break;
            default:
                logger.error("Should not be reached. Something is wrong with policies.");
        }*/
        return victimKey;
    }

    public void prettyPrintCache() {

        if (policy == LFU)
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

    public boolean isFull() {
        if (map.size() == cacheSize)
            return true;

        return false;
    }


    // Test routine for the KVCache class.
    public static void main (String[] args) {

        KVCache c = null;
        try {
            c = new KVCache(3, "LFU");
        } catch (StorageException e) {
            e.printStackTrace();
        }
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
