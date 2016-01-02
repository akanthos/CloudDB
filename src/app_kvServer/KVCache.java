package app_kvServer;

import common.ServerInfo;
import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import common.messages.KVPair;
import common.utils.KVRange;
import hashing.MD5Hash;
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

    private ServerInfo server;
    private LinkedHashMap<String,String> map;
    private LFUCache lfu;
    private KVPersistenceEngine persistence = new KVPersistenceEngine();
    final Integer cacheSize;
    CachePolicy policy;
    private static Logger logger = Logger.getLogger(KVCache.class);


    /**
     * Creates a new LRU or FIFO or LFU cache according to the cache replacing policy.
     * @param cacheSize the maximum number of entries that will be kept in this cache.
     */
    public KVCache (final int cacheSize, String Policy) throws StorageException {

        this.server = new ServerInfo("127.0.0.1", 50000);
        this.cacheSize = cacheSize;
        this.persistence = new KVPersistenceEngine(server);
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

    }


    /**
     * Creates a new LRU or FIFO or LFU cache according to the cache replacing policy.
     * @param cacheSize the maximum number of entries that will be kept in this cache.
     */
    public KVCache (final int cacheSize, String Policy, ServerInfo server) throws StorageException {

        this.server = server;
        this.cacheSize = cacheSize;
        this.persistence = new KVPersistenceEngine(server);
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

    }

    /**
     * Retrieves an entry from the cache.
     * e.g. If we use LRU policy: The retrieved entry becomes the MRU (most recently used) entry.
     * @param key the key whose the associated KVMessage is to be returned by the function.
     * @return    KVMessage representation of KV found, retrieved from Cache or Disk (File)
     */
    public synchronized KVMessageImpl get (String key) {

        if (policy == LFU) {
            // LFU does the job
            return lfu.getLfuCacheEntry(key); // Just forward the request to the other cache
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
     * Adds an entry to the Cache.
     * We are using Write-Back and Write-Allocate policies for
     * the case of Cache Hit and Miss respectively
     * @param key    Key of the KV pair to be stored
     * @param value Value of the KV pair to be stored
     */
    public synchronized KVMessageImpl put(String key, String value) {

        if (policy == LFU) {
            return lfu.addLfuCacheEntry(key, value); // Just forward the request to the LFU cache
        }
        else {
            // "This" does the job
            if (value.equals("null")) {
                map.remove(key);
                return persistence.remove(key);
            }
            else {
                if (map.containsKey(key)) {
                    map.put(key, value);
                    //return persistence.put(key, value); // Write-through policy
                    return new KVMessageImpl(key, value, KVMessage.StatusType.PUT_UPDATE);
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

    /**
     *
     * @return the Key of the victim to throw from Cache
     */
    private String findVictimKey() {
        String victimKey;
        Iterator it = map.entrySet().iterator();
        Map.Entry pair = (Map.Entry)it.next();
        victimKey = pair.getKey().toString();
        return victimKey;
    }

    /**
     * Simple print function
     */
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

    /**
     *
     * @return True if we reached the Cache's max capacity else False
     */
    public boolean isFull() {
        if (policy == LFU)
            return this.lfu.isFull();
        else
            return map.size() == cacheSize;
    }

    public synchronized Collection<Map.Entry<String, String>> getAll() {

        return new ArrayList<Map.Entry<String, String>>(map.entrySet());

    }

    /**
     * LFU cache getter
     * @return the LFU cache instance
     */
    public LFUCache getLFU(){

        return this.lfu;
    }

    /**
     * Hashmap getter
     * @return the cache hashmap
     */
    public LinkedHashMap<String, String> getCacheMap(){
        return map;
    }

    /**
     * Computes the key-value pairs of the cache that
     * belong to the given range
     * @param range the range that keys should belong to
     * @return the pairs required
     */
    public ArrayList<KVPair> getPairsInRange(KVRange range) {
        ArrayList<KVPair> allCurrentPairs = getAllCurrentPairs();
        ArrayList<KVPair> filteredPairs = new ArrayList<>();
        MD5Hash md5Hash = new MD5Hash();
        for (KVPair pair : allCurrentPairs) {
            String keyHash = md5Hash.hash(pair.getKey());
            if (range.isIndexInRange(keyHash)) {
                filteredPairs.add(pair);
            }
        }
        return filteredPairs;
    }

    private ArrayList<KVPair> getAllCurrentPairs() {
        ArrayList<KVPair> currentPairs = new ArrayList<>();
        if (policy == LFU) {
            LinkedHashMap<String, LfuCacheEntry> currentMap;
            synchronized (this) {
                currentMap = new LinkedHashMap<>(lfu.getCacheMap());
            }
            for (String key : currentMap.keySet()) {
                currentPairs.add(new KVPair(key, currentMap.get(key).getValue()));
            }
        }
        else {
            LinkedHashMap<String, String> currentMap;
            synchronized (this) {
                currentMap = new LinkedHashMap<>(map);
            }
            for (String key : currentMap.keySet()) {
                currentPairs.add(new KVPair(key, currentMap.get(key)));
            }
        }
        return currentPairs;
    }

    /**
     * Clears the cache and the persistence
     */
    public void cleanUp() {
        if (policy == LFU) {
            lfu.cleanUp();
        }
        else {
            map.clear();
        }
        this.persistence.cleanUp();
    }
}
