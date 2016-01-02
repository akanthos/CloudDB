package app_kvServer;

import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import org.apache.log4j.Logger;

import java.util.*;

import static app_kvServer.CachePolicy.LFU;

/**
 * Class representing the LFU Cache
 * The KV pairs are represented by the LfuCacheEntry class
 */
public class LFUCache {


    private static int cacheSize = 10;
    private static LinkedHashMap<String, LfuCacheEntry> map = new LinkedHashMap<String, LfuCacheEntry>();
    private KVPersistenceEngine persistence;
    private static Logger logger = Logger.getLogger(KVCache.class);

    /**
     *
     * @param cacheSize size of the LFU Cache
     * @param persistence KVPersistenceEngine instance for accessing the store File
     */
    public LFUCache(int cacheSize, KVPersistenceEngine persistence) {
        this.cacheSize = cacheSize;
        this.persistence = persistence;
    }

    /**
     *
     * @param key key of the Cache Entry to be accessed
     * @return KVMessage representation of the returned KV pair (Cache Entry)
     */
    public KVMessageImpl getLfuCacheEntry(String key) {
        // "This" does the job
        if (map.containsKey(key)) {
            // Cache has the key
            LfuCacheEntry oldLfuCacheEntry = map.get(key);
            map.put(key, new LfuCacheEntry(oldLfuCacheEntry.getValue(), oldLfuCacheEntry.getFrequency()+1));
            return new KVMessageImpl(key, oldLfuCacheEntry.getValue(), KVMessage.StatusType.GET_SUCCESS);
        }
        else {
            // Cache miss.... Forward request to KVPersistenceEngine.
            KVMessageImpl result = persistence.get(key);
            if (result.getStatus().equals(KVMessage.StatusType.GET_SUCCESS)) {
                // Key found in persistence file. Put it in cache too.
                if (!isFull()) {
                    map.put(key, new LfuCacheEntry(result.getValue(), 0));
                }
                else {
                    // Find victim, write it to persistence and
                    String victimKey = findVictimKey();

                    // Write victim to persistence
                    // Then delete it from cache and add new (k,v)
                    if (!victimKey.isEmpty()) {
                        String victimValue = map.get(victimKey).getValue();
                        map.remove(victimKey);
                        map.put(key, new LfuCacheEntry(result.getValue(), 0));
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
        /*if (map.containsKey(key))  // cache hit
        {
            LfuCacheEntry temp = map.get(key);
            temp.frequency++;
            map.put(key, temp);
            return temp.value;
        }
        return null; // cache miss*/
    }

    /**
     * Add a new KV pair in Cache
     * @param key key of the Cache Entry to be inserted
     * @param value value of the Cache Entry to be inserted
     * @return
     */
    public KVMessageImpl addLfuCacheEntry(String key, String value) {

        // "This" does the job
        if (value.equals("null")) {
            map.remove(key);
            return persistence.remove(key);
        }
        else {
            if (map.containsKey(key)) {
                // Cache has the key
                LfuCacheEntry oldEntry = map.get(key);
                map.put(key, new LfuCacheEntry(value, oldEntry.getFrequency()+1));
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
                        map.put(key, new LfuCacheEntry(value, 0));
                    } else {
                        // Find victim, write it to persistence and
                        String victimKey = findVictimKey();

                        // Write victim to persistence
                        // Then delete it from cache and add new (k,v)
                        if (!key.isEmpty()) {
                            LfuCacheEntry victimEntry = map.get(victimKey);
                            map.remove(victimKey);
                            map.put(key, new LfuCacheEntry(result.getValue(), 0));
                            persistence.put(victimKey, victimEntry.getValue());
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

    /**
     *
     * @return the value of the key of the Victim KV pair
     */
    public String findVictimKey() {
        String key = new String();
        Integer minFreq = Integer.MAX_VALUE;

        for (Map.Entry<String, LfuCacheEntry> entry : map.entrySet()) {
            if (minFreq > entry.getValue().getFrequency()) {
                key = entry.getKey();
                minFreq = entry.getValue().getFrequency();
            }
        }

        return key;
    }

    /**
     *
     * @return true if Cache is full else false
     */
    public static boolean isFull() {
        if (map.size() == cacheSize)
            return true;

        return false;
    }

    public synchronized Collection<Map.Entry<String, LfuCacheEntry>> getAll() {

        return new ArrayList<Map.Entry<String, LfuCacheEntry>>(map.entrySet());

    }


    /**
     * Simple print of the LFU Cache
     */
    public void printLFUCache() {

        Set set = map.entrySet();
        Iterator i = set.iterator();

        // Display elements
        while (i.hasNext()) {
            Map.Entry me = (Map.Entry) i.next();
            System.out.print(me.getKey() + ": ");
            LfuCacheEntry x = (LfuCacheEntry) me.getValue();
            System.out.println(x.getValue());
        }
    }

    /**
     *
     * @return the LinkedHashMap representing the Cache
     */
    public LinkedHashMap<String, LfuCacheEntry> getCacheMap(){
        return map;
    }

    /**
     * Clears the lru cache
     */
    public void cleanUp() {
        map.clear();
    }

}
