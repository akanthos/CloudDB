package app_kvServer;

import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class LFUCache {

    class CacheEntry {
        private String value;
        private Integer frequency;

        // default constructor
        private CacheEntry(String value, Integer frequency) {
            this.value = value;
            this.frequency = frequency;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public Integer getFrequency() {
            return frequency;
        }

        public void setFrequency(Integer frequency) {
            this.frequency = frequency;
        }

    }

    private static int cacheSize = 10;
    private static LinkedHashMap<String, CacheEntry> map = new LinkedHashMap<String, CacheEntry>();
    private KVPersistenceEngine persistence;
    private static Logger logger = Logger.getLogger(KVCache.class);

    public LFUCache(int cacheSize, KVPersistenceEngine persistence) {
        this.cacheSize = cacheSize;
        this.persistence = persistence;
    }

    public KVMessageImpl getCacheEntry(String key) {
        // "This" does the job
        if (map.containsKey(key)) {
            // Cache has the key
            CacheEntry oldCacheEntry = map.get(key);
            map.put(key, new CacheEntry(oldCacheEntry.getValue(), oldCacheEntry.getFrequency()+1));
            return new KVMessageImpl(key, oldCacheEntry.getValue(), KVMessage.StatusType.GET_SUCCESS);
        }
        else {
            // Cache miss.... Forward request to KVPersistenceEngine.
            KVMessageImpl result = persistence.get(key);
            if (result.getStatus().equals(KVMessage.StatusType.GET_SUCCESS)) {
                // Key found in persistence file. Put it in cache too.
                if (!isFull()) {
                    map.put(key, new CacheEntry(result.getValue(), 1)); // TODO: Is "1" right?
                }
                else {
                    // Find victim, write it to persistence and
                    String victimKey = findVictimKey();

                    // Write victim to persistence
                    // Then delete it from cache and add new (k,v)
                    if (!victimKey.isEmpty()) {
                        String victimValue = map.get(victimKey).getValue();
                        map.remove(victimKey);
                        map.put(key, new CacheEntry(result.getValue(), 1)); // TODO: Is "1" right?
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
            CacheEntry temp = map.get(key);
            temp.frequency++;
            map.put(key, temp);
            return temp.value;
        }
        return null; // cache miss*/
    }

    public KVMessageImpl addCacheEntry(String key, String value) {

        // TODO: Delete is missing (if key is "")

        // "This" does the job
        if (value.equals("null")) {
            map.remove(key);
            return persistence.remove(key);
        }
        else {
            if (map.containsKey(key)) {
                // Cache has the key
                CacheEntry oldEntry = map.get(key);
                map.put(key, new CacheEntry(value, oldEntry.getFrequency()+1));
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
                        map.put(key, new CacheEntry(value, 1)); // TODO: Is "1" right?
                    } else {
                        // Find victim, write it to persistence and
                        String victimKey = findVictimKey();

                        // Write victim to persistence
                        // Then delete it from cache and add new (k,v)
                        if (!key.isEmpty()) {
                            CacheEntry victimEntry = map.get(victimKey);
                            map.remove(victimKey);
                            map.put(key, new CacheEntry(result.getValue(), 1)); // TODO: Is "1" right?
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

    public String findVictimKey() {
        String key = new String();
        Integer minFreq = Integer.MAX_VALUE;

        for (Map.Entry<String, CacheEntry> entry : map.entrySet()) {
            if (minFreq > entry.getValue().frequency) {
                key = entry.getKey();
                minFreq = entry.getValue().frequency;
            }
        }

        return key;
    }

    public static boolean isFull() {
        if (map.size() == cacheSize)
            return true;

        return false;
    }



    public void printLFUCache() {

        Set set = map.entrySet();
        Iterator i = set.iterator();

        // Display elements
        while (i.hasNext()) {
            Map.Entry me = (Map.Entry) i.next();
            System.out.print(me.getKey() + ": ");
            CacheEntry x = (CacheEntry) me.getValue();
            System.out.println(x.getValue());
        }
    }

}
