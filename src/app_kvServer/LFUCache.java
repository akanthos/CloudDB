package app_kvServer;

import common.messages.KVMessage;
import common.messages.KVMessageImpl;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class LFUCache {

    class CacheEntry {
        private String data;
        private int frequency;

        // default constructor
        private CacheEntry() {
        }

        public String getData() {
            return data;
        }

        public void setData(String data) {
            this.data = data;
        }

        public int getFrequency() {
            return frequency;
        }

        public void setFrequency(int frequency) {
            this.frequency = frequency;
        }

    }

    private static int initialCapacity = 10;
    private static LinkedHashMap<String, CacheEntry> cacheMap = new LinkedHashMap<String, CacheEntry>();
    private KVPersistenceEngine persistene;

    public LFUCache(int initialCapacity, KVPersistenceEngine persistence) {
        this.initialCapacity = initialCapacity;
        this.persistene = persistence;
    }

    public KVMessageImpl addCacheEntry(String key, String data) {
        if (!isFull()) {
            CacheEntry temp = new CacheEntry();
            temp.setData(data);
            temp.setFrequency(0);

            cacheMap.put(key, temp);
        } else {
            String entryKeyToBeRemoved = getLFUKey();
            cacheMap.remove(entryKeyToBeRemoved);

            CacheEntry temp = new CacheEntry();
            temp.setData(data);
            temp.setFrequency(0);

            cacheMap.put(key, temp);
        }
    }

    public String getLFUKey() {
        String key = "";
        int minFreq = Integer.MAX_VALUE;

        for (Map.Entry<String, CacheEntry> entry : cacheMap.entrySet()) {
            if (minFreq > entry.getValue().frequency) {
                key = entry.getKey();
                minFreq = entry.getValue().frequency;
            }
        }

        return key;
    }

    public KVMessageImpl getCacheEntry(String key) {
        // "This" does the job
        if (cacheMap.containsKey(key)) {
            // Cache has the key
            return new KVMessageImpl(key, cacheMap.get(key), KVMessage.StatusType.GET_SUCCESS);
        }
        else {
            // Cache miss.... Forward request to KVPersistenceEngine.
            KVMessageImpl result = persistene.get(key);
            if (result.getStatus().equals(KVMessage.StatusType.GET_SUCCESS)) {
                // Key found in persistence file. Put it in cache too.
                if (map.size() < cacheSize) {
                    map.put(key, result.getValue());
                }
                else {
                    String victimKey = "";
                    // Find victim, write it to persistence and
                    // TODO: Write victim to persistence
                    // Then delete it from cache and add new (k,v)
                    if (key != null) {
                        map.remove(victimKey);
                        map.put(key, result.getValue());
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
        /*if (cacheMap.containsKey(key))  // cache hit
        {
            CacheEntry temp = cacheMap.get(key);
            temp.frequency++;
            cacheMap.put(key, temp);
            return temp.data;
        }
        return null; // cache miss*/
    }

    public static boolean isFull() {
        if (cacheMap.size() == initialCapacity)
            return true;

        return false;
    }

    public void printLFUCache() {

        Set set = cacheMap.entrySet();
        Iterator i = set.iterator();

        // Display elements
        while (i.hasNext()) {
            Map.Entry me = (Map.Entry) i.next();
            System.out.print(me.getKey() + ": ");
            CacheEntry x = (CacheEntry) me.getValue();
            System.out.println(x.getData());
        }
    }

}
