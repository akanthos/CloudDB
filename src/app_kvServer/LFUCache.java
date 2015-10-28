package app_kvServer;

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

    public LFUCache(int initialCapacity) {
        this.initialCapacity = initialCapacity;
    }

    public void addCacheEntry(String key, String data) {
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

    public String getCacheEntry(String key) {
        if (cacheMap.containsKey(key))  // cache hit
        {
            CacheEntry temp = cacheMap.get(key);
            temp.frequency++;
            cacheMap.put(key, temp);
            return temp.data;
        }
        return null; // cache miss
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
