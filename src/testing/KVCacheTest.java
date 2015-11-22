package testing;


import app_kvServer.KVCache;
import app_kvServer.LFUCache;
import app_kvServer.LfuCacheEntry;
import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import helpers.StorageException;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.*;

/**
 * Test Class for KVCache
 */
public class KVCacheTest extends TestCase{

    private String policy="LRU";
    private int cacheSize=1;
    KVMessageImpl CacheRet;

    /**
     * Test put operation
     * @throws StorageException
     */
    @Test
    //set size =1, cache size =1
    public void testPut() throws StorageException {

        String key="1";
        String value="1st item";
        KVCache kvCache= new KVCache(cacheSize, policy);

        kvCache.put(key,value);
        assertEquals(value, kvCache.get(key).getValue());
        if (policy.equals("LFU"))
            assertTrue(kvCache.getLFU().isFull());
        else
            assertTrue(kvCache.isFull());
        key="2";
        value="2nd item";
        kvCache.put(key, value);

        assertEquals(value, kvCache.get(key).getValue());

    }

    /**
     * Test Discard operation
     * @throws StorageException
     */
    @Test
    public void testCacheDiscard() throws StorageException {

        String []key={"1","2","3"};
        String []value={"1st item","2nd item","3rd item"};
        KVCache kvCache= new KVCache(cacheSize, policy);

        for(int i =0; i<key.length; i++){
            kvCache.put(key[i],value[i]);
            kvCache.get(key[i]);
        }
        String key2="newKey";
        String value2="newValue";
        kvCache.put(key2, value2);

        if (policy.equals("LFU"))
            assertEquals(null, ((LfuCacheEntry) kvCache.getLFU().getCacheMap().get(key[0])));
        else
            assertEquals(null, kvCache.getCacheMap().get(key[0]));

    }


    /**
     * Test LRU Policy
     * @throws StorageException
     */
    @Test
    public void testLRUPolicy() throws StorageException {

        if (policy.equals("LFU") || policy.equals("FIFO"))
            return;

        String[] key = {"1", "2", "3"};
        String[] value = {"1st item", "2nd item", "3rd item"};
        KVCache kvCache = new KVCache(3, policy);

        for (int i = 0; i < key.length; i++) {
            kvCache.put(key[i], value[i]);
        }

        for (int i = 0; i < key.length; i++) {
            assertEquals(value[i], kvCache.getCacheMap().get(key[i]));
        }
        CacheRet = kvCache.get("1");
        String key2 = "4";
        String value2 = "4th item";
        kvCache.put(key2, value2);

        assertEquals(null, kvCache.getCacheMap().get(key[1]));

    }

    /**
     * Test LFU Policy
     * @throws StorageException
     */
    @Test
    public void testLFUPolicy() throws StorageException {

        if (policy.equals("LRU") || policy.equals("FIFO"))
            return;

        String []key={"1","2","3"};
        String []value={"1st item","2nd item","3rd item"};
        KVCache kvCache= new KVCache(3, policy);

        for(int i =0; i<key.length; i++){
            kvCache.put(key[i],value[i]);
        }

        for(int i =0; i<key.length; i++){
            assertEquals(value[i], ((LfuCacheEntry) kvCache.getLFU().getCacheMap().get(key[i])).getValue());
        }
        //1 2 3
        CacheRet = kvCache.get("1");
        CacheRet = kvCache.get("2");
        String key2="4";
        String value2="4th item";
        kvCache.put(key2, value2);

        assertEquals(null, ((LfuCacheEntry) kvCache.getLFU().getCacheMap().get(key[2])));

    }

    /**
     * Test FIFO policy
     * @throws StorageException
     */
    @Test
    public void testFIFOPolicy() throws StorageException {

        if (policy.equals("LRU") || policy.equals("LFU"))
            return;

        String[] key = {"1", "2", "3"};
        String[] value = {"1st item", "2nd item", "3rd item"};
        KVCache kvCache = new KVCache(3, policy);

        for (int i = 0; i < key.length; i++) {
            kvCache.put(key[i], value[i]);
        }

        for (int i = 0; i < key.length; i++) {
            assertEquals(value[i], kvCache.getCacheMap().get(key[i]));
        }
        CacheRet = kvCache.get("1");
        String key2 = "4";
        String value2 = "4th item";
        kvCache.put(key2, value2);

        assertEquals(null, kvCache.getCacheMap().get(key[0]));

    }

}
