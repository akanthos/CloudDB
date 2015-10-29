package app_kvServer;

import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import helpers.StorageException;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;

/**
 * Created by aacha on 10/28/2015.
 */
public class KVPersistenceEngine {

    private Properties prop;
    private final String fileName = "data.store";
    private InputStream input = null;
    private OutputStream output = null;
    private static Logger logger = Logger.getLogger(KVPersistenceEngine.class);


    public KVPersistenceEngine() throws StorageException {
        try {
            // Initialize properties file
            File storeFile = new File(fileName);
            prop = new Properties();
            if(!storeFile.exists()) {
                storeFile.createNewFile();
                System.out.println("Created file!");
            }
            input = new FileInputStream(fileName);
            output = new FileOutputStream(fileName);
            prop.load(input);

        } catch (IOException e) {
            logger.error("Cannot initialize persistence file", e);
            throw new StorageException("Cannot initialize persistence file");
        }
    }

    /**
     * Retrieves an entry from the cache.
     * e.g. If we use LRU policy: The retrieved entry becomes the MRU (most recently used) entry.
     * @param key the key whose the associated value is to be returned by the function.
     * @return    the value for this key, or null if no value with this key exists in the cache.
     */
    public KVMessageImpl get (String key){

        String resultValue = prop.getProperty(key);
        if (resultValue !=null)
            return new KVMessageImpl(key, resultValue, KVMessage.StatusType.GET_SUCCESS);
        else
            return new KVMessageImpl(key, "", KVMessage.StatusType.GET_ERROR);

    }

    /**
     * Adds an entry to the cache.
     * The new entry becomes the most recently used (MRU) entry.
     * If an entry with the specified key already exists in the cache, it is replaced by the new entry.
     * If the cache is full, the least recently used (LRU) entry is removed from the cache.
     * @param key    the key with which the specified value.
     * @param value  a value, associated with the specified key.
     */
    public KVMessageImpl put (String key, String value){

        try {

            String oldValue = prop.getProperty(key);
            prop.setProperty(key, value);
            prop.put(key, value);
            output = new FileOutputStream(fileName);
            prop.store(output, null);
            String newValue = prop.getProperty(key);

            return oldValue==null ? new KVMessageImpl(key, newValue, KVMessage.StatusType.PUT_SUCCESS)
                    : new KVMessageImpl(key, newValue, KVMessage.StatusType.PUT_UPDATE);

        } catch (IOException e) {
            logger.error("Cannot write to persistence file", e);
            return new KVMessageImpl(key, "", KVMessage.StatusType.PUT_ERROR);
        }

    }


    public KVMessageImpl remove (String key){

        try {
            String resultValue = prop.getProperty(key);
            if (prop.remove(key) == null)
                return new KVMessageImpl(key, resultValue, KVMessage.StatusType.DELETE_ERROR);
            output = new FileOutputStream(fileName);
            prop.store(output, null);
            return new KVMessageImpl(key, resultValue, KVMessage.StatusType.DELETE_SUCCESS);
        }
        catch (IOException e){
            logger.error("Cannot remove entry from persistence file", e);
            return new KVMessageImpl(key, "", KVMessage.StatusType.DELETE_ERROR);
        }


    }


}
