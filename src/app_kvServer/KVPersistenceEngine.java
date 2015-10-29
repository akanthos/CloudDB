package app_kvServer;

import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import helpers.StorageException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by aacha on 10/28/2015.
 */
public class KVPersistenceEngine {
    private Properties prop;
    private final String fileName = "data.persistence";
    private InputStream fileStream;
    private static Logger logger = Logger.getLogger(KVPersistenceEngine.class);


    public KVPersistenceEngine() throws StorageException {
        try {
            // Initialize properties file
            prop = new Properties();
            fileStream = getClass().getClassLoader().getResourceAsStream(fileName);
            if (fileStream != null) {
                prop.load(fileStream);
            }
            else {
                // TODO: Create new file
            }
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
    public KVMessageImpl get (String key) {
        // TODO: Do the get.
        // TODO: If key exists then return GET_SUCCESS.
        // TODO: If key doesn't exist then return GET_ERROR.
        return new KVMessageImpl("", "", KVMessage.StatusType.GET_SUCCESS); // Dummy
    }

    /**
     * Adds an entry to the cache.
     * The new entry becomes the most recently used (MRU) entry.
     * If an entry with the specified key already exists in the cache, it is replaced by the new entry.
     * If the cache is full, the least recently used (LRU) entry is removed from the cache.
     * @param key    the key with which the specified value.
     * @param value  a value, associated with the specified key.
     */
    public KVMessageImpl put (String key, String value) {
        // TODO: Do the put.
        // TODO: If key doesn't exist then return PUT_SUCCESS.
        // TODO: If key exists then return PUT_UPDATE.
        // TODO: If there is any trouble return PUT_ERROR.
        return new KVMessageImpl("", "", KVMessage.StatusType.PUT_SUCCESS); // Dummy
    }


    public KVMessageImpl remove (String key) {
        // TODO: Do the remove.
        // TODO: If key exists then return DELETE_SUCCESS.
        // TODO: If key doesn't exist then return DELETE_ERROR.
        return new KVMessageImpl("", "", KVMessage.StatusType.DELETE_SUCCESS); // Dummy
    }


}
