package app_kvServer;

import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import helpers.StorageException;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;


/**
 * Class handling the communication with
 * the Store File (Persistant Storage) read/write
 */
public class KVPersistenceEngine {

    private Properties prop;
    private final String fileName = "data.store";
    private InputStream input = null;
    private OutputStream output = null;
    private static Logger logger = Logger.getLogger(KVPersistenceEngine.class);


    /**
     *
     * @throws StorageException
     * indicating problems accessing the persistant file
     */
    public KVPersistenceEngine() throws StorageException {
        try {
            // Initialize properties file
            File storeFile = new File(fileName);
            prop = new Properties();
            if(!storeFile.exists()) {
                storeFile.createNewFile();
                //System.out.println("Created file!");
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
     * Retrieves an entry from file.
     * @param key the key of the KV pair to be retrieved from the file.
     * @return    KVMessage representation of the retrieved KV pair with the respective Status.
     */
    public KVMessageImpl get (String key){

        String resultValue = prop.getProperty(key);
        if (resultValue !=null)
            return new KVMessageImpl(key, resultValue, KVMessage.StatusType.GET_SUCCESS);
        else
            return new KVMessageImpl(key, "", KVMessage.StatusType.GET_ERROR);

    }

    /**
     * Writes an KV pair entry to file.
     * @param key the key of the KV pair to be written in the file.
     * @param value  the value, associated with the specified key.
     * @return    KVMessage representation of the KV pair written to the file with
     *            respective Status.
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

    /**
     * Remove an entry from the file representing the persistant space
     * @param key key of the KV pair to be removed
     * @return KVMessage representation of the KeyValue pair to be removed including
     * the Status of the operation
     */
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
