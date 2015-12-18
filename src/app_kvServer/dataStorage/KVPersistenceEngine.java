package app_kvServer.dataStorage;

import common.ServerInfo;
import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import common.messages.KVPair;
import common.utils.KVRange;
import hashing.MD5Hash;
import helpers.StorageException;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * Class handling the communication with
 * the Store File (Persistant Storage) read/write
 */
public class KVPersistenceEngine {

    private Properties prop;
    private final String fileNamePrefix = "data.store";
    private  String fileName;
    private InputStream input = null;
    private OutputStream output = null;
    private static Logger logger = Logger.getLogger(KVPersistenceEngine.class);

    public KVPersistenceEngine(String suffix) throws StorageException {
        initialization(fileNamePrefix + suffix);
    }
    public KVPersistenceEngine(int replicaNumber) throws StorageException {
        initialization(fileNamePrefix + "_replica_" + String.valueOf(replicaNumber));
    }
    /**
     *
     * @throws StorageException
     * indicating problems accessing the persistant file
     */
    public KVPersistenceEngine(ServerInfo info) throws StorageException {
        initialization(fileNamePrefix + String.valueOf(info.getServerPort()));
    }

    /**
     * Initialization of storage file
     * @param persistenceFileName name used for the storage file
     * @throws StorageException
     */
    private void initialization(String persistenceFileName) throws StorageException {
        fileName = persistenceFileName;
        try {
            File storeFile = new File(fileName);
            prop = new Properties();
            if(storeFile.exists()) {
                storeFile.delete();
            }
            storeFile.createNewFile();
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

    /**
     * Removes a whole key range from the persistence file.
     * @param range the range of keys that need to be removed
     */
    public void remove (KVRange range){
        for (String key : prop.stringPropertyNames()) {
            MD5Hash md5 = new MD5Hash();
            Long hashedKey = md5.hash(key);
            if (range.isIndexInRange(hashedKey)) {
                this.remove(key);
            }
        }
    }

    public List<KVPair> get (KVRange range){
        List<KVPair> pairs = new ArrayList<>();
        for (String key : prop.stringPropertyNames()) {
            MD5Hash md5 = new MD5Hash();
            Long hashedKey = md5.hash(key);
            if (range.isIndexInRange(hashedKey)) {
                String resultValue = prop.getProperty(key);
                if (resultValue != null)
                    pairs.add(new KVPair(key, resultValue));
            }
        }
        return pairs;
    }

    /**
     * Cleans up the persistence file
     */
    public void cleanUp() {
        File storeFile = new File(fileName);
        if(storeFile.exists()) {
            storeFile.delete();
        }
    }
}
