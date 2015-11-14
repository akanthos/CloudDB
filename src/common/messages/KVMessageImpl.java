package common.messages;

import common.utils.KVMetadata;
import common.utils.Utilities;
import hashing.MD5Hash;
import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


/**
 * KVMessage represantion of connection Message
 */
public class KVMessageImpl implements KVMessage, Serializable {

    String key;
    String value;
    StatusType status;
    KVMetadata metadata;

    private static Logger logger = Logger.getLogger(KVMessageImpl.class);

    static {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
    }

    /**
     * Constructor
     * @param key Key of the connection KV Message
     * @param value Value of the connection KV Message
     * @param status Status of the connection KV Messag
     */
    public KVMessageImpl(String key, String value, StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
        metadata = new KVMetadata();
    }

    /**
     * Constructor using String representation of connection message
     * @param messageString String representation of the KV Message
     * @throws Exception
     */
    public KVMessageImpl(String messageString) throws Exception {
        try {
            String[] msgParts = messageString.split(":");
            this.status = StatusType.valueOf(msgParts[0]);
            String[] keyAndValue = msgParts[1].split("(?<!\\\\),");
            this.key = keyAndValue[0].replaceAll("\\\\,",",");
            // For GET requests, value would be null
            if (keyAndValue.length > 1) {
                this.value = keyAndValue[1].replaceAll("\\\\,",",");
            } else {
                this.value = "";
            }
        } catch (Exception e) {
            logger.error(String.format("Cannot parse message string"), e);
            throw new Exception("Unable to parse message string");
        }
    }

    /**
     * Key getter
     * @return key of Message
     */
    @Override
    public String getKey() {
        return key;
    }

    /**
     * Value getter
     * @return Value of Message
     */
    @Override
    public String getValue() {
        return value;
    }

    /**
     * Status getter
     * @return Status of Message
     */
    @Override
    public StatusType getStatus() {
        return status;
    }

    /**
     * Key setter
     * @param key key to set in Message
     */
    public void setKey(String key) {
        this.key = key;
    }

    /**
     * Key setter
     * @param value value to set in Message
     */
    public void setValue(String value) {
        this.value = value;
    }

    /**
     * Status setter
     * @param status status to set in Message
     */
    @Override
    public void setStatus(StatusType status) {
        this.status = status;
    }

    public KVMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(KVMetadata metadata) {
        this.metadata = metadata;
    }

    /**
     * toString method for KV Message
     * @return String representation of Message
     */
    @Override
    public String toString() {
        StringBuilder msgString = new StringBuilder();
        msgString.append(status);
        msgString.append(":");


        String delimitedKey = key.replaceAll(",", "\\\\,");
        msgString.append(delimitedKey);
        msgString.append(",");
        String delimitedValue = value.replaceAll(",", "\\\\,");
        msgString.append(delimitedValue);
        return msgString.toString();
    }

    /**
     *
     * @return bytes repreentation of Message
     * @throws UnsupportedEncodingException
     */
    public byte[] getMsgBytes() throws UnsupportedEncodingException {
        return Utilities.getBytes(this);
    }

    public long getHash() {
        MD5Hash md5 = new MD5Hash();
        return md5.hash(key);
    }
}
