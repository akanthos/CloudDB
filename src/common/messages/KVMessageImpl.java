package common.messages;

import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;


/**
 * KVMessage represantion of connection Message
 */
public class KVMessageImpl implements KVMessage, Serializable {

    String key;
    String value;
    StatusType status;
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;
    private static Logger logger = Logger.getLogger(KVMessageImpl.class);

    static {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
    }

    /**
     * Constractor
     * @param key Key of the connection KV Message
     * @param value Value of the connection KV Message
     * @param status Status of the connection KV Messag
     */
    public KVMessageImpl(String key, String value, StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    /**
     * Contructor using String represantion of connection message
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
        byte[] bytes;
        byte[] ctrBytes;
        byte[] tmp;
        try {
            bytes = this.toString().getBytes("UTF-8");
            ctrBytes = new byte[]{LINE_FEED, RETURN};
            tmp = new byte[bytes.length + ctrBytes.length];

            System.arraycopy(bytes, 0, tmp, 0, bytes.length);
            System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

        } catch (UnsupportedEncodingException e) {
            logger.error(String.format("Cannot convert message to byte array"), e);
            throw new UnsupportedEncodingException("Cannot convert message to byte array");
        }
        return tmp;
    }
}
