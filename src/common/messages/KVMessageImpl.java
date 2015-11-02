package common.messages;

import helpers.Constants;
import helpers.MessageCommands;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

/**
 * Created by sreenath on 27/10/15.
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

    public KVMessageImpl(String key, String value, StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    public KVMessageImpl(String messageString) throws Exception {
        try {
            String[] msgParts = messageString.split(":");
            this.status = StatusType.valueOf(msgParts[0]);
            String[] keyAndValue = msgParts[1].split(",");
            this.key = keyAndValue[0];
            // For GET requests, value would be null
            if (keyAndValue.length > 1) {
                this.value = keyAndValue[1];
            } else {
                this.value = "";
            }
        } catch (Exception e) {
            //logger.error(String.format("Cannot parse message string"), e);
            throw new Exception("Unable to parse message string");
        }
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public StatusType getStatus() {
        return status;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public void setStatus(StatusType status) {
        this.status = status;
    }

    @Override
    public String toString() {
        StringBuilder msgString = new StringBuilder();
        msgString.append(status);
        msgString.append(":");
        msgString.append(key);
        msgString.append(",");
        msgString.append(value);
        return msgString.toString();
    }

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
