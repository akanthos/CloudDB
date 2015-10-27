package common.messages;

import helpers.Constants;
import helpers.MessageCommands;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Created by sreenath on 27/10/15.
 */
public class KVMessageImpl implements KVMessage {

    String key;
    String value;
    StatusType status;
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
            String[] keyAndValue = messageString.split(",");
            this.key = keyAndValue[0];
            this.value = keyAndValue[1];
        } catch (Exception e) {
            logger.error(String.format("Cannot parse message string: %s", messageString), e);
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
}
