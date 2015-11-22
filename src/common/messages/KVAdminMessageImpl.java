package common.messages;

import common.Serializer;
import common.ServerInfo;
import common.utils.KVRange;
import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by akanthos on 11.11.15.
 */
public class KVAdminMessageImpl implements KVAdminMessage, Serializable {

    List<ServerInfo> metadata = new ArrayList<ServerInfo>();
    Integer cacheSize;
    String displacementStrategy;
    KVRange range = new KVRange();
    ServerInfo serverInfo;
    KVAdminMessage.StatusType status;

    private static Logger logger = Logger.getLogger(KVMessageImpl.class);

    static {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
    }

    public KVAdminMessageImpl () {

    }

    /**
     * Simple message like START, STOP, START_SUCCESS, ...
     *
     * @param status
     */
    public KVAdminMessageImpl(KVAdminMessage.StatusType status) {
        // This constructor is about simple acknowledge returns to messages like
        // START, STOP, WRITE_LOCK, WRITE_UNLOCK, SHUTDOWN
        // It returns usually a SUCCESS or OPERATION_FAILED
        this.status = status;
    }

    /**
<<<<<<< HEAD
=======
     * Contructor using String represantion of connection message
     * @param messageString String representation of the KV Message
     * @throws Exception
     */
    public KVAdminMessageImpl(String messageString) throws Exception {

//        try {
//            String[] msgParts = messageString.split(":");
//            this.status = KVAdminMessage.StatusType.valueOf(msgParts[0]);
//            if (this.status.equals(StatusType.INIT)) {
//                String[] initMessageParts = msgParts[1].split(",");
//                this.metadata = new KVMetadata(initMessageParts[0]);
//                this.cacheSize = Integer.valueOf(initMessageParts[1]);
//                this.displacementStrategy = initMessageParts[2];
//            } else if (this.status.equals(StatusType.MOVE_DATA)) {
//                String[] moveMsgParts = msgParts[1].split(",");
//                this.range = new KVRange(moveMsgParts[0]);
//                this.serverInfo = new ServerInfo(moveMsgParts[1]);
//            } else if (this.status.equals(StatusType.UPDATE_METADATA)) {
//                this.metadata = new KVMetadata(msgParts[1]);
//            } else {
//                logger.error(String.format("Unable to instantiate KVAdminMessageImpl. Unknown message format: %s", messageString));
//                throw new Exception("Unknown message format");
//            }
//        } catch (Exception e) {
//            logger.error(String.format("Cannot parse message string"), e);
//            throw new Exception("Unable to parse message string");
//        }
    }

    /**
>>>>>>> 16a7e542787e5249be1b491de5f47db2a9e9c3ca
     * INIT message
     *
     * @param status
     * @param metadata
     * @param cacheSize
     * @param displacementStrategy
     */
    public KVAdminMessageImpl(KVAdminMessage.StatusType status, List<ServerInfo> metadata, Integer cacheSize, String displacementStrategy) {
        // This constructor is used in the INIT message
        // where only the following attributes are needed
        this.status = status;
        this.metadata = metadata;
        this.cacheSize = cacheSize;
        this.displacementStrategy = displacementStrategy;
    }


    public KVAdminMessageImpl(KVAdminMessage.StatusType status, KVRange range, ServerInfo serverInfo) {
        // This constructor is used in the MOVE_DATA command
        // where only the following attributes are needed
        this.status = status;
        this.range = range;
        this.serverInfo = serverInfo;
    }

    /**
     * toString method for KV Message
     * @return String representation of Message
     */
    @Override
    public String toString() {
        // TODO: Using Serializer

//        StringBuilder msgString = new StringBuilder();
//        msgString.append(status);
//        msgString.append(":");
//        if (status.equals(StatusType.INIT)) {
//            msgString.append(metadata.toString());
//            msgString.append(",");
//            msgString.append(cacheSize);
//            msgString.append(",");
//            msgString.append(displacementStrategy);
//        }
//        else if (status.equals(StatusType.MOVE_DATA)) {
//            msgString.append(range.toString());
//            msgString.append(",");
//            msgString.append(serverInfo+"");
//        }
//        else if (status.equals(StatusType.UPDATE_METADATA)) {
//            msgString.append(metadata.toString());
//        }
//        else {
//
//        }


//        String delimitedValue = value.replaceAll(",", "\\\\,");

//        msgString.append(delimitedValue);
        return Serializer.toByteArray(this).toString();
    }

    /**
     *
     * @return bytes repreentation of Message
     * @throws UnsupportedEncodingException
     */
    public byte[] getMsgBytes() {
        return Serializer.toByteArray(this);
    }

    @Override
    public List<ServerInfo> getMetadata() {
        return metadata;
    }

    @Override
    public void setMetadata(List<ServerInfo> metadata) {
        this.metadata = metadata;
    }

    @Override
    public Integer getCacheSize() {
        return cacheSize;
    }

    @Override
    public void setCacheSize(Integer cacheS) {
        this.cacheSize=cacheS;
    }


    @Override
    public String getDisplacementStrategy() {
        return displacementStrategy;
    }

    @Override
    public void setDisplacementStrategy(String strategy) {
        this.displacementStrategy = strategy;
    }

    @Override
    public KVRange getRange() {
        return range;
    }

    @Override
    public void setRange(Long low, Long high) {
    }

    public void setLow(Long low){
        range.setLow(low);
    }

    public void setHigh(Long high){
        range.setHigh(high);
    }

    @Override
    public ServerInfo getServerInfo() {
        return serverInfo;
    }

    @Override
    public void setServerInfo(ServerInfo server) {
        this.serverInfo = server;
    }

    @Override
    public KVAdminMessage.StatusType getStatus() {
        return status;
    }

    @Override
    public void setStatus(KVAdminMessage.StatusType statusType) {
        this.status = statusType;
    }


    @Override
    public MessageType getMessageType() {
        return MessageType.ECS_MESSAGE;
    }
}
