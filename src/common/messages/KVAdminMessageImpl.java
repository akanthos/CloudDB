package common.messages;

import common.Serializer;
import common.ServerInfo;
import common.utils.KVRange;
import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Created by akanthos on 11.11.15.
 */
public class KVAdminMessageImpl implements KVAdminMessage, Serializable {

    List<ServerInfo> metadata;
    Integer cacheSize;
    String displacementStrategy;
    KVRange range;
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


    /**
     * MOVE_DATA message
     * @param status
     * @param range
     * @param serverInfo
     */
    public KVAdminMessageImpl(KVAdminMessage.StatusType status, KVRange range, ServerInfo serverInfo) {
        // This constructor is used in the MOVE_DATA command
        // where only the following attributes are needed
        this.status = status;
        this.range = range;
        this.serverInfo = serverInfo;
    }

    /**
     * UPDATE_METADATA message
     * @param status
     * @param metadata
     */
    public KVAdminMessageImpl(KVAdminMessage.StatusType status, List<ServerInfo> metadata) {
        // This constructor is used in the MOVE_DATA command
        // where only the following attributes are needed
        this.status = status;
        this.metadata = metadata;
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
    public void setCacheSize(Integer cacheSize) {
        this.cacheSize = cacheSize;
    }

    @Override
    public String getDisplacementStrategy() {
        return displacementStrategy;
    }
    @Override
    public void setDisplacementStrategy(String displacementStrategy) {
        this.displacementStrategy = displacementStrategy;
    }

    @Override
    public KVRange getRange() {
        return range;
    }

    @Override
    public void setRange(KVRange range) {
        this.range = range;
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
