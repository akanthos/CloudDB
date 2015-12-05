package common.messages;

import common.Serializer;
import common.ServerInfo;
import common.utils.KVRange;
import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Date;
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
    /**
     * For a server failure message, this will contain the failed server's details
     */
    ServerInfo failedServerInfo;

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


    public KVAdminMessageImpl(KVAdminMessage.StatusType status, KVRange range, ServerInfo serverInfo) {
        // This constructor is used in the MOVE_DATA command
        // where only the following attributes are needed
        this.status = status;
        this.range = range;
        this.serverInfo = serverInfo;
    }

    public KVAdminMessageImpl(KVAdminMessage.StatusType status, ServerInfo failedServerInfo) {
        // This constructor is used in the SERVER_FAILURE command
        // where only the following attributes are needed
        this.failedServerInfo = failedServerInfo;
        this.status = status;
    }

    /**
     * toString method for KV Message
     * @return String representation of Message
     */
    @Override
    public String toString() {
        // TODO: Using Serializer
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

    /**
     * Key range low limit setter
     * @param low the low limit to set
     */
    public void setLow(Long low){
        range.setLow(low);
    }

    /**
     * Key range high limit setter
     * @param high the high limit to set
     */
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

    public ServerInfo getFailedServerInfo() {
        return failedServerInfo;
    }

    public void setFailedServerInfo(ServerInfo failedServerInfo) {
        this.failedServerInfo = failedServerInfo;
    }
}
