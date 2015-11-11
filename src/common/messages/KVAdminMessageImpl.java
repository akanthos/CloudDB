package common.messages;

import app_kvEcs.ServerInfo;
import common.utils.KVMetadata;
import common.utils.KVRange;
import common.utils.Utilities;
import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.pattern.IntegerPatternConverter;

import javax.rmi.CORBA.Util;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;

/**
 * Created by akanthos on 11.11.15.
 */
public class KVAdminMessageImpl implements KVAdminMessage, Serializable {

    KVMetadata metadata;
    Integer cacheSize;
    String displacementStrategy;
    KVRange range;
    ServerInfo serverInfo;
    KVAdminMessage.StatusType status;

    private static Logger logger = Logger.getLogger(KVMessageImpl.class);

    static {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
    }

    /**
     * Simple message like START, STOP, START_SUCCESS, ...
     *
     * @param status
     */
    public KVAdminMessageImpl(KVAdminMessage.StatusType status) {
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
    public KVAdminMessageImpl(KVAdminMessage.StatusType status, KVMetadata metadata, Integer cacheSize, String displacementStrategy) {
        this.status = status;
        this.metadata = metadata;
        this.cacheSize = cacheSize;
        this.displacementStrategy = displacementStrategy;
    }


    public KVAdminMessageImpl(KVAdminMessage.StatusType status, KVRange range, ServerInfo serverInfo) {
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
        // TODO:
        return null;
//        StringBuilder msgString = new StringBuilder();
//        msgString.append(status);
//        msgString.append(":");
//
//
//        String delimitedKey = key.replaceAll(",", "\\\\,");
//        msgString.append(delimitedKey);
//        msgString.append(",");
//        String delimitedValue = value.replaceAll(",", "\\\\,");
//        msgString.append(delimitedValue);
//        return msgString.toString();
    }

    /**
     *
     * @return bytes repreentation of Message
     * @throws UnsupportedEncodingException
     */
    public byte[] getMsgBytes() throws UnsupportedEncodingException {
        return Utilities.getBytes(this);
    }

    @Override
    public KVMetadata getMetadata() {
        return metadata;
    }

    @Override
    public Integer getCacheSize() {
        return cacheSize;
    }

    @Override
    public String getDisplacementStrategy() {
        return displacementStrategy;
    }

    @Override
    public KVRange getRange() {
        return range;
    }

    @Override
    public ServerInfo getServerInfo() {
        return serverInfo;
    }

    @Override
    public KVAdminMessage.StatusType getStatus() {
        return status;
    }

    @Override
    public void setStatus(KVAdminMessage.StatusType statusType) {
        this.status = statusType;
    }


}
