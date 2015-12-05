package common.messages;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by aacha on 11/16/2015.
 */
public class KVServerMessageImpl implements KVServerMessage {

    List<KVPair> kvPairs;
    StatusType status;

    /**
     * Information related to heartbeat messages
     */
    String sourceIP;
    Date timeOfSendingMsg;

    /**
     * Information related to replica messages.
     * Other variables used: sourceIP (coordinator IP), kvPairs (list of pairs that needs to be replicated)
     */
    int replicaNumber;

    /**
     * Default constructor
     */
    public KVServerMessageImpl() {}

    /**
     * Constructor which sets the status for simple messages
     *
     * @param status the status of the message
     */
    public KVServerMessageImpl(StatusType status) {
        this.status = status;
    }

    /**
     * Constructor which sets the status and the key-value pairs
     * to be sent
     *
     * @param kvPairs the key-value pairs of the message
     * @param status the status of the message
     */
    public KVServerMessageImpl(ArrayList<KVPair> kvPairs, StatusType status) {
        this.kvPairs = kvPairs;
        this.status = status;
    }

    @Override
    public List<KVPair> getKVPairs() {
        return kvPairs;
    }

    @Override
    public void setKVPairs(List<KVPair> kvPair) {
        this.kvPairs = kvPair;
    }

    @Override
    public StatusType getStatus() {
        return status;
    }

    @Override
    public void setStatus(StatusType statusType) {
        this.status = statusType;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.SERVER_MESSAGE;
    }

    public List<KVPair> getKvPairs() {
        return kvPairs;
    }

    public void setKvPairs(List<KVPair> kvPairs) {
        this.kvPairs = kvPairs;
    }

    public String getSourceIP() {
        return sourceIP;
    }

    public void setSourceIP(String sourceIP) {
        this.sourceIP = sourceIP;
    }

    public Date getTimeOfSendingMsg() {
        return timeOfSendingMsg;
    }

    public void setTimeOfSendingMsg(Date timeOfSendingMsg) {
        this.timeOfSendingMsg = timeOfSendingMsg;
    }

    public int getReplicaNumber() {
        return replicaNumber;
    }

    public void setReplicaNumber(int replicaNumber) {
        this.replicaNumber = replicaNumber;
    }
}
