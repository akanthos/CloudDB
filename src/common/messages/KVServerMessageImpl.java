package common.messages;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by aacha on 11/16/2015.
 */
public class KVServerMessageImpl implements KVServerMessage {

    private List<KVPair> kvPairs;
    private StatusType status;
    private Integer serialNumber;

    /**
     * Information related to heartbeat messages
     */
    private Date timeOfSendingMsg;

    /**
     * Information related to replica messages.
     * Other variables used: sourceIP (coordinator IP), kvPairs (list of pairs that needs to be replicated)
     */
    private String replicaID;

    /**
     * Default constructor
     */
    public KVServerMessageImpl() {}

    /**
     * Constructor which sets the status for simple messages
     * like MOVE_DATA_SUCCESS or error message
     *
     * @param status the status of the message
     */
    public KVServerMessageImpl(StatusType status) {
        this.status = status;
    }

    /**
     * Constructor which sets the status and the key-value pairs
     * to be sent - To be used for MOVE_DATA command
     *
     * @param kvPairs the key-value pairs of the message
     * @param status the status of the message
     */
    public KVServerMessageImpl(ArrayList<KVPair> kvPairs, StatusType status) {
        this.kvPairs = kvPairs;
        this.status = status;
    }

    /**
     * Constructor which sets the status and the relevant information for
     * a HEARTBEAT message
     * @param coordinatorID the server that sends the heartbeat
     * @param timeOfSendingMsg timestamp of sending the message
     * @param status the status of the message
     */
    public KVServerMessageImpl(String coordinatorID, Date timeOfSendingMsg, StatusType status) {
        this.replicaID = coordinatorID;
        this.timeOfSendingMsg = timeOfSendingMsg;
        this.status = status;
    }

    /**
     * Constructor which sets the status and the key-value pairs
     * to be sent - To be used for REPLICATE command
     * @param kvPairs the key-value pairs to be replicated
     * @param status the status of the message (REPLICATE or GOSSIP)
     */
    public KVServerMessageImpl(List<KVPair> kvPairs, StatusType status) {
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

    @Override
    public Date getTimeOfSendingMsg() {
        return timeOfSendingMsg;
    }

    @Override
    public void setTimeOfSendingMsg(Date timeOfSendingMsg) {
        this.timeOfSendingMsg = timeOfSendingMsg;
    }

    public String getReplicaID() {
        return replicaID;
    }

    public void setReplicaID(String replicaID) {
        this.replicaID = replicaID;
    }
}
