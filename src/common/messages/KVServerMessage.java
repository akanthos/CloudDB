package common.messages;

import common.ServerInfo;
import common.utils.KVRange;

import java.util.Date;
import java.util.List;

/**
 *
 * Server to server message interface
 */
public interface KVServerMessage extends AbstractMessage {
    enum StatusType {
        MOVE_DATA, 			        /* Move data message */
        MOVE_DATA_SUCCESS,
        MOVE_DATA_FAILURE,
        GENERAL_ERROR,
        SERVER_FAILURE,             /* Message to ECSInterface informing about a possible server failure */
        HEARTBEAT,                  /* Heartbeats between coordinators and replicas */
        HEARTBEAT_RESPONSE,
        REPLICATE,                  /* Coordinator message from coordinator to replicas */
        REPLICATE_SUCCESS,
        REPLICATE_FAILURE,
        GOSSIP,
        GOSSIP_SUCCESS,
        GOSSIP_FAILURE
    }

    /**
     *
     * @return
     */
    List<KVPair> getKVPairs();
    void setKVPairs(List<KVPair> kvPairs);

    /**
     * @return a status string that is used to identify request types,
     * response types and error types associated to the message.
     */
    StatusType getStatus();

    /**
     * Status setter
     * @param statusType the status to set
     */
    void setStatus(StatusType statusType);

    /**
     * The replica number of the server if the server message is of REPLICATE type
     * @return
     */
    String getReplicaID();

    void setReplicaID(String replicaID);

    Date getTimeOfSendingMsg();
    void setTimeOfSendingMsg(Date timeOfSendingMsg);
}
