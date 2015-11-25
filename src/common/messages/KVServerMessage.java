package common.messages;

import common.ServerInfo;
import common.utils.KVRange;

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
        GENERAL_ERROR
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
}
