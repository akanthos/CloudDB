package common.messages;

import common.ServerInfo;
import common.utils.KVRange;
import jdk.internal.util.xml.impl.Pair;

import java.util.List;

/**
 * Created by aacha on 11/16/2015.
 */
public interface KVServerMessage extends AbstractMessage {
    enum StatusType {
        MOVE_DATA, 			        /* Move data message */
        MOVE_DATA_SUCCESS,
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

    void setStatus(StatusType statusType);
}
