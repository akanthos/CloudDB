package common.messages;

import app_kvEcs.ServerInfo;
import common.utils.KVMetadata;
import common.utils.KVRange;

/**
 * Created by akanthos on 11.11.15.
 */
public interface KVAdminMessage extends GenericMessage {

    public enum StatusType {
        INIT, 			        /* Server initialization */
        INIT_SUCCESS,
        START, 		            /* Server start, so that he accepts client requests too */
        START_SUCCESS,
        STOP, 	                /* Server stop, so that he doesn't accept client requests */
        STOP_SUCCESS,
        SHUT_DOWN, 		        /* Exits the KVServer application */
        SHUT_DOWN_SUCCESS,
        LOCK_WRITE, 	        /* Lock KVServer for write operations */
        LOCK_WRITE_SUCCESS,
        UNLOCK_WRITE, 	        /* Unlock KVServer for write operations */
        UNLOCK_WRITE_SUCCESS,
        MOVE_DATA, 		        /* Transfer subrange to another server and notify ECS when complete */
        MOVE_DATA_SUCCESS,
        UPDATE_METADATA,        /* Update meta-data repository */
        OPERATION_FAILED,       /* Operation requested by the ECS failed */
        GENERAL_ERROR           /* For other types of errors */
    }

    /**
     * @return the metadata that is associated with this message.
     *
     */
    public KVMetadata getMetadata();

    /**
     * @return the cache size that is associated with this message.
     *
     */
    public Integer getCacheSize();

    /**
     *
     * @return the cache displacement strategy that is associated
     *         with this message.
     */
    public String getDisplacementStrategy();

    /**
     *
     * @return the range that is associated to the message
     */
    public KVRange getRange();

    /**
     *
     * @return the information about the target server associated
     *         to this message
     */
    public ServerInfo getServerInfo();

    /**
     * @return a status string that is used to identify request types,
     * response types and error types associated to the message.
     */
    public StatusType getStatus();

    public void setStatus(StatusType statusType);
}
