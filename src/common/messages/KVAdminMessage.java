package common.messages;

import common.ServerInfo;
import common.utils.KVRange;

import java.util.List;

/**
 * Admin (ECS) message interface
 */
public interface KVAdminMessage extends AbstractMessage {

    enum StatusType {
        INIT, 			        /* Server initialization */
        START, 		            /* Server start, so that he accepts client requests too */
        STOP, 	                /* Server stop, so that he doesn't accept client requests */
        SHUT_DOWN, 		        /* Exits the KVServer application */
        LOCK_WRITE, 	        /* Lock KVServer for write operations */
        UNLOCK_WRITE, 	        /* Unlock KVServer for write operations */
        MOVE_DATA, 		        /* Transfer subrange to another server and notify ECSImpl when complete */
        UPDATE_METADATA,        /* Update meta-data repository */
        OPERATION_SUCCESS,      /* Operation requested by the ECS was successful */
        OPERATION_FAILED,       /* Operation requested by the ECS failed */
        GENERAL_ERROR,          /* For other types of errors */
        SERVER_FAILURE          /* Message to ECS informing about a possible server failure */
    }

    /**r
     * @return the metadata that is associated with this message.
     *
     */
    List<ServerInfo> getMetadata();

    /**
     * Metadata setter
     * @param metadata the metadata to be set
     */
    void setMetadata(List<ServerInfo> metadata);

    /**
     * @return the cache size that is associated with this message.
     *
     */
    Integer getCacheSize();

    /**
     * Cache size setter
     * @param cacheS the cache size to be set
     */
    void setCacheSize(Integer cacheS);

    /**
     *
     * @return the cache displacement strategy that is associated
     *         with this message.
     */
    String getDisplacementStrategy();

    /**
     * Displacement strategy setter
     * @param strategy the displacement stratef=gy to be set
     */
    void setDisplacementStrategy(String strategy);

    /**
     *
     * @return the range that is associated to the message
     */
    KVRange getRange();

    /**
     *
     * @return the range that is associated to the message
     */
    void setRange(Long low, Long high);

    /**
     *
     * @return the information about the target server associated
     *         to this message
     */
    ServerInfo getServerInfo();

    /**
     * Server info setter
     * @param server the server info to be set
     */
    void setServerInfo(ServerInfo server);

    /**
     * @return a status string that is used to identify request types,
     * response types and error types associated to the message.
     */
    StatusType getStatus();

    /**
     * Status setter
     *
     * @param statusType status to set to
     */
    void setStatus(StatusType statusType);
}
