package app_kvServer.replication;

import common.ServerInfo;
import org.apache.log4j.Logger;

/**
 * Class that represents a server's replica.
 * Contains the basic information that might be needed.
 */
public class Replica {
    private final ServerInfo info;
    private ReplicationHandler handler;
    private final String ID;
    private static Logger logger = Logger.getLogger(Replica.class);


    /**
     * Constructor of the replica
     * @param handler the replication handler this replica is associated with
     * @param ID the replica's id
     * @param info the server info of the replica
     */
    public Replica(ReplicationHandler handler, String ID, ServerInfo info) {
        this.handler = handler;
        this.ID = ID;
        this.info = info;
    }

    /**
     * Replica ID getter
     * @return the replica's id
     */
    public String getReplicaID() {
        return ID;
    }

    /**
     * Replica's server info getter
     * @return the replica's server info
     */
    public ServerInfo getInfo() { return info; }

}
