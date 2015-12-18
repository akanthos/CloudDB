package app_kvServer.replication;

import common.ServerInfo;
import org.apache.log4j.Logger;

/**
 * Created by akanthos on 10.12.15.
 */
public class Replica {
    private final ServerInfo info;
    private ReplicationHandler handler;
    private final String ID;
    private static Logger logger = Logger.getLogger(Replica.class);


    public Replica(ReplicationHandler handler, String ID, ServerInfo info) {
        this.handler = handler;
        this.ID = ID;
        this.info = info;
    }

    public String getReplicaID() {
        return ID;
    }
    public ServerInfo getInfo() { return info; }

}
