package app_kvServer;

import common.ServerInfo;
import org.apache.log4j.Logger;

import java.util.Date;

/**
 * Created by akanthos on 10.12.15.
 */
public class Replica {
    private final ServerInfo info;
    private final String ID;
    private static Logger logger = Logger.getLogger(SocketServer.class);


    public Replica(String ID, ServerInfo info) {
        this.ID = ID;
        this.info = info;
    }
    public String getReplicaID() {
        return ID;
    }
    public ServerInfo getInfo() { return info; }
}
