package app_kvServer;

import common.ServerInfo;
import org.apache.log4j.Logger;

import java.util.Date;

/**
 * Created by akanthos on 10.12.15.
 */
public class Replica {
    private final ServerInfo info;
    private ReplicationHandler handler;
    private final String ID;
    private HeartbeatSender heartbeatSender;
    private static Logger logger = Logger.getLogger(Replica.class);


    public Replica(ReplicationHandler handler, String ID, ServerInfo info) {
        this.handler = handler;
        this.ID = ID;
        this.info = info;
        spawnHeartbeatThread();
    }
    public String getReplicaID() {
        return ID;
    }
    public ServerInfo getInfo() { return info; }

    private void spawnHeartbeatThread() {
        heartbeatSender = new HeartbeatSender(handler, this);
        handler.submit(heartbeatSender);
    }

    public void stop() {
        heartbeatSender.stop();
    }
}
