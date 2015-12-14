package app_kvServer.replication;

import app_kvServer.SocketServer;
import common.ServerInfo;
import org.apache.log4j.Logger;

import java.util.Date;

/**
 * Created by akanthos on 10.12.15.
 */
public class Coordinator {
    private final ServerInfo info;
    private final ReplicationHandler handler;
    private final String ID;
    private long HEARTBEAT_PERIOD; // In milliseconds
    private HeartbeatSender heartbeatSender;
    private static Logger logger = Logger.getLogger(SocketServer.class);


    public Coordinator(String ID, ServerInfo info, long heartbeatPeriod, ReplicationHandler handler) {
        this.ID = ID;
        this.handler = handler;
        this.info = info;
        this.HEARTBEAT_PERIOD = heartbeatPeriod;
        spawnTimeoutThread();
    }

    public String getCoordinatorID() {
        return ID;
    }
    public ServerInfo getInfo() { return info; }

    private void spawnTimeoutThread() {
        heartbeatSender = new HeartbeatSender(this, HEARTBEAT_PERIOD);
        handler.submit(heartbeatSender);
    }

    public void stop() {
        heartbeatSender.stop();
    }

    public void sendHeartbeat() {
        handler.sendHeartbeat(this.ID);
    }
}

