package app_kvServer.replication;

import app_kvServer.SocketServer;
import common.ServerInfo;
import org.apache.log4j.Logger;

import java.util.Date;

/**
 * Class that represents a server's replica.
 * Contains the basic information that might be needed.
 */
public class Coordinator {
    private final ServerInfo info;
    private final ReplicationHandler handler;
    private final String ID;
    private long HEARTBEAT_PERIOD; // In milliseconds
    private HeartbeatSender heartbeatSender;
    private static Logger logger = Logger.getLogger(SocketServer.class);


    /**
     * Constructor of the Coordinator representation
     * @param ID the coordinator's id
     * @param info the coordinator's info
     * @param heartbeatPeriod the heartbeat period to used
     * @param handler the replication handler this coordinator is associated with
     */
    public Coordinator(String ID, ServerInfo info, long heartbeatPeriod, ReplicationHandler handler) {
        this.ID = ID;
        this.handler = handler;
        this.info = info;
        this.HEARTBEAT_PERIOD = heartbeatPeriod;
        spawnTimeoutThread();
    }

    /**
     * Coordinator's id getter
     * @return the coordinator's id
     */
    public String getCoordinatorID() {
        return ID;
    }

    /**
     * Coordinator's server info getter
     * @return the coordinator's server info
     */
    public ServerInfo getInfo() { return info; }

    /**
     * Spawns a heartbeat sender thread for the respective coordinator
     */
    private void spawnTimeoutThread() {
        heartbeatSender = new HeartbeatSender(this, HEARTBEAT_PERIOD);
        handler.submit(heartbeatSender);
    }

    /**
     * Stops the heartbeat thread associated with this coordinator
     */
    public void stop() {
        heartbeatSender.stop();
    }

    /**
     * Sends a heartbeat request to the respective coordinator
     */
    public void sendHeartbeat() {
        handler.sendHeartbeat(this.ID);
    }
}

