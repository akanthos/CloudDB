package app_kvServer.replication;

import app_kvServer.SocketServer;
import common.ServerInfo;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Created by akanthos on 10.12.15.
 */
public class Coordinator {
    private final ServerInfo info;
    private final ReplicationHandler handler;
    private final String ID;
    private long HEARTBEAT_PERIOD; // In milliseconds
    private Date currentTimestamp;
    private long timeDiff;
    private TimeoutWatch timeoutWatch;
    private static Logger logger = Logger.getLogger(SocketServer.class);


    public Coordinator(String ID, ServerInfo info, long heartbeatPeriod, ReplicationHandler handler) {
        this.ID = ID;
        this.handler = handler;
        this.info = info;
        this.currentTimestamp = new Date();
        this.timeDiff = 0;
        this.HEARTBEAT_PERIOD = heartbeatPeriod;
        spawnTimeoutThread();
    }

    public String getCoordinatorID() {
        return ID;
    }
    public ServerInfo getInfo() { return info; }

    public synchronized void heartbeat(Date newTimestamp) {
        this.timeDiff = TimeUnit.MILLISECONDS.toMillis(newTimestamp.getTime() - currentTimestamp.getTime());
        this.currentTimestamp = newTimestamp;
    }

    public synchronized boolean timestampDiffExceededPeriod() {
        return (timeDiff > HEARTBEAT_PERIOD);
    }

    private void spawnTimeoutThread() {
        timeoutWatch= new TimeoutWatch(handler, this);
        handler.submit(timeoutWatch);
    }

    public void stop() {
        timeoutWatch.stop();
    }
}

