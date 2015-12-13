package app_kvServer;

import common.ServerInfo;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Created by akanthos on 10.12.15.
 */
public class Coordinator {
    private final ServerInfo info;
    private final String ID;
    private long HEARTBEAT_PERIOD; // In milliseconds
    private Date currentTimestamp;
    private static Logger logger = Logger.getLogger(SocketServer.class);


    public Coordinator(String ID, ServerInfo info, long heartbeatPeriod) {
        this.ID = ID;
        this.info = info;
        this.currentTimestamp = new Date();
        this.HEARTBEAT_PERIOD = heartbeatPeriod;
    }

    public String getCoordinatorID() {
        return ID;
    }
    public ServerInfo getInfo() { return info; }

    public void heartbeat(Date newTimestamp) {
        synchronized (currentTimestamp) {
            currentTimestamp = newTimestamp;
        }
    }

    public void periodExpired() {
        // To be called from thread waiting for period
    }

    public boolean timestampDiffExceededPeriod() {
        Date newTimestamp = new Date();
        long diff;
        synchronized (currentTimestamp) {
            diff = newTimestamp.getTime() - currentTimestamp.getTime();
        }
        long milliseconds = TimeUnit.MILLISECONDS.toMillis(diff);
        return (milliseconds > HEARTBEAT_PERIOD);
    }
}
