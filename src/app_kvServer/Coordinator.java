package app_kvServer;

import org.apache.log4j.Logger;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Created by akanthos on 10.12.15.
 */
public class Coordinator {
    private int coordinatorNumber;
    private String sourceIP;
    private long HEARTBEAT_PERIOD; // In milliseconds
    private Date currentTimestamp;
    private static Logger logger = Logger.getLogger(SocketServer.class);


    public Coordinator(int coordinatorNumber, String sourceIP, long heartbeatPeriod) {
        this.coordinatorNumber = coordinatorNumber;
        this.sourceIP = sourceIP;
        this.currentTimestamp = new Date();
        this.HEARTBEAT_PERIOD = heartbeatPeriod;
    }

    public int getCoordinatorNumber() {
        return coordinatorNumber;
    }

    public void heartbeat(Date newTimestamp) {
        synchronized (currentTimestamp) {
            currentTimestamp = newTimestamp;
        }
    }

    public void periodExpired() {
        // To be called from thread waiting for period
    }


    public String getSourceIP() {
        return sourceIP;
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
