package app_kvServer.replication;

import org.apache.log4j.Logger;

/**
 * This class represents a timer to trigger the heartbeat request
 * to the respective coordinator
 */
public class HeartbeatSender implements Runnable {
    private final Coordinator coordinator;
    private volatile boolean continueHeartbeating;
    private long heartbeatPeriod;
    private static Logger logger = Logger.getLogger(Replica.class);

    /**
     * Constructor for the heartbeat sender
     * @param coordinator the coordinator this heartbeat sender is associated with
     * @param heartbeatPeriod the heartbeat period to be used
     */
    public HeartbeatSender(Coordinator coordinator, long heartbeatPeriod) {
        this.coordinator = coordinator;
        this.continueHeartbeating = true;
        this.heartbeatPeriod = heartbeatPeriod;
    }

    /**
     * Stops the heartbeating thread
     */
    public synchronized void stop() {
        this.continueHeartbeating = false;
    }

    /**
     * Periodically asks for heartbeat request from the respective coordinator
     */
    @Override
    public void run() {
        try {
            // TODO: Initial wait for everybody to settle? Sleep for 20 seconds initially
            Thread.sleep(10 * 1000);
        } catch (InterruptedException e) { }
        while (continueHeartbeating) {
            try {
                Thread.sleep(heartbeatPeriod);
            } catch (InterruptedException e) {
            }
            if (continueHeartbeating)
                coordinator.sendHeartbeat();
        }
    }
}