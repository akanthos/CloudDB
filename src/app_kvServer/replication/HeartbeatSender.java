package app_kvServer.replication;

import org.apache.log4j.Logger;

/**
 * Created by akanthos on 13.12.15.
 */
public class HeartbeatSender implements Runnable {
    private final Coordinator coordinator;
    private volatile boolean continueHeartbeating;
    private long heartbeatPeriod;
    private static Logger logger = Logger.getLogger(Replica.class);

    public HeartbeatSender(Coordinator coordinator, long heartbeatPeriod) {
        this.coordinator = coordinator;
        this.continueHeartbeating = true;
        this.heartbeatPeriod = heartbeatPeriod;
    }
    public synchronized void stop() {
        this.continueHeartbeating = false;
    }

    @Override
    public void run() {
        try {
            // TODO: Initial wait for everybody to settle? Sleep for 20 seconds initially
            Thread.sleep(20 * 1000);
        } catch (InterruptedException e) { }
        while (continueHeartbeating) {
            try {
                Thread.sleep(heartbeatPeriod);
            } catch (InterruptedException e) {
            }
            coordinator.sendHeartbeat();
        }
    }
}