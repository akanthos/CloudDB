package app_kvServer.replication;

import org.apache.log4j.Logger;

/**
 * Created by akanthos on 13.12.15.
 */
public class TimeoutWatch implements Runnable {
    private final Coordinator coordinator;
    private final ReplicationHandler replicationHandler;
    private volatile boolean continueChecking;
    private static Logger logger = Logger.getLogger(Replica.class);

    public TimeoutWatch(ReplicationHandler replicationHandler, Coordinator coordinator) {
        this.replicationHandler = replicationHandler;
        this.coordinator = coordinator;
        this.continueChecking = true;
    }
    public synchronized void stop() {
        this.continueChecking = false;
    }

    @Override
    public void run() {
        try {
            // TODO: Initial wait for everybody to settle? Sleep for 1 minute initially
            Thread.sleep(60 * 1000);
        } catch (InterruptedException e) { }
        boolean noTimeExceeded = true;
        while ( noTimeExceeded && continueChecking ) {
            try {
                noTimeExceeded = false;
                Thread.sleep(60 * 1000); // Sleep for 1 minute
            } catch (InterruptedException e) {
            }
            if (!coordinator.timestampDiffExceededPeriod()) {
                // Continue sleeping
                noTimeExceeded = true;
            }
        }
        if (continueChecking) {
            // We were not stopped from outside
            // Timestamp too old, node is dead
            logger.info("Detected Failure for Coordinator " + coordinator.getCoordinatorID());
            replicationHandler.coordinatorFailed(coordinator.getCoordinatorID());
        }
    }
}