package app_kvServer;

/**
 * Created by akanthos on 13.12.15.
 */
public class HeartbeatSender implements Runnable {
    final Replica replica;
    final ReplicationHandler replicationHandler;
    private volatile boolean continueHeartbeating;

    public HeartbeatSender(ReplicationHandler replicationHandler, Replica replica) {
        this.replicationHandler = replicationHandler;
        this.replica = replica;
        this.continueHeartbeating = true;
    }

    public synchronized void stop() {
        this.continueHeartbeating = false;
    }

    @Override
    public void run() {
        while (continueHeartbeating) {
            try {
                Thread.sleep(30 * 1000); // Sleep for 1 minute
            } catch (InterruptedException e) {
            }
            replicationHandler.sendHeartbeat(replica.getReplicaID());
        }
    }
}
