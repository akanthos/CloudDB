package app_kvServer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by akanthos on 10.12.15.
 */
public class Replica {
    private int replicaNumber;
    private String sourceIP;
    private ReplicaTimer timer;

    public Replica(int replicaNumber, String sourceIP) {
        this.replicaNumber = replicaNumber;
        this.sourceIP = sourceIP;
        this.timer = new ReplicaTimer();
    }

    public int getReplicaNumber() {
        return replicaNumber;
    }

    public void heartbeat(Date timeStamp) {
        timer.heartbeat(timeStamp);
    }


    public String getSourceIP() {
        return sourceIP;
    }

    private class ReplicaTimer {
        private Date currentTimestamp;
        private final long HEARTBEAT_PERIOD = 5000; // In milliseconds

        public ReplicaTimer() {
            this.currentTimestamp = new Date();
            // TODO: Spawn thread to compare currentTimestamp and throw exception if exceeded
                // TODO: replica died, deregister it and inform ECS.
                // replicaHandler.deregisterReplica(replicaNumber);
                // messenger.sendToECS(sourceIP, replicaNumber);
        }

        public void heartbeat(Date newTimestamp) {
//            long diff = newTimestamp.getTime() - currentTimestamp.getTime();
//            long milliseconds = TimeUnit.MILLISECONDS.toMillis(diff);
//            if (milliseconds > HEARTBEAT_PERIOD) {
//                throw new TimeoutException();
//            }
            synchronized (currentTimestamp) {
               currentTimestamp = newTimestamp;
            }
        }
    }
}
