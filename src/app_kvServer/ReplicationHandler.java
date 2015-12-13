package app_kvServer;

import com.sun.corba.se.spi.activation.Server;
import common.ServerInfo;
import common.messages.KVPair;
import common.utils.KVRange;
import helpers.StorageException;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by akanthos on 10.12.15.
 */
public class ReplicationHandler {

    private final HashMap<String, Coordinator> coordinators;
    private final HashMap<String, Replica> replicas;
    private final HashMap<String, TimeoutWatch> timeoutWatches;
    private final HashMap<String, HeartbeatSender> heartbeatSenders;

    private final KVPersistenceEngine replicatedData;
    private final SocketServer server;
    private ExecutorService timeoutThreadpool;
    private final long heartbeatPeriod;
    private static final Logger logger = Logger.getLogger(SocketServer.class);


    public ReplicationHandler(SocketServer server, List<ServerInfo> metadata, KVRange range, long heartbeatPeriod) throws StorageException {
        this.server = server;
        this.heartbeatPeriod = heartbeatPeriod;
        this.coordinators = new HashMap<>();
        this.replicas = new HashMap<>();
        this.timeoutWatches = new HashMap<>();
        this.heartbeatSenders = new HashMap<>();
        findAndRegisterReplicas(metadata, range);
        findAndRegisterCoordinators(metadata, range);
        this.replicatedData = new KVPersistenceEngine("_replica");
        this.timeoutThreadpool = Executors.newCachedThreadPool();
    }

    private void findAndRegisterCoordinators(List<ServerInfo> metadata, KVRange range) {
        int size = metadata.size();
        for (int i=0; i<size; i++) {
            ServerInfo info = metadata.get(i);
            if (info.getServerRange().equals(range)) {
                String coordinator1ID, coordinator2ID;
                int offset1 = getOffsetOnRing(i-1, size);
                int offset2 = getOffsetOnRing(i-2, size);
                ServerInfo coordinator1Info = metadata.get(offset1);
                ServerInfo coordinator2Info = metadata.get(offset2);
                coordinator1ID = coordinator1Info.getID();
                coordinator2ID = coordinator2Info.getID();

                coordinators.put(coordinator1ID, new Coordinator(coordinator1ID, coordinator1Info, heartbeatPeriod));
                coordinators.put(coordinator2ID, new Coordinator(coordinator2ID, coordinator2Info, heartbeatPeriod));

                spawnTimeoutThreads();

                logger.info(info.getID() + ": Found my coordinators");
                break;
            }
        }
    }

    private int getOffsetOnRing(int index, int size) {
        int modulo = index % size;
        return (modulo>0) ? modulo : (size+modulo);
    }


    private void findAndRegisterReplicas(List<ServerInfo> metadata, KVRange range) {
        int size = metadata.size();
        for (int i=0; i<size; i++) {
            ServerInfo info = metadata.get(i);
            if (info.getServerRange().equals(range)) {
                String replica1ID, replica2ID;
                int offset1 = getOffsetOnRing(i+1, size);
                int offset2 = getOffsetOnRing(i+2, size);
                ServerInfo replica1Info = metadata.get(offset1);
                ServerInfo replica2Info = metadata.get(offset2);
                replica1ID = replica1Info.getID();
                replica2ID = replica2Info.getID();

                replicas.put(replica1ID, new Replica(replica1ID, replica1Info));
                replicas.put(replica2ID, new Replica(replica2ID, replica2Info));

                spawnHeartbeatThreads();

                logger.info(info.getID() + ": Found my replicas");
                break;
            }
        }
    }

    private void spawnTimeoutThreads() {
        for (String coordinatorID : coordinators.keySet()) {
            spawnTimeoutThread(coordinatorID);
        }
    }
    private void spawnTimeoutThread(String coordinatorID) {
        Coordinator c = coordinators.get(coordinatorID);
        TimeoutWatch t = new TimeoutWatch(this, c);
        timeoutWatches.put(coordinatorID, t);
        timeoutThreadpool.submit(t);
    }
    private void spawnHeartbeatThreads() {
        for (String replicaID : replicas.keySet()) {
            spawnHeartbeatThread(replicaID);
        }
    }
    private void spawnHeartbeatThread(String replicaID) {
        Replica r = replicas.get(replicaID);
        HeartbeatSender h = new HeartbeatSender(this, r);
        heartbeatSenders.put(replicaID, h);
        timeoutThreadpool.submit(h);
    }

    public void bulkInsert(String coordinatorID, List<KVPair> kvPairs) {
        synchronized (replicatedData) {
            for (KVPair pair : kvPairs) {
                replicatedData.put(pair.getKey(), pair.getValue());
            }
        }
    }

    public void bulkRemove(String coordinatorID, List<KVPair> kvPairs) {
        synchronized (replicatedData) {
            for (KVPair pair : kvPairs) {
                replicatedData.remove(pair.getKey());
            }
        }
    }

    public void removeRange(KVRange range) {
        synchronized (replicatedData) {
            replicatedData.remove(range);
        }
    }

    // TODO: Add/Remove replicas/coordinators ??
    private synchronized void deregisterCoordinator(String coordinatorID) {
        timeoutWatches.get(coordinatorID).stop();
        timeoutWatches.remove(coordinatorID);
        removeRange(coordinators.get(coordinatorID).getInfo().getServerRange());
        coordinators.remove(coordinatorID);
    }
    private synchronized void deregisterReplica(String replicaID) {
        heartbeatSenders.get(replicaID).stop();
        heartbeatSenders.remove(replicaID);
        replicas.remove(replicaID);
    }


    public void heartbeat(String replicaID, Date timeOfSendingMessage) {
        coordinators.get(replicaID).heartbeat(timeOfSendingMessage);
    }

    private synchronized void coordinatorFailed(String coordinatorID) {
        server.reportFailureToECS(coordinators.get(coordinatorID));
        deregisterCoordinator(coordinatorID);
    }
    private void sendHeartbeat(String replicaID) {
        server.sendHeartbeatToServer(replicas.get(replicaID));
    }

    public void cleanup() {
        /* Remove coordinators */
        coordinators.clear();
        /* Remove replica information */
        replicas.clear();
        /* Clean replicated data */
        replicatedData.cleanUp();
        /* Shutdown timers and heartbeats */
        timeoutThreadpool.shutdownNow();
        timeoutWatches.clear();
        heartbeatSenders.clear();
    }

    private class TimeoutWatch implements Runnable {
        final Coordinator coordinator;
        final ReplicationHandler replicationHandler;
        private volatile boolean continueChecking;

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
                Thread.sleep(60 * 1000); // Sleep for 1 minute initially
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

    private class HeartbeatSender implements Runnable {
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




}
