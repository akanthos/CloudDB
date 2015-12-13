package app_kvServer;

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

    private HashMap<String, Coordinator> coordinators;
    private HashMap<String, Replica> replicas;
    private HashMap<String, TimeoutWatch> timeoutWatches;

    private final KVPersistenceEngine replicatedData;
    private SocketServer server;
    private ExecutorService timeoutThreadpool;
    private long heartbeatPeriod;
    private static Logger logger = Logger.getLogger(SocketServer.class);


    public ReplicationHandler(SocketServer server, List<ServerInfo> metadata, KVRange range, long heartbeatPeriod) throws StorageException {
        this.server = server;
        this.heartbeatPeriod = heartbeatPeriod;
        this.coordinators = new HashMap<>();
        this.replicas = new HashMap<>();
        this.timeoutWatches = new HashMap<>();
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

                spawnTimeoutThreads(); // TODO: Start heartbeats detection??

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
                replica1ID = metadata.get(offset1).getID();
                replica2ID = metadata.get(offset2).getID();

                replicas.put(replica1ID, new Replica()); // TODO: Create proper Replica objects
                replicas.put(replica2ID, new Replica());

                logger.info(info.getID() + ": Found my replicas");
                break;
            }
        }
    }

//    public synchronized void registerCoordinator(int replicaNumber,
//                                                 String sourceIP,
//                                                 List<KVPair> kvPairs, long heartbeatPeriod) {
//        bulkInsert(replicatedData, kvPairs);
//    }

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
    private void stopTimeoutThread(String coordinatorID) {
        TimeoutWatch t = timeoutWatches.get(coordinatorID);
        // TODO: How can we shut it down ??
        timeoutWatches.remove(coordinatorID);
    }

    private void bulkInsert(KVPersistenceEngine kvPersistenceEngine, List<KVPair> kvPairs) {
        synchronized (replicatedData) {
            for (KVPair pair : kvPairs) {
                kvPersistenceEngine.put(pair.getKey(), pair.getValue());
            }
        }
    }

    private void bulkRemove(KVPersistenceEngine kvPersistenceEngine, List<KVPair> kvPairs) {
        synchronized (replicatedData) {
            for (KVPair pair : kvPairs) {
                kvPersistenceEngine.remove(pair.getKey());
            }
        }
    }

    public synchronized void deregisterCoordinator(String coordinatorID) {
        coordinators.remove(coordinatorID);
    }


    public void heartbeat(String sourceIP, int replicaNumber) {
        coordinators.get(replicaNumber).heartbeat(new Date());
    }

    private class TimeoutWatch implements Runnable {
        Coordinator coordinator;
        ReplicationHandler replicationHandler;
        public TimeoutWatch(ReplicationHandler replicationHandler, Coordinator coordinator) {
            this.replicationHandler = replicationHandler;
            this.coordinator = coordinator;
        }
        @Override
        public void run() {
            try {
                Thread.sleep(60 * 1000); // Sleep for 1 minute initially
            } catch (InterruptedException e) { }
            boolean sleep = true;
            while (sleep) {
                try {
                    sleep = false;
                    Thread.sleep(60 * 000); // Sleep for 1 minute
                } catch (InterruptedException e) {
                    sleep = true;
                }
                if (!coordinator.timestampDiffExceededPeriod()) {
                    // Continue sleeping
                    sleep = true;
                }
            }
            // Timestamp too old, node is dead
            logger.info("Detected Failure for Coordinator " + coordinator.getCoordinatorID());
            replicationHandler.coordinatorFailed(coordinator.getCoordinatorID());
        }
    }

    private synchronized void coordinatorFailed(String coordinatorID) {
        server.reportFailureToECS(coordinators.get(coordinatorID));
        deregisterCoordinator(coordinatorID);
    }

    public void cleanup() {
        /* Remove coordinators */
        coordinators.clear();
        /* Remove replica information */
        replicas.clear();
        /* Clean replicated data */
        replicatedData.cleanUp();
        /* Shutdown timers */
        timeoutThreadpool.shutdownNow();
    }


}
