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
    private final KVPersistenceEngine replicatedData;
    private ExecutorService timeoutThreadpool;
    private long heartbeatPeriod;
    private static Logger logger = Logger.getLogger(SocketServer.class);


    public ReplicationHandler(List<ServerInfo> metadata, KVRange range, long heartbeatPeriod) throws StorageException {
        this.heartbeatPeriod = heartbeatPeriod;
        this.coordinators = new HashMap<>();
        this.replicas = new HashMap<>();
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
                coordinator1ID = metadata.get(offset1).getID();
                coordinator2ID = metadata.get(offset2).getID();

                coordinators.put(coordinator1ID, new Coordinator(coordinator1ID, heartbeatPeriod));
                coordinators.put(coordinator2ID, new Coordinator(coordinator2ID, heartbeatPeriod));

                // spawnTimeoutThread(coordinator); // TODO: Start heartbeats detection??

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
                int offset1 = (i+1)%(metadata.size());
                int offset2 = (i+2)%(metadata.size());
                replica1ID = metadata.get(offset1).getID();
                replica2ID = metadata.get(offset2).getID();

                replicas.put(replica1ID, new Replica()); // TODO: Create proper Replica objects
                replicas.put(replica2ID, new Replica());

                // spawnTimeoutThread(coordinator); // TODO: Start heartbeats detection??

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

    private void spawnTimeoutThread(Coordinator coordinator) {
        timeoutThreadpool.submit(new TimeoutWatch(coordinator));
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

    public synchronized void deregisterCoordinator(String replicaID) {
        // TODO: To be used in TimeoutException, when it occurs
        coordinators.remove(replicaID);
    }


    public void heartbeat(String sourceIP, int replicaNumber) {
        coordinators.get(replicaNumber).heartbeat(new Date());
    }

    private class TimeoutWatch implements Runnable {
        Coordinator coordinator;
        public TimeoutWatch(Coordinator coordinator) {
            this.coordinator = coordinator;
        }
        @Override
        public void run() {
            // TODO: Wait for period
            // TODO: then check the new currentTimestamp of the coordinator
            // TODO: if old, then somehow raise trigger the timeout exception
//            if (coordinator.timestampDiffExceededPeriod()) {
//                throw new TimeoutException();
//            }

//            boolean sleep = true;
//            while (sleep) {
//                try {
//                    sleep = false;
//                    Thread.sleep(2000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                    Check also current timestamp
//                    // Restart sleep
//                    sleep = true;
//                }
//            }
            // It is dead
        }
    }

    public void cleanup() {
        // TODO: To be called when we receive changes in the ring with new replicas and new coordinators
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
