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

    private HashMap<Integer, Coordinator> coordinators;
    private HashMap<Integer, Replica> replicas;
    private HashMap<Integer, KVPersistenceEngine> replicatedData;
    private ExecutorService timeoutThreadpool;
    private static Logger logger = Logger.getLogger(SocketServer.class);

    public ReplicationHandler(List<ServerInfo> metadata, KVRange range) {
        coordinators = new HashMap<>();
        replicas = new HashMap<>();
        findAndRegisterReplicas(metadata, range);
        replicatedData = new HashMap<>();
        timeoutThreadpool = Executors.newCachedThreadPool();
    }

    private void findAndRegisterReplicas(List<ServerInfo> metadata, KVRange range) {
        ServerInfo replica1=null, replica2=null;
        for (int i=0; i<metadata.size(); i++) {
            ServerInfo info = metadata.get(i);
            if (info.getServerRange().equals(range)) {
                int ii1 = (i+1)%(metadata.size());
                int ii2 = (i+2)%(metadata.size());
                replica1 = metadata.get(ii1);
                replica2 = metadata.get(ii2);
                logger.info("Found my place on the ring");
                replicas.put(ii1, new Replica()); // TODO: Create proper Replica objects
                replicas.put(ii2, new Replica());
            }
        }
    }

    public synchronized void registerCoordinator(int replicaNumber,
                                                 String sourceIP,
                                                 List<KVPair> kvPairs, long heartbeatPeriod) {
        if (!coordinators.containsKey(replicaNumber)) {
            Coordinator coordinator = new Coordinator(replicaNumber, sourceIP, heartbeatPeriod);
            coordinators.put(coordinator.getCoordinatorNumber(), coordinator);
            try {
                KVPersistenceEngine data = new KVPersistenceEngine(replicaNumber);
                bulkInsert(data, kvPairs);
                replicatedData.put(replicaNumber, data);
                spawnTimeoutThread(coordinator);
            } catch (StorageException e) {
                logger.error("Cannot initialize persistence file for coordinator: " + replicaNumber);
            }
        }
        else {
            bulkInsert(replicatedData.get(replicaNumber), kvPairs);
        }
    }

    private void spawnTimeoutThread(Coordinator coordinator) {
        timeoutThreadpool.submit(new TimeoutWatch(coordinator));
    }

    private void bulkInsert(KVPersistenceEngine kvPersistenceEngine, List<KVPair> kvPairs) {
        for (KVPair pair : kvPairs) {
            kvPersistenceEngine.put(pair.getKey(), pair.getValue());
        }
    }

    public synchronized void deregisterCoordinator(int replicaNumber) {
        // TODO: To be used in TimeoutException, when it occurs
        coordinators.remove(replicaNumber);
        replicatedData.get(replicaNumber).cleanUp();
        replicatedData.remove(replicaNumber);
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
        for (int replicaNumber : replicatedData.keySet()) {
            replicatedData.get(replicaNumber).cleanUp();
        }
        replicatedData.clear();
        /* Shutdown timers */
        timeoutThreadpool.shutdownNow();
    }


}
