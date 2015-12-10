package app_kvServer;

import common.messages.KVPair;
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
    private HashMap<Integer, KVPersistenceEngine> replicatedData;
    private ExecutorService timeoutThreadpool;
    private static Logger logger = Logger.getLogger(SocketServer.class);

    public ReplicationHandler() {
        coordinators = new HashMap<>();
        replicatedData = new HashMap<>();
        timeoutThreadpool = Executors.newCachedThreadPool();
    }

    public synchronized void registerCoordinator(int replicaNumber,
                                                 String sourceIP,
                                                 List<KVPair> kvPairs, long heartbeatPeriod) {
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
        }
    }
}
