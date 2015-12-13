package app_kvServer;

import com.sun.corba.se.spi.activation.Server;
import common.ServerInfo;
import common.messages.KVPair;
import common.utils.KVRange;
import helpers.StorageException;
import org.apache.log4j.Logger;

import java.sql.Time;
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
    private ExecutorService timeoutThreadpool;

    private final KVPersistenceEngine replicatedData;
    private final SocketServer server;

    private final long heartbeatPeriod;
    private static final Logger logger = Logger.getLogger(ReplicationHandler.class);


    /*****************************************************************/
    /*                    Initialization and Update                  */
    /*****************************************************************/
    public ReplicationHandler(SocketServer server, List<ServerInfo> metadata, KVRange range, long heartbeatPeriod) throws StorageException {
        this.server = server;
        this.heartbeatPeriod = heartbeatPeriod;
        this.replicatedData = new KVPersistenceEngine("_replica");
        findCoordsAndReplicas(metadata, range);
    }

    public void updateMetadata(List<ServerInfo> metadata, KVRange range) {
        cleanupNoData();
        findCoordsAndReplicas(metadata, range);
    }

    private void findCoordsAndReplicas(List<ServerInfo> metadata, KVRange range) {
        this.timeoutThreadpool = Executors.newCachedThreadPool();
        this.coordinators = new HashMap<>();
        this.replicas = new HashMap<>();
        findAndRegisterReplicas(metadata, range);
        findAndRegisterCoordinators(metadata, range);
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

                coordinators.put(coordinator1ID, new Coordinator(coordinator1ID, coordinator1Info, heartbeatPeriod, this));
                coordinators.put(coordinator2ID, new Coordinator(coordinator2ID, coordinator2Info, heartbeatPeriod, this));

                logger.info(info.getID() + ": Found my coordinators");
                break;
            }
        }
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

                replicas.put(replica1ID, new Replica(this, replica1ID, replica1Info));
                replicas.put(replica2ID, new Replica(this, replica2ID, replica2Info));

                logger.info(info.getID() + ": Found my replicas");
                break;
            }
        }
    }
    private int getOffsetOnRing(int index, int size) {
        int modulo = index % size;
        return (modulo>0) ? modulo : (size+modulo);
    }
    public void submit(Runnable r) {
        this.timeoutThreadpool.submit(r);
    }

    /*****************************************************************/
    /*               Add/Remove Coordinators and Replicas            */
    /*****************************************************************/
    // TODO: Not needed if we find all again with update metadata ???
    private synchronized void deregisterCoordinator(String coordinatorID) {
        coordinators.get(coordinatorID).stop();
        removeRange(coordinators.get(coordinatorID).getInfo().getServerRange());
        coordinators.remove(coordinatorID);
    }
    private synchronized void deregisterReplica(String replicaID) {
        replicas.get(replicaID).stop();
        replicas.remove(replicaID);
    }

    /*****************************************************************/
    /*                   Replicated Data Manipulation                */
    /*****************************************************************/
    public void insertReplicatedData(String coordinatorID, ServerInfo coordinatorInfo, List<KVPair> kvPairs) {
        if (!coordinators.containsKey(coordinatorID)) {
            coordinators.put(coordinatorID, new Coordinator(coordinatorID, coordinatorInfo, heartbeatPeriod, this));
        }
        synchronized (replicatedData) {
            for (KVPair pair : kvPairs) {
                replicatedData.put(pair.getKey(), pair.getValue());
            }
        }
    }
    public void removeReplicatedData(String coordinatorID, List<KVPair> kvPairs) {
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


    /*****************************************************************/
    /*                  Heartbeat and Failure handling               */
    /*****************************************************************/
    public void heartbeat(String replicaID, Date timeOfSendingMessage) {
        coordinators.get(replicaID).heartbeat(timeOfSendingMessage);
    }
    public void sendHeartbeat(String replicaID) {
        server.sendHeartbeatToServer(replicas.get(replicaID));
    }
    public synchronized void coordinatorFailed(String coordinatorID) {
        server.reportFailureToECS(coordinators.get(coordinatorID));
        deregisterCoordinator(coordinatorID);
    }

    /*****************************************************************/
    /*         Heartbeat and Timer and Data thread cleanup           */
    /*****************************************************************/
    private void shutdownWatchesAndHeartbeats() {
        for (Coordinator c : coordinators.values()) {
            c.stop();
        }
        for (Replica r : replicas.values()) {
            r.stop();
        }
        /* Shutdown thread pool */
        timeoutThreadpool.shutdownNow();
    }

    public void cleanupNoData() {
        /* Shutdown timers and heartbeats*/
        shutdownWatchesAndHeartbeats();
        /* Remove coordinators */
        coordinators.clear();
        /* Remove replica information */
        replicas.clear();
    }

    public void cleanupAll() {
        /* Shutdown timers and heartbeats*/
        shutdownWatchesAndHeartbeats();
        /* Remove coordinators */
        coordinators.clear();
        /* Remove replica information */
        replicas.clear();
        /* Clean replicated data */
        replicatedData.cleanUp();
    }




}
