package app_kvServer.replication;

import app_kvServer.dataStorage.KVPersistenceEngine;
import app_kvServer.SocketServer;
import common.ServerInfo;
import common.messages.*;
import common.utils.KVRange;
import common.utils.Utilities;
import helpers.StorageException;
import org.apache.log4j.Logger;

import java.util.*;
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
        findCoordinatorsAndReplicas(metadata, range);
        logger.info("Here");
    }

    public void updateMetadata(List<ServerInfo> metadata, KVRange range) {
        cleanupNoData();
        findCoordinatorsAndReplicas(metadata, range);
    }

    private synchronized void findCoordinatorsAndReplicas(List<ServerInfo> metadata, KVRange range) {
        this.timeoutThreadpool = Executors.newCachedThreadPool();
        this.coordinators = new HashMap<>();
        this.replicas = new HashMap<>();
        logger.info("Before finding replicas and coordinators");
        findAndRegisterReplicas(metadata, range);
        logger.info("After finding replicas");
        findAndRegisterCoordinators(metadata, range);
        logger.info("After finding coordinators");
        logger.info(server.getInfo().getID() + ": Just updated me metadata...");
        logger.info(server.getInfo().getID() + ": My replicas are:");
        for (Replica r : replicas.values()) {
            logger.info(r.getReplicaID());
        }
        logger.info(server.getInfo().getID() + ": My coordinators are:");
        for (Coordinator c : coordinators.values()) {
            logger.info(c.getCoordinatorID());
        }
    }

    private void findAndRegisterCoordinators(List<ServerInfo> metadata, KVRange range) {
        List<ServerInfo> coords = Utilities.getCoordinators(metadata, server.getInfo());
        logger.info(server.getInfo().getID() + ": Found my coordinators");
        for (ServerInfo coordInfo: coords) {
            logger.info(server.getInfo().getID() + ": COORDINATOR : " + coordInfo.getID());
            String coordID = coordInfo.getID();
            coordinators.put(coordID, new Coordinator(coordID, coordInfo, heartbeatPeriod, this));
        }
    }
    private void findAndRegisterReplicas(List<ServerInfo> metadata, KVRange range) {
        List<ServerInfo> repls = Utilities.getReplicas(metadata, server.getInfo());
        logger.info(server.getInfo().getID() + ": Found my replicas");
        for (ServerInfo replInfo: repls) {
            logger.info(server.getInfo().getID() + ": REPLICA : " + replInfo.getID());
            String replID = replInfo.getID();
            replicas.put(replID, new Replica(this, replID, replInfo));
        }
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
        replicas.remove(replicaID);
    }

    /*****************************************************************/
    /*                   Replicated Data Manipulation                */
    /*****************************************************************/

    ///////////////////////////////////////////
    //            INCOMING CHANGES           //
    ///////////////////////////////////////////

    // Used from REPLICATE and GOSSIP message
    public boolean insertReplicatedData(List<KVPair> kvPairs) {
        synchronized (replicatedData) {
            for (KVPair pair : kvPairs) {
                KVMessageImpl status = replicatedData.put(pair.getKey(), pair.getValue());
                if ( ! (status.getStatus().equals(KVMessage.StatusType.PUT_SUCCESS) ||
                        status.getStatus().equals(KVMessage.StatusType.PUT_UPDATE)) )
                    return false;
            }
        }
        return true;
    }

    // Used from the REMOVE_DATA message
    public KVMessageImpl removeRange(KVRange range) {
        synchronized (replicatedData) {
            return replicatedData.remove(range);
        }
    }

    ///////////////////////////////////////////
    //           OUTGOING CHANGES            //
    ///////////////////////////////////////////

    public KVMessageImpl get(String key) {
        synchronized (replicatedData) {
            return replicatedData.get(key);
        }
    }
    public List<KVPair> getData(KVRange range) {
        synchronized (replicatedData) {
            return replicatedData.get(range);
        }
    }


    public synchronized boolean gossipToReplicas(ArrayList<KVPair> list) {
        for (Replica replica : replicas.values()) {
            if (!server.gossipToReplica(replica.getInfo(), list))
                return false;
        }
        return true;
    }

    ///////////////////////////////////////////
    //                GENERAL                //
    ///////////////////////////////////////////

    public boolean isResponsibleForHash(KVMessage message) {
        for (Coordinator c : coordinators.values()) {
            if (c.getInfo().getServerRange().isIndexInRange(message.getHash()))
                return true;
        }
        return false;
    }

    /*****************************************************************/
    /*                  Heartbeat and Failure handling               */
    /*****************************************************************/
    public void sendHeartbeat(String coordinatorID) {
        server.sendHeartbeatToServer(coordinators.get(coordinatorID));
    }
    public KVServerMessageImpl heartbeatReceived(String coordinatorID) {
        logger.info(server.getInfo().getID() + " ---- Received heartbeat from: " + coordinatorID);
        logger.info("Coords: " + coordinators.size());
        logger.info("Replicas: " + replicas.size());
        return new KVServerMessageImpl(KVServerMessage.StatusType.HEARTBEAT_RESPONSE);
//        server.answerHeartbeat(coordinators.get(coordinatorID));
    }
    public synchronized void coordinatorFailed(Coordinator coordinator) {
        deregisterCoordinator(coordinator.getCoordinatorID());
    }

    /*****************************************************************/
    /*         Heartbeat and Timer and Data thread cleanup           */
    /*****************************************************************/
    private void shutdownHeartbeats() {
        for (Coordinator c : coordinators.values()) {
            c.stop();
        }
        /* Shutdown thread pool */
        timeoutThreadpool.shutdownNow();
    }

    public synchronized void cleanupNoData() {
        logger.info("Cleaning UP DATA!!!!");
        /* Shutdown timers and heartbeats*/
        shutdownHeartbeats();
        /* Remove coordinators */
        coordinators.clear();
        /* Remove replica information */
        replicas.clear();
    }

    public synchronized void shutdown() {
        /* Shutdown timers and heartbeats*/
        shutdownHeartbeats();
        /* Remove coordinators */
        coordinators.clear();
        /* Remove replica information */
        replicas.clear();
        /* Clean replicated data */
        replicatedData.cleanUp();
    }



}
