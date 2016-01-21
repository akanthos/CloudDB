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
 * Class that represents the replication logic into each server
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
    /**
     * Constructor of the replication handler
     * @param server the server that this replication handler belongs to
     * @param metadata the metadata of the server
     * @param range the key range of the server
     * @param heartbeatPeriod the heartbeat period to be used
     * @throws StorageException
     */
    public ReplicationHandler(SocketServer server, List<ServerInfo> metadata, KVRange range, long heartbeatPeriod) throws StorageException {
        this.server = server;
        this.heartbeatPeriod = heartbeatPeriod;
        this.replicatedData = new KVPersistenceEngine("_replica_"+server.getInfo().getServerPort());
        findCoordinatorsAndReplicas(metadata, range);
    }

    /**
     * Called when metadata in the server have been changed
     * @param metadata the new metadata
     * @param range the new server's range
     */
    public void updateMetadata(List<ServerInfo> metadata, KVRange range) {
        cleanupNoData();
        findCoordinatorsAndReplicas(metadata, range);
    }

    /**
     * Finds the new coordinators and the new replicas according to
     * the new metadata and the server's range that have been updated (or initialized)
     * Starts a new thread pool for the new heartbeat senders
     * @param metadata the new metadata
     * @param range the new server's key range
     */
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

    /**
     * Finds the coordinators according to the new metadata
     * @param metadata the metadata
     * @param range the server's key range
     */
    private void findAndRegisterCoordinators(List<ServerInfo> metadata, KVRange range) {
        List<ServerInfo> coords = Utilities.getCoordinators(metadata, server.getInfo());
        logger.info(server.getInfo().getID() + ": Found my coordinators");
        for (ServerInfo coordInfo: coords) {
            logger.info(server.getInfo().getID() + ": COORDINATOR : " + coordInfo.getID());
            String coordID = coordInfo.getID();
            coordinators.put(coordID, new Coordinator(coordID, coordInfo, heartbeatPeriod, this));
        }
    }

    /**
     * Finds the replicas according to the new metadata
     * @param metadata the metadata
     * @param range the server's key range
     */
    private void findAndRegisterReplicas(List<ServerInfo> metadata, KVRange range) {
        List<ServerInfo> repls = Utilities.getReplicas(metadata, server.getInfo());
        logger.info(server.getInfo().getID() + ": Found my replicas");
        for (ServerInfo replInfo: repls) {
            logger.info(server.getInfo().getID() + ": REPLICA : " + replInfo.getID());
            String replID = replInfo.getID();
            replicas.put(replID, new Replica(this, replID, replInfo));
        }
    }

    /**
     * Submits a new heartbeat sender to the thread pool
     * @param r the runnable that represents the heartbeat sender
     */
    public void submit(Runnable r) {
        this.timeoutThreadpool.submit(r);
    }

    /*****************************************************************/
    /*               Add/Remove Coordinators and Replicas            */
    /*****************************************************************/
    // TODO: Not needed if we find all again with update metadata ???

    /**
     * Deregisters a coordinator and
     * shuts down the respective heartbeat request sender
     * @param coordinatorID the coordinator ID to be deregistered (IP:port)
     */
    private synchronized void deregisterCoordinator(String coordinatorID) {
        coordinators.get(coordinatorID).stop();
        removeRange(coordinators.get(coordinatorID).getInfo().getServerRange());
        coordinators.remove(coordinatorID);
    }

    /**
     * Deregisters a replica
     * @param replicaID the replica ID to be deregistered (IP:port)
     */
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
                logger.info(server.getInfo().getID() + " : Inserting gossip pair ::: " + pair.getKey() +
                        " , " + pair.getValue());
                KVMessageImpl status = replicatedData.put(pair.getKey(), pair.getValue());
                if ( ! (status.getStatus().equals(KVMessage.StatusType.PUT_SUCCESS) ||
                        status.getStatus().equals(KVMessage.StatusType.PUT_UPDATE)) )
                    return false;
            }
        }
        logger.info(server.getInfo().getID() + " : All gossips inserted!");
        return true;
    }

    /**
     * Called when a REMOVE_DATA admin message has arrived
     * @param range the range to which the data to be removed should belong to
     * @return the server message response
     */
    public KVMessageImpl removeRange(KVRange range) {
        synchronized (replicatedData) {
            return replicatedData.remove(range);
        }
    }

    ///////////////////////////////////////////
    //           OUTGOING CHANGES            //
    ///////////////////////////////////////////

    /**
     * Gets a value for the given key from the replicated data
     * @param key the key of the GET request
     * @return the value corresponding to the given key
     */
    public KVMessageImpl get(String key) {
        logger.info(server.getInfo().getID() + " : Getting key from replicated data (" + key + ")");
        synchronized (replicatedData) {
            return replicatedData.get(key);
        }
    }

    /**
     * Gets all key-value pairs from the replicated data that belong
     * to the given key range
     * @param range the key of the GET request
     * @return the value corresponding to the given key
     */
    public List<KVPair> getData(KVRange range) {
        synchronized (replicatedData) {
            return replicatedData.get(range);
        }
    }

    /**
     * Gossips the new values for some keys to all the replicas
     * @param list the new key-value pairs
     * @return a status boolean for successful operation.
     *          If gossip to all replicas successful, returns true
     *          else returns false
     */
    public synchronized boolean gossipToReplicas(ArrayList<KVPair> list) {
        logger.info(server.getInfo().getID() + " : Sending gossip to replicas!");
        for (Replica replica : replicas.values()) {
            if (!server.gossipToReplica(replica.getInfo(), list))
                return false;
        }
        return true;
    }

    ///////////////////////////////////////////
    //                GENERAL                //
    ///////////////////////////////////////////

    /**
     * Checks if the replicated data contain the key in
     * the input GET request
     * @param message client message representing the GET request
     * @return True if key is in replicated data, else false
     */
    public boolean isResponsibleForHash(KVMessage message) {
        for (Coordinator c : coordinators.values()) {
            if (c.getInfo().getServerRange().isIndexInRange(message.getHash())) {
                logger.info(server.getInfo().getID() +
                        " : Key found in our coordinator : " + c.getCoordinatorID());
                return true;
            }
        }
        logger.info(server.getInfo().getID() +
                " : Key NOT found in my coordinators' ranges...");
        return false;
    }

    /*****************************************************************/
    /*                  Heartbeat and Failure handling               */
    /*****************************************************************/
    /**
     * Sends a heartbeat request to the given coordinator
     * @param coordinatorID the coordinator to ask the heartbeat from
     */
    public void sendHeartbeat(String coordinatorID) {
        server.askHeartbeatFromServer(coordinators.get(coordinatorID));
    }

    /**
     * Called when a heartbeat request has been received
     * @param replicaID the replica that sent the heartbeat request
     * @return the server message with the heartbeat response
     */
    public KVServerMessageImpl heartbeatReceived(String replicaID) {
//        logger.info(server.getInfo().getID() + " ---- Received heartbeat request from: " + replicaID);
//        logger.info("Coords: " + coordinators.size());
//        logger.info("Replicas: " + replicas.size());
        return new KVServerMessageImpl(KVServerMessage.StatusType.HEARTBEAT_RESPONSE);
    }

    /**
     * Called when a coordinator has failed. Reports failure to the ECS
     * @param coordinator the coordinator that was detected to have failed
     */
    public synchronized void coordinatorFailed(Coordinator coordinator) {
        deregisterCoordinator(coordinator.getCoordinatorID());
    }

    /*****************************************************************/
    /*         Heartbeat and Timer and Data thread cleanup           */
    /*****************************************************************/
    /**
     * Shuts down the active heartbeat senders for all the coordinators
     * Also shuts down the thread pool
     */
    private void shutdownHeartbeats() {
        for (Coordinator c : coordinators.values()) {
            c.stop();
        }
        /* Shutdown thread pool */
        timeoutThreadpool.shutdownNow();
    }

    /**
     * Clears the coordinators and the replicas
     * without cleaning the replicated data
     */
    public synchronized void cleanupNoData() {
        logger.info("Cleaning UP replicas and coordinators !!!!");
        /* Shutdown timers and heartbeats*/
        shutdownHeartbeats();
        /* Remove coordinators */
        coordinators.clear();
        /* Remove replica information */
        replicas.clear();
    }

    /**
     * Shuts down the replication handler
     * Cleans the replicas and coordinators representation
     * as well as the replicated data.
     */
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
