package app_kvServer;

import app_kvServer.replication.Coordinator;
import app_kvServer.replication.Replica;
import app_kvServer.replication.ReplicationHandler;
import app_kvServer.dataStorage.KVCache;
import common.ServerInfo;
import common.messages.*;
import common.utils.KVRange;
import helpers.StorageException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * Class handling all TCP  connections
 * arriving on a given (host, port).
 * The connection handling lives in a KVRequestHandler
 */
public class SocketServer {
    private ServerInfo info;
    private ServerInfo ecsInfo;
    private volatile ServerState state;
    private ConnectionHandler handler;
    private KVCache kvCache;
    private ServerSocket server;
    private int numOfClients;
    private List<ServerInfo> metadata;
    private ReplicationHandler replicationHandler;
    Messenger messenger;
    final long heartbeatPeriod = 5000; // In milliseconds
//    private CopyOnWriteArraySet<ServerActionListener> runnableListeners;
    private static Logger logger = Logger.getLogger(SocketServer.class);
    private boolean ECSRegistered;


    /**
     * Constructor of Socket server
     * @param info
     */
    public SocketServer(ServerInfo info) {
        this.info = info;
        this.ecsInfo = new ServerInfo();
        ecsInfo.setServerPort(60036);
        this.ECSRegistered = false;
        this.state = new ServerState(
                /*init*/ false,
                /*open*/ false,
                /*writeLock*/ false,
                /*stop*/ true
        );
        messenger = new Messenger(this);
//        this.runnableListeners = new CopyOnWriteArraySet<>();//Collections.synchronizedList(new ArrayList<>());
    }

    /**
     * Creation of unbound server socket
     * @throws IOException
     */
    public void connect() throws IOException {
        if (state.isOpen()) return;
        server = new ServerSocket();
        server.bind(new InetSocketAddress(info.getAddress(), info.getServerPort()));
        state.setIsOpen(true);
    }

    /**
     * While the servers is open, accept requests and service them asynchronously.
     * @throws IOException if there is a network error (for instance if the socket is inadvertently closed)
     */
    public void run() throws IOException {
        if (!state.isOpen()) {
            throw new IOException();
        }
        while (state.isOpen()) {
            numOfClients++;
            handler.handle(server.accept(), numOfClients);
        }
    }

    /**
     * Add the connection handler for the current socket server
     * @param handler is logic for servicing a network connection
     */
    public void addConnectionHandler(ConnectionHandler handler) { this.handler = handler; }

    /**
     * Stop the ServerSocket
     */
    public void stop() {
        state.setIsOpen(false);
        this.closeSocket();
    }

    /**
     * close Server Socket
     */
    private void closeSocket() {
        try {
            server.close();
            System.out.println("socket closed");
        } catch (IOException e) {
        }
    }

    /********************************************************************/
    /*                      Administrative Commands                     */
    /********************************************************************/
    /**
     * Registers ECS info
     * @param inetAddress ECS IP address
     */
    public void registerECS(InetAddress inetAddress) {
        if (!ECSRegistered) {
            ecsInfo.setAddress(inetAddress.getHostAddress());
            ECSRegistered = true;
        }
    }
    /**
     * Initializes the server
     * @param metadata metadata for initialization
     * @param cacheSize cache size for initialization
     * @param displacementStrategy displacement strategy for initialization
     * @return a status message
     */
    public synchronized KVAdminMessageImpl initKVServer(List<ServerInfo> metadata, Integer cacheSize, String displacementStrategy){
        try {
            this.kvCache = new KVCache(cacheSize, displacementStrategy, this);
        } catch (StorageException e) {
            logger.error("Cannot create KVCache", e);
            return new KVAdminMessageImpl(KVAdminMessage.StatusType.GENERAL_ERROR);
        }
        setMetadata(metadata);
        state.setInitialized(true);
        info.setLaunched(true);
        try {
            replicationHandler = new ReplicationHandler(this, metadata, info.getServerRange(), heartbeatPeriod);
        } catch (StorageException e) {
            return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_FAILED);
        }
//        logger.info("Just initialized myself!!!");
//        logger.info("My Address is: " + this.info.getAddress());
//        logger.info("My Port is: " + this.info.getServerPort());
//        logger.info("My Range is: " + this.info.getFromIndex() + ":" + this.info.getToIndex());
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_SUCCESS);
    }


    /**
     * Starts the server
     * @return a status message
     */
    public synchronized KVAdminMessageImpl startServing() {
        state.setStopped(false);
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_SUCCESS);
    }

    /**
     * Stops the server
     * @return a status message
     */
    public synchronized KVAdminMessageImpl stopServing() {
        state.setStopped(true);
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_SUCCESS);
    }

    /**
     * Locks the server for writes
     * @return a status message
     */
    public synchronized KVAdminMessageImpl writeLock() {
        state.setWriteLock(true);
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_SUCCESS);
    }

    /**
     * Unlocks the sever for writes
     * @return a status message
     */
    public synchronized KVAdminMessageImpl writeUnlock() {
        state.setWriteLock(false);
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_SUCCESS);
    }

    /**
     * Shuts down the server
     * @return a status message
     */
    public synchronized KVAdminMessageImpl shutDown() {
        state.setIsOpen(false);
        this.closeSocket();
        this.handler.shutDown();
//        logger.info("SHUTTING DOWN: ");
//        logger.info("My Address is: " + this.info.getAddress());
//        logger.info("My Port is: " + this.info.getServerPort());
//        logger.info("My Range is: " + this.info.getFromIndex() + ":" + this.info.getToIndex());
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_SUCCESS);
    }

    /**
     * Moves the key-value pairs that belong to a range to another server
     * @param range range that the moved data should belong to
     * @param server server to move the data to
     * @return a status message
     */
    public synchronized KVAdminMessageImpl moveData(KVRange range, ServerInfo server) {
        ArrayList<KVPair> pairsToSend = kvCache.getPairsInRange(range);
        for (KVPair pair : pairsToSend) {
            kvCache.put(pair.getKey(), "null");
        }
        return messenger.sendToServer(pairsToSend, server);
    }

    /**
     * Replicates the key-value pairs that belong to a range to another server
     * @param range range that the replicated data should belong to
     * @param server server to replicate the data to
     * @return a status message
     */
    public KVAdminMessageImpl replicateData(KVRange range, ServerInfo server) {
        ArrayList<KVPair> pairsToSend = kvCache.getPairsInRange(range);
        return messenger.replicateToServer(pairsToSend, server);
    }

    /**
     * Update the server's metadata
     * @param metadata new metadata to adopt
     * @return a status message
     */
    public synchronized KVAdminMessageImpl update(List<ServerInfo> metadata) {
        setMetadata(metadata);
        replicationHandler.updateMetadata(metadata, this.info.getServerRange());
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_SUCCESS);
    }

    /********************************************************************/
    /*                          Server Requests                         */
    /********************************************************************/
    /**
     * Inserts new key-value pairs to cache
     * @param kvPairs
     * @return
     */
    public synchronized KVServerMessageImpl insertNewDataToCache(List<KVPair> kvPairs) {
        for (KVPair kv : kvPairs) {
            KVMessageImpl response = kvCache.put(kv.getKey(), kv.getValue());
            if (response.getStatus().equals(KVMessage.StatusType.PUT_ERROR)) {
                return new KVServerMessageImpl(KVServerMessage.StatusType.MOVE_DATA_FAILURE);
            }
        }
        return new KVServerMessageImpl(KVServerMessage.StatusType.MOVE_DATA_SUCCESS);
        // If it fails, respond with MOVE_FAILURE ???
    }


    /********************************************************************/
    /*                          State Getters                           */
    /********************************************************************/
    /**
     * State getter
     * @return the server's current state
     */
    public synchronized ServerState getState() {
        return state;
    }

    /**
     * Initialization state getter
     * @return the server's current initialization state
     */
    public boolean isInitialized() {
        return state.isInitialized();
    }

    /**
     * Open/closed getter
     * @return the server's current open/closed state
     */
    public boolean isOpen() { return state.isOpen(); }

    /**
     * Write_lock getter
     * @return the server's current write_lock state
     */
    public boolean isWriteLocked() {
        return state.isWriteLock();
    }

    /**
     * Stopped getter
     * @return the server's current stopped state
     */
    public boolean isStopped() {
        return state.isStopped();
    }

    /********************************************************************/
    /*                       State Setters                              */
    /********************************************************************/
    /**
     * State setter
     * @param state The server state to set
     */
    public synchronized void setState(ServerState state) { this.state = state; }

    /**
     * Initialized setter
     * @param initialized The initialized state to set
     */
    public void setInitialized(boolean initialized) { state.setInitialized(initialized); }

    /**
     * Open/closed setter
     * @param open The open/closed state to set
     */
    public void setIsOpen(boolean open) { state.setIsOpen(open); }

    /**
     * Write_lock setter
     * @param wl The write_lock state to set
     */
    public void setWriteLock(boolean wl) { state.setWriteLock(wl); }

    /**
     * Stopped setter
     * @param stopped The stopped state to set
     */
    public void setStopped(boolean stopped) { state.setStopped(stopped); }

    /********************************************************************/
    /*                     Metadata Setters/Getters                     */
    /********************************************************************/
    /**
     * Metadata setter
     * @param metadata the metadata to set
     */
    public void setMetadata(List<ServerInfo> metadata) {
        this.metadata = metadata;
        for (ServerInfo info : metadata) {
            logger.info("Server " + info.getAddress() + ":" + info.getServerPort() +
                    " range : " + info.getFromIndex() + ":" + info.getToIndex());
            if (info.getAddress().equals(this.info.getAddress()) && info.getServerPort().equals(this.info.getServerPort())) {
                this.info.setServerRange(info.getServerRange());
                logger.info("Update my range to: " + this.info.getFromIndex() +":"+ this.info.getToIndex());
            }
        }
    }

    /**
     * Metadata getter
     * @return the current server's metadata
     */
    public synchronized List<ServerInfo> getMetadata() { return metadata; }

    /**
     * Server info getter
     * @return the current server's info
     */
    public ServerInfo getInfo() {
        return this.info;
    }

    /**
     * KVCache getter
     * @return the server's kvcache
     */
    public KVCache getKvCache() {
        return kvCache;
    }

    /**
     * KVCache setter
     * @param kvCache cache to set
     */
    public void setKvCache(KVCache kvCache) {
        this.kvCache = kvCache;
    }

    public ReplicationHandler getReplicationHandler() {
        return this.replicationHandler;
    }

    /**
     * Clears the cache
     */
    public void cleanUp() {
        this.kvCache.cleanUp();
    }

    public KVServerMessageImpl newReplicatedData(List<KVPair> kvPairs) {
        boolean status = replicationHandler.insertReplicatedData(kvPairs);
        return status ? new KVServerMessageImpl(KVServerMessage.StatusType.REPLICATE_SUCCESS)
                : new KVServerMessageImpl(KVServerMessage.StatusType.REPLICATE_FAILURE) ;
    }
    public KVServerMessageImpl updateReplicatedData(List<KVPair> kvPairs) {
        boolean status = replicationHandler.insertReplicatedData(kvPairs);
        return status ? new KVServerMessageImpl(KVServerMessage.StatusType.GOSSIP_SUCCESS)
                : new KVServerMessageImpl(KVServerMessage.StatusType.GOSSIP_FAILURE) ;
    }

    public KVAdminMessageImpl removeReplicatedData(KVRange range, ServerInfo serverInfo) {
        KVMessageImpl response = replicationHandler.removeRange(range);
        if (response.getStatus().equals(KVMessage.StatusType.DELETE_ERROR)) {
            return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_FAILED);
        }
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_SUCCESS);
    }

    public KVAdminMessageImpl restoreData(KVRange range, ServerInfo serverInfo) {
        // Get data to restore from the replicated data
        List<KVPair> pairsToRestore = replicationHandler.getData(range);
        // Delete this range from the replicated data
        KVMessageImpl response = replicationHandler.removeRange(range);
        if (response.getStatus().equals(KVMessage.StatusType.DELETE_ERROR)) {
            return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_FAILED);
        }
        // Insert the restored data to the cache
        KVServerMessageImpl response2 = this.insertNewDataToCache(pairsToRestore);
        if (response2.getStatus().equals(KVServerMessage.StatusType.MOVE_DATA_FAILURE)) {
            return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_FAILED);
        }
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_SUCCESS);
    }

    public void heartbeatReceived(String replicaID, Date timeOfSendingMessage) {
        replicationHandler.heartbeatReceived(replicaID);
    }


    public void reportFailureToECS(Coordinator coordinator) {
        messenger.reportFailureToECS(coordinator.getInfo(), ecsInfo);
    }


    public void sendHeartbeatToServer(Coordinator coordinator) {
        try {
            messenger.sendHeartBeatToServer(coordinator.getInfo());
        } catch (SocketTimeoutException e) {
            reportFailureToECS(coordinator);
            replicationHandler.coordinatorFailed(coordinator);
        }
    }
    public void answerHeartbeat(Replica replica) {
        messenger.respondToHeartbeatRequest(replica.getInfo());
    }


    public boolean gossipToReplica(ServerInfo replicaInfo, ArrayList<KVPair> list) {
        return messenger.gossipToReplica(replicaInfo, list);
    }


}