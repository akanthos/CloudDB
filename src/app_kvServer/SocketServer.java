package app_kvServer;

import app_kvServer.replication.Coordinator;
import app_kvServer.replication.ReplicationHandler;
import app_kvServer.dataStorage.KVCache;
import common.ServerInfo;
import common.messages.*;
import common.utils.KVRange;
import helpers.StorageException;
import org.apache.log4j.Logger;
import org.apache.log4j.lf5.util.StreamUtils;

import java.io.IOException;
import java.net.*;
import java.util.*;

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
    final long heartbeatPeriod = 2000; // In milliseconds
//    private CopyOnWriteArraySet<ServerActionListener> runnableListeners;
    private static Logger logger = Logger.getLogger(SocketServer.class);
    private boolean ECSRegistered;
    private final Map<String, ArrayList<ClientSubscription>> subscriptions;


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
        subscriptions = new HashMap<>();
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
     * Registers ECSInterface info
     * @param inetAddress ECSInterface IP address
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
        this.subscriptions.clear();
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
        HashMap<String, ArrayList<ClientSubscription>> subscribersToSend = new HashMap<>();
        for (KVPair pair : pairsToSend) {
            // Delete it from our storage
            kvCache.put(pair.getKey(), "null");
            // Get the relevant subscribers
            ArrayList<ClientSubscription> allSubscribersForKey = subscriptions.get(pair.getKey());
            if (allSubscribersForKey != null) {
                subscribersToSend.put(pair.getKey(), allSubscribersForKey);
            }
            subscriptions.remove(pair.getKey());
        }
        return messenger.sendToServer(pairsToSend, subscribersToSend, server);
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
    public synchronized KVServerMessageImpl insertNewDataToCache(List<KVPair> kvPairs, Map<String, ArrayList<ClientSubscription>> subscribers) {
        for (KVPair kv : kvPairs) {
            KVMessageImpl response = kvCache.put(kv.getKey(), kv.getValue());
            if (response.getStatus().equals(KVMessage.StatusType.PUT_ERROR)) {
                return new KVServerMessageImpl(KVServerMessage.StatusType.MOVE_DATA_FAILURE);
            }
        }
        for (String key : subscribers.keySet()) {
            subscriptions.put(key, subscribers.get(key));
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
        logger.info("DONE setting metadata");
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

    /**
     * Inserts new data into the replicated data file
     * Used by the REPLICATE server command
     * @param kvPairs the key value pairs to be inserted to the replicated data
     * @return the Server message response
     */
    public KVServerMessageImpl newReplicatedData(List<KVPair> kvPairs) {
        boolean status = replicationHandler.insertReplicatedData(kvPairs);
        return status ? new KVServerMessageImpl(KVServerMessage.StatusType.REPLICATE_SUCCESS)
                : new KVServerMessageImpl(KVServerMessage.StatusType.REPLICATE_FAILURE) ;
    }

    /**
     * Inserts new data into the replicated data file
     * Used by the GOSSIP server command
     * @param kvPairs the key value pairs to be inserted to the replicated data
     * @return the Server message response
     */
    public KVServerMessageImpl updateReplicatedData(List<KVPair> kvPairs) {
        logger.info(getInfo().getID() + " : Got gossip!! ::: " + kvPairs.get(0).getKey() +
                                        " , " + kvPairs.get(0).getValue());
        boolean status = replicationHandler.insertReplicatedData(kvPairs);
        return status ? new KVServerMessageImpl(KVServerMessage.StatusType.GOSSIP_SUCCESS)
                : new KVServerMessageImpl(KVServerMessage.StatusType.GOSSIP_FAILURE) ;
    }

    /**
     * Removes replicated data belonging to a specific range
     * Used by the REMOVE_DATA admin command
     * @param range the range into which the keys to be deleted should belong
     * @return the admin message response
     */
    public KVAdminMessageImpl removeReplicatedData(KVRange range) {
        KVMessageImpl response = replicationHandler.removeRange(range);
        if (response.getStatus().equals(KVMessage.StatusType.DELETE_ERROR)) {
            logger.info(info.getID() + " : Replicated range removal failed !!!");
            return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_FAILED);
        }
        logger.info(info.getID() + " : Replicated range removal SUCCESSFUL !!!");
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_SUCCESS);
    }

    /**
     * Restores data from the replicated to the main (kvcache) data
     * Used by the RESTORE_DATA admin command
     * @param range
     * @return the server message response
     */
    public KVAdminMessageImpl restoreData(KVRange range) {
        // Get data to restore from the replicated data
        List<KVPair> pairsToRestore = replicationHandler.getData(range);
        // Delete this range from the replicated data
        KVMessageImpl response = replicationHandler.removeRange(range);
        if (response.getStatus().equals(KVMessage.StatusType.DELETE_ERROR)) {
            logger.info(getInfo().getID() + " : Delete error on restoring data");
            return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_FAILED);
        }
        // Insert the restored data to the cache
        KVServerMessageImpl response2 = this.insertNewDataToCache(pairsToRestore, new HashMap<String, ArrayList<ClientSubscription>>());
        if (response2.getStatus().equals(KVServerMessage.StatusType.MOVE_DATA_FAILURE)) {
            logger.info(getInfo().getID() + " : Insertion error on restoring data");
            return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_FAILED);
        }
        logger.info(getInfo().getID() + " : Successfully restored data");
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_SUCCESS);
    }

    /**
     * Called when a HEARTBEAT server mesasge arrives from one of our replicas
     * @param replicaID the string representing the replica (IP:port)
     * @param timeOfSendingMessage datetime of the heartbeat
     * @return the server message response
     */
    public KVServerMessageImpl heartbeatReceived(String replicaID, Date timeOfSendingMessage) {
        return replicationHandler.heartbeatReceived(replicaID);
    }


    /**
     * Reports a failed coordinator to the ECS
     * @param coordinator the coordinator that was detected to have failed
     */
    public void reportFailureToECS(Coordinator coordinator) {
        messenger.reportFailureToECS(coordinator.getInfo(), ecsInfo);
    }

    /**
     * Called periodically by the HeartbeatSender runnable class.
     * Used to ask for a heartbeat from a server's coordinator
     * @param coordinator the coordinator to be asked for a heartbeat
     */
    public void askHeartbeatFromServer(Coordinator coordinator) {
        try {
            messenger.askHeartbeatFromServer(coordinator.getInfo());
        } catch (SocketTimeoutException e) {
            reportFailureToECS(coordinator);
            replicationHandler.coordinatorFailed(coordinator);
        }
    }

    /**
     * Called during a PUT client command to update the respective key-value
     * to one of our replicas.
     * @param replicaInfo the replica to be updated
     * @param list the key-values that changed (usually only one)
     * @return a status boolean to indicate if the value has been updated to the replica
     */
    public boolean gossipToReplica(ServerInfo replicaInfo, ArrayList<KVPair> list) {
        return messenger.gossipToReplica(replicaInfo, list);
    }


    public KVMessageImpl subscribeUser(String key, ClientSubscription clientSubscription) {
        synchronized (subscriptions) {
            if (!subscriptions.containsKey(key)) {
                subscriptions.put(key, new ArrayList<ClientSubscription>());
            }
            ArrayList<ClientSubscription> users = subscriptions.get(key);
            boolean found = false;
            for (ClientSubscription user : users) {
                if (user.getAddress().equals(clientSubscription.getAddress())) {
                    found = true;
                    user.getInterests().addAll(clientSubscription.getInterests());
                    break;
                }
            }
            if (!found) {
                users.add(clientSubscription);
            }

        }
        return new KVMessageImpl(KVMessage.StatusType.SUBSCRIBE_SUCCESS);

    }

    public KVMessageImpl unsubscribeUser(String key, ClientSubscription clientSubscription) {
        synchronized (subscriptions) {
            if (!subscriptions.containsKey(key)) {
                return new KVMessageImpl(KVMessage.StatusType.UNSUBSCRIBE_SUCCESS);
            }
            ArrayList<ClientSubscription> users = subscriptions.get(key);
            int client = -1;
            for (ClientSubscription user : users) {
                if (user.getAddress().equals(clientSubscription.getAddress())) {
                    client = users.indexOf(user);
                    user.getInterests().removeAll(clientSubscription.getInterests());
                    break;
                }
            }
            if (client != -1 && users.get(client).getInterests().isEmpty()) {
                users.remove(client);
            }
        }
        return new KVMessageImpl(KVMessage.StatusType.UNSUBSCRIBE_SUCCESS);
    }

    public ArrayList<String> getSubscribersForKey(String key, ClientSubscription.Interest interest) {
        ArrayList<String> clients = new ArrayList<>();
        synchronized (subscriptions) {
            logger.debug(info.getID() + " : Getting subscriptions for key: " + key);
            ArrayList<ClientSubscription> cs = subscriptions.get(key);
            if (cs != null) {
                logger.debug(info.getID() + " : Size of subscriptions for key: " + cs.size());
                for (ClientSubscription c : cs) {
                    logger.debug(info.getID() + " : Iterating cs...");
                    if (c.getInterests().contains(interest)) {
                        clients.add(c.getAddress());
                    }
                }
                logger.debug(info.getID() + " : Size of clients with interest for key: " + clients.size());
            }
            else {
                logger.debug(info.getID() + " : No such key in subscriptions map ");
            }
        }
        return clients;
    }

    public void deleteSubscribersForKey(String key) {
        synchronized (subscriptions) {
            subscriptions.remove(key);
            logger.debug(info.getID() + " : Removing subscriptions for key: " + key);
        }
    }
}