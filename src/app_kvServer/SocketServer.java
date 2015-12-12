package app_kvServer;

import common.ServerInfo;
import common.messages.*;
import common.utils.KVRange;
import helpers.StorageException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Class handling all TCP  connections
 * arriving on a given (host, port).
 * The connection handling lives in a KVRequestHandler
 */
public class SocketServer {
    private ServerInfo info;
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


    /**
     * Constructor of Socket server
     * @param info
     */
    public SocketServer(ServerInfo info) {
        this.info = info;
        this.state = new ServerState(
                /*init*/ false,
                /*open*/ false,
                /*writeLock*/ false,
                /*stop*/ true
        );
        messenger = new Messenger();
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
    public void addHandler(ConnectionHandler handler) { this.handler = handler; }

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
            return;
        }
    }

    /********************************************************************/
    /*                      Administrative Commands                     */
    /********************************************************************/
    /**
     * Initializes the server
     * @param metadata metadata for initialization
     * @param cacheSize cache size for initialization
     * @param displacementStrategy displacement strategy for initialization
     * @return a status message
     */
    public synchronized KVAdminMessageImpl initKVServer(List<ServerInfo> metadata, Integer cacheSize, String displacementStrategy){
        try {
            this.kvCache = new KVCache(cacheSize, displacementStrategy, info);
        } catch (StorageException e) {
            logger.error("Cannot create KVCache", e);
            return new KVAdminMessageImpl(KVAdminMessage.StatusType.GENERAL_ERROR);
        }
        setMetadata(metadata);
        state.setInitialized(true);
        info.setLaunched(true);
        try {
            replicationHandler = new ReplicationHandler(metadata, info.getServerRange(), heartbeatPeriod);
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
//        logger.info("My Address is: " + this.info.getAddress());
//        logger.info("My Port is: " + this.info.getServerPort());
//        logger.info("My Range is: " + this.info.getFromIndex() + ":" + this.info.getToIndex());
//        logger.info("Move data called");
        ArrayList<KVPair> pairsToSend = kvCache.getPairsInRange(range);
        for (KVPair pair : pairsToSend) {
            kvCache.put(pair.getKey(), "null");
        }
        return messenger.sendToServer(pairsToSend, server);
    }

    /**
     * Update the server's metadata
     * @param metadata new metadata to adopt
     * @return a status message
     */
    public synchronized KVAdminMessageImpl update(List<ServerInfo> metadata) {
        setMetadata(metadata);
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
//        logger.info("My Address is: " + this.info.getAddress());
//        logger.info("My Port is: " + this.info.getServerPort());
//        logger.info("My Range is: " + this.info.getFromIndex() + ":" + this.info.getToIndex());
//        logger.info("Inserting new data");
        for (KVPair kv : kvPairs) {
            kvCache.put(kv.getKey(), kv.getValue());
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


    /**
     * Clears the cache
     */
    public void cleanUp() {
        this.kvCache.cleanUp();
    }

    public KVServerMessageImpl newReplica(String sourceIP, int replicaNumber, List<KVPair> kvPairs) {
        // TODO: ???
//        replicationHandler.registerCoordinator(replicaNumber, sourceIP, kvPairs, heartbeatPeriod);
        return null; // TODO: Send appropriate response?? New ServerMessage status??
    }

    public void heartbeatReceived(String sourceIP, int replicaNumber) {
        replicationHandler.heartbeat(sourceIP, replicaNumber);
    }

}