package app_kvServer;

import common.ServerInfo;
import common.messages.KVAdminMessage;
import common.messages.KVAdminMessageImpl;
import common.utils.KVMetadata;
import common.utils.KVRange;
import helpers.StorageException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;

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
    private CopyOnWriteArraySet<ServerActionListener> runnableListeners;
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
        this.runnableListeners = new CopyOnWriteArraySet<>();//Collections.synchronizedList(new ArrayList<>());
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
    public synchronized KVAdminMessageImpl initKVServer(List<ServerInfo> metadata, Integer cacheSize, String displacementStrategy){
        try {
            this.kvCache = new KVCache(cacheSize, displacementStrategy);
        } catch (StorageException e) {
            logger.error("Cannot create KVCache", e);
            return new KVAdminMessageImpl(KVAdminMessage.StatusType.GENERAL_ERROR);
        }
        setMetadata(metadata);
        state.setInitialized(true);
        info.setLaunched(true);
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.INIT_SUCCESS);
    }

    public synchronized KVAdminMessageImpl startServing() {
        state.setStopped(false);
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.START_SUCCESS);
    }
    public synchronized KVAdminMessageImpl stopServing() {
        state.setStopped(true);
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.STOP_SUCCESS);
    }
    public synchronized KVAdminMessageImpl writeLock() {
        state.setWriteLock(true);
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.LOCK_WRITE_SUCCESS);
    }
    public synchronized KVAdminMessageImpl writeUnlock() {
        state.setWriteLock(false);
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.UNLOCK_WRITE_SUCCESS);
    }
    public synchronized KVAdminMessageImpl shutDown() {
        state.setIsOpen(false);
        this.closeSocket();
        this.handler.shutDown();
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.SHUT_DOWN_SUCCESS);
    }

    public synchronized KVAdminMessageImpl moveData(KVRange range, ServerInfo server) {
        // TODO:
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.GENERAL_ERROR);
    }

    public synchronized KVAdminMessageImpl update(List<ServerInfo> metadata) {
        setMetadata(metadata);
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.UPDATE_SUCCESS);
    }
//    private void updateStateToListeners() {
//        for (ServerActionListener l : runnableListeners) {
//            // Maybe exclude myself
////            if (l != handler) {
//            l.updateState(this.state);
////            }
//        }
//    }
//    private void updateMetadataToListeners() {
//        for (ServerActionListener l : runnableListeners) {
//            // Maybe exclude myself
////            if (l != handler) {
//            l.updateMetadata(this.metadata);
////            }
//        }
//    }

    /********************************************************************/
    /*                       State Getters                              */
    /********************************************************************/
    public synchronized ServerState getState() {
        return state;
    }
    public boolean isInitialized() {
        return state.isInitialized();
    }
    public boolean isOpen() { return state.isOpen(); }
    public boolean isWriteLocked() {
        return state.isWriteLock();
    }
    public boolean isStopped() {
        return state.isStopped();
    }

    /********************************************************************/
    /*                       State Setters                              */
    /********************************************************************/
    public synchronized void setState(ServerState state) { this.state = state; }
    public void setInitialized(boolean initialized) { state.setInitialized(initialized); }
    public void setIsOpen(boolean open) { state.setIsOpen(open); }
    public void setWriteLock(boolean wl) { state.setWriteLock(wl); }
    public void setStopped(boolean stopped) { state.setStopped(stopped); }

    /********************************************************************/
    /*                     Metadata Setters/Getters                     */
    /********************************************************************/
    public void setMetadata(List<ServerInfo> metadata) {
        this.metadata = metadata;
        for (ServerInfo info : metadata) {
            if (info.getAddress().equals(this.info.getAddress()) && info.getServerPort().equals(this.info.getServerPort())) {
                this.info.setServerRange(info.getServerRange());
            }
        }
    }

    public synchronized List<ServerInfo> getMetadata() { return metadata; }

    /********************************************************************/
    /*                     Add/Remove listeners                         */
    /********************************************************************/
    public void addListener(ServerActionListener l) {
        runnableListeners.add(l);
    }

    public void removeListener(KVRequestHandler kvRequestHandler) {
        runnableListeners.remove(kvRequestHandler);
    }

    public ServerInfo getInfo() {
        return this.info;
    }

    public KVCache getKvCache() {
        return kvCache;
    }

    public void setKvCache(KVCache kvCache) {
        this.kvCache = kvCache;
    }
}