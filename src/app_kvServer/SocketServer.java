package app_kvServer;

import app_kvEcs.ServerInfos;
import com.sun.corba.se.spi.activation.Server;
import common.messages.KVAdminMessage;
import common.messages.KVAdminMessageImpl;
import common.utils.KVMetadata;
import common.utils.KVRange;
import helpers.StorageException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Class handling all TCP  connections
 * arriving on a given (host, port).
 * The connection handling lives in a KVRequestHandler
 */
public class SocketServer {
    ServerState state;
    String hostname;
    int port;
    ConnectionHandler handler;
    ServerSocket server;
    int numOfClients;

    private CopyOnWriteArraySet<ServerActionListener> runnableListeners;
    private KVMetadata metadata;

    /**
     * Constructor of Socket server
     * @param hostname name of Server host
     * @param port Port Server is running on
     */
    public SocketServer(String hostname, Integer port) {
        this.state = new ServerState();
        state.setInitialized(false);
        state.setStopped(true);
        state.setWriteLock(false);
        state.setIsOpen(false);
        this.hostname = hostname;
        this.port = port;
        this.runnableListeners = new CopyOnWriteArraySet<>();//Collections.synchronizedList(new ArrayList<>());
    }

    /**
     * Creation of unbound server socket
     * @throws IOException
     */
    public void connect() throws IOException {
        if (state.isOpen()) return;
        server = new ServerSocket();
        server.bind(new InetSocketAddress(hostname, port));
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
    public synchronized KVAdminMessageImpl initKVServer(KVMetadata metadata, Integer cacheSize, String displacementStrategy){
        try {
            handler.setCache(new KVCache(cacheSize, displacementStrategy));
        } catch (StorageException e) {
            return new KVAdminMessageImpl(KVAdminMessage.StatusType.GENERAL_ERROR);
        }
        setMetadata(metadata);
        state.setInitialized(true);
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.INIT_SUCCESS);
    }
    public synchronized KVAdminMessageImpl startServing(KVRequestHandler handler) {
        state.setStopped(false);
        updateStateToListeners(handler);
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.START_SUCCESS);
    }
    public synchronized KVAdminMessageImpl stopServing(KVRequestHandler handler) {
        state.setStopped(true);
        updateStateToListeners(handler);
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.STOP_SUCCESS);
    }
    public synchronized KVAdminMessageImpl writeLock(KVRequestHandler handler) {
        state.setWriteLock(true);
        updateStateToListeners(handler);
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.LOCK_WRITE_SUCCESS);
    }
    public synchronized KVAdminMessageImpl writeUnlock(KVRequestHandler handler) {
        state.setWriteLock(false);
        updateStateToListeners(handler);
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.UNLOCK_WRITE_SUCCESS);
    }
    public synchronized KVAdminMessageImpl shutDown(KVRequestHandler handler) {
        state.setIsOpen(false);
        updateStateToListeners(handler);
        this.closeSocket();
        this.handler.shutDown();
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.SHUT_DOWN_SUCCESS)
    }
    public synchronized KVAdminMessageImpl moveData(KVRange range, ServerInfos server) {
        // TODO:
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.GENERAL_ERROR);
    }

    public synchronized KVAdminMessageImpl update(KVMetadata metadata, KVRequestHandler handler) {
        setMetadata(metadata);
        updateMetadataToListeners(handler);
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.UPDATE_SUCCESS);
    }
    private void updateStateToListeners(KVRequestHandler handler) {
        for (ServerActionListener l : runnableListeners) {
            // Maybe exclude myself
//            if (l != handler) {
            l.updateState(this.state);
//            }
        }
    }
    private void updateMetadataToListeners(KVRequestHandler handler) {
        for (ServerActionListener l : runnableListeners) {
            // Maybe exclude myself
//            if (l != handler) {
            l.updateMetadata(this.metadata);
//            }
        }
    }

    /********************************************************************/
    /*                       State Getters                              */
    /********************************************************************/
    public synchronized ServerState getState() {
        return state;
    }
    public synchronized boolean isInitialized() {
        return state.isInitialized();
    }
    public synchronized boolean isOpen() {
        return state.isOpen();
    }
    public synchronized boolean isWriteLocked() {
        return state.isWriteLock();
    }
    public synchronized boolean isStopped() {
        return state.isStopped();
    }

    /********************************************************************/
    /*                       State Setters                              */
    /********************************************************************/
    public synchronized void setState(ServerState state) { this.state = state; }
    public synchronized void setInitialized(boolean initialized) { state.setInitialized(initialized); }
    public synchronized void setIsOpen(boolean open) { state.setIsOpen(open); }
    public synchronized void setWriteLock(boolean wl) { state.setWriteLock(wl); }
    public synchronized void setStopped(boolean stopped) { state.setStopped(stopped); }

    /********************************************************************/
    /*                     Metadata Setters/Getters                     */
    /********************************************************************/
    public synchronized void setMetadata(KVMetadata metadata) { this.metadata = metadata; }

    public synchronized KVMetadata getMetadata() { return metadata; }

    /********************************************************************/
    /*                     Add/Remove listeners                         */
    /********************************************************************/
    public void addListener(ServerActionListener l) {
        runnableListeners.add(l);
    }

    public void removeListener(KVRequestHandler kvRequestHandler) {
        runnableListeners.remove(kvRequestHandler);
    }
}