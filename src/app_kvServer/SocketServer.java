package app_kvServer;

import common.utils.KVMetadata;

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
    public void addHandler(ConnectionHandler handler) {

        this.handler = handler;
    }

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

    public synchronized void setInitialized(boolean initialized) {
        state.setInitialized(initialized);
    }

    public synchronized boolean isInitialized() {
        return state.isInitialized();
    }

    public synchronized void setMetadata(KVMetadata metadata) {
        this.metadata = metadata;
    }

    public synchronized KVMetadata getMetadata() {
        return metadata;
    }

    public void addListener(ServerActionListener l) {
        runnableListeners.add(l);
    }

    public void removeListener(KVRequestHandler kvRequestHandler) {
        runnableListeners.remove(kvRequestHandler);
    }

    private void updateStateToListeners(KVRequestHandler handler) {
        for (ServerActionListener l : runnableListeners) {
            // Maybe exclude myself
//            if (l != handler) {
                l.updateState(this.state);
//            }
        }
    }
    /* State Setters - only one of them can be called at a time by other threads */
    public synchronized void startServing(KVRequestHandler handler) {
        state.setStopped(false);
        updateStateToListeners(handler);
    }
    public synchronized void stopServing(KVRequestHandler handler) {
        state.setStopped(true);
        updateStateToListeners(handler);
    }
    public synchronized void writeLock(KVRequestHandler handler) {
        state.setWriteLock(true);
        updateStateToListeners(handler);
    }
    public synchronized void writeUnlock(KVRequestHandler handler) {
        state.setWriteLock(false);
        updateStateToListeners(handler);
    }
    public synchronized void shutDown(KVRequestHandler handler) {
        state.setIsOpen(false);
        updateStateToListeners(handler);
        this.closeSocket();
        this.handler.shutDown();
    }

    /* State Getters*/
    public synchronized boolean isWriteLocked() {
        return state.isWriteLock();
    }
    public synchronized boolean isStopped() {
        return state.isStopped();
    }
}