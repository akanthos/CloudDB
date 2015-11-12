package app_kvServer;

import common.utils.KVMetadata;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
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
        this.initialized = false;
        this.stopped = true;
        this.writeLocked = false;
        this.hostname = hostname;
        this.port = port;
        this.open = false;
        this.runnableListeners = new CopyOnWriteArraySet<>();//Collections.synchronizedList(new ArrayList<>());
    }

    /**
     * Creation of unbound server socket
     * @throws IOException
     */
    public void connect() throws IOException {
        if (open) return;
        server = new ServerSocket();
        server.bind(new InetSocketAddress(hostname, port));
        open = true;
    }

    /**
     * While the servers is open, accept requests and service them asynchronously.
     * @throws IOException if there is a network error (for instance if the socket is inadvertently closed)
     */
    public void run() throws IOException {
        if (!open) {
            throw new IOException();
        }
        while (open) {
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
        this.open = false;
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

    public void setInitialized(boolean initialized) {
        this.initialized = initialized;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public void setMetadata(KVMetadata metadata) {
        this.metadata = metadata;
    }

    public KVMetadata getMetadata() {
        return metadata;
    }

    public void addListener(ServerActionListener l) {
        runnableListeners.add(l);
    }

    public void removeListener(KVRequestHandler kvRequestHandler) {
        runnableListeners.remove(kvRequestHandler);
    }

    public void startServing() {
        this.stopped = false;
        for (ServerActionListener l : runnableListeners) {
            l.serverStarted();
        }
    }
    public void stopServing() {
        this.stopped = true;
        for (ServerActionListener l : runnableListeners) {
            l.serverStopped();
        }
    }
    public synchronized void writeLock() {
        this.writeLocked = true;
//        for (ServerActionListener l : runnableListeners) {
//            l.serverWriteLocked();
//        }
    }
    public synchronized void writeUnlock() {
        this.writeLocked = false;
//        for (ServerActionListener l : runnableListeners) {
//            l.serverWriteUnlocked();
//        }
    }
    public synchronized boolean isWriteLocked() {
        return writeLocked;
    }

    public void shutDown() {
        this.open = false;
        this.closeSocket();
        this.handler.shutDown();
        for (ServerActionListener l : runnableListeners) {
            l.serverShutDown();
        }
        // In Java 8 : runnableListeners.forEach(ServerActionListener::serverShutDown);
    }



}