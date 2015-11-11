package app_kvServer;

import common.utils.KVMetadata;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

/**
 * Class handling all TCP  connections
 * arriving on a given (host, port).
 * The connection handling lives in a KVRequestHandler
 */
public class SocketServer {
    boolean initialized;
    boolean writeLock;
    String hostname;
    int port;
    ConnectionHandler handler;
    ServerSocket server;
    boolean open;
    int numOfClients;
    private KVMetadata metadata;

    /**
     * Constructor of Socket server
     * @param hostname name of Server host
     * @param port Port Server is running on
     */
    public SocketServer(String hostname, Integer port) {
        this.initialized = false;
        this.writeLock = false;
        this.hostname = hostname;
        this.port = port;
        this.open = false;
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

    public synchronized void setWriteLock(){
        this.writeLock = true;
    }
    public synchronized void unsetWriteLock(){
        this.writeLock = false;
    }

    public void setMetadata(KVMetadata metadata) {
        this.metadata = metadata;
    }

}