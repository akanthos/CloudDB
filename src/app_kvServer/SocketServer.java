package app_kvServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

/**
 * Class handling all TCP  connections
 * arriving on a given (host, port).
 * The connection handling lives in a ConnectionHandler (KVConnectionHandler)
 */
public class SocketServer {
    String hostname;
    int port;
    ConnectionHandler handler;
    ServerSocket server;
    boolean open;

    public SocketServer(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
        open = false;
    }

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
            handler.handle(server.accept());
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


    private void closeSocket() {
        try {
            server.close();
            System.out.println("socket closed");
        } catch (IOException e) {
            return;
        }
    }

    protected void finalize(){

        closeSocket();
    }
}