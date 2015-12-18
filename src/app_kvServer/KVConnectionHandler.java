package app_kvServer;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * ConnectionHandler asynchronously handles the socket connections.
 * Uses a Executor Service for non-blocking concurrency.
 *
 */
public class KVConnectionHandler implements ConnectionHandler {


    private SocketServer server;
    private ExecutorService threadpool = null;


    /**
     *
     * @param server the SocketServer instance that carries this handler
     */
    public KVConnectionHandler(SocketServer server) {
        this.server = server;
        threadpool = Executors.newCachedThreadPool();
    }

    /**
     *
     * @param client Client Socket for connection
     * @param numOfClient Number of Clients (for debugging purposes)
     * @throws IOException
     */
    @Override
    public void handle(Socket client, int numOfClient) throws IOException {
        KVRequestHandler rr = new KVRequestHandler(this, server, client, numOfClient);
//        server.addListener(rr);
        threadpool.submit(rr);
    }

    @Override
    public void shutDown() {
        threadpool.shutdownNow();
    }

}


