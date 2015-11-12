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
    private KVCache kv_cache = null;
    private ExecutorService threadpool = null;

    public KVConnectionHandler(SocketServer server) {
        this.server = server;
    }

    /**
     *
     * @param server the SocketServre instance that carries this handler
     * @param connections number of connections/threads
     */
    public KVConnectionHandler(/*KVCache kv_cache*/ SocketServer server, int connections) {
        this.server = server;
//        this.kv_cache = kv_cache;
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
        KVRequestHandler rr = new KVRequestHandler(this, server, client, numOfClient, kv_cache);
        server.addListener(rr);
//        new Thread(rr).start();
        threadpool.submit(rr);
    }

    public synchronized void setCache(KVCache kvCache) {
        this.kv_cache = kv_cache;
    }
    @Override
    public void shutDown() {
        threadpool.shutdownNow();
    }
}


