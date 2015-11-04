package app_kvServer;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * ConnectionHandler asynchronously handles the socket connections.
 * Uses a Executor Service for non-blocking concurrency.
 *
 */
public class KVConnectionHandler implements ConnectionHandler {

    private KVCache kv_cache = null;
    private ExecutorService threadpool = null;

    /**
     *
     * @param kv_cache Cache to be queried
     * @param connections number of connections/threads
     */
    public KVConnectionHandler(KVCache kv_cache, int connections) {
        this.kv_cache = kv_cache;
//        threadpool = Executors.newFixedThreadPool(connections);
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
        Runnable rr = new KVRequestHandler(client, numOfClient, kv_cache);
//        Callable<Object> rr = new KVRequestHandler(client, numOfClient, kv_cache);
//        new Thread(rr).start();
        threadpool.submit(rr);
    }
}


