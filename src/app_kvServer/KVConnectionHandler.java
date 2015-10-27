package app_kvServer;

import app_kvClient.KVClient;

import java.io.IOException;
import java.net.Socket;

/**
 * ConnectionHandler asynchronously handles the socket connections.
 * Uses a Threadpool for non-blocking concurrency.
 *
 */
public class KVConnectionHandler implements ConnectionHandler {

    private KVCache kv_cache = null;
    private ThreadPool threadpool = null;


    public KVConnectionHandler(KVCache kv_cache, int connections) {
        this.kv_cache = kv_cache;
        threadpool = new ThreadPool(connections);
    }

    /**
     *
     */
    @Override
    public void handle(Socket client, int numOfClients) throws IOException {

        //Runnable r = new ClientHandler(kv_cache, client);
        Runnable rr = new KVClient(client, numOfClients);
        try {
            threadpool.addToQueue(rr);
        } catch (InterruptedException e) {
            // Ignore this error
            return;
        }

    }
}
