package app_kvServer;

import app_kvClient.KVClient;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * ConnectionHandler asynchronously handles the socket connections.
 * Uses a Threadpool for non-blocking concurrency.
 *
 */
public class KVConnectionHandler implements ConnectionHandler {

    private KVCache kv_cache = null;
    //private ThreadPool threadpool = null;
    private ExecutorService threadpool = null;


    public KVConnectionHandler(KVCache kv_cache, int connections) {
        this.kv_cache = kv_cache;
        //threadpool = new ThreadPool(connections);
        threadpool = Executors.newFixedThreadPool(connections);
    }

    /**
     *
     */
    @Override
    public void handle(Socket client, int numOfClients) throws IOException {
        Runnable rr = new KVClient(client, numOfClients, kv_cache);
        //try {
            //threadpool.addToQueue(rr);
            threadpool.execute(rr);
       /* } catch (InterruptedException e) {
            // Ignore this error
            return;
        }*/

    }
}
