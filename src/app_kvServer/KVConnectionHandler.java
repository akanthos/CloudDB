package app_kvServer;

import java.io.IOException;
import java.net.Socket;

/**
 * ConnectionHandler asynchronously handles the socket connections.
 * Uses a Threadpool for non-blocking concurrency.
 *
 */
public class KVConnectionHandler implements ConnectionHandler {

    private KVServerNew kv_Server = null;
    private ThreadPool threadpool = null;


    public KVConnectionHandler(KVServerNew kvServerNew, int connections) {
        this.kv_Server = kvServerNew;
        threadpool = new ThreadPool(connections);
    }


    /**
     * Handling the client request (GET/SEND socket message using KVMessage send/)
     */
    //==================================================================
    //******************************************************************
    private class ClientHandler implements Runnable {

        private KVServerNew kvServerNew = null;
        private Socket client = null;

        @Override
        public void run() {
            System.out.println("Handling");

        }

        public ClientHandler(KVServerNew kvServerNew, Socket client) {
            this.kvServerNew = kvServerNew;
            this.client = client;
        }
    }

    /**
     *
     */
    @Override
    public void handle(Socket client) throws IOException {

        Runnable r = new ClientHandler(kv_Server, client);
        try {
            threadpool.addToQueue(r);
        } catch (InterruptedException e) {
            // Ignore this error
            return;
        }

    }
}
