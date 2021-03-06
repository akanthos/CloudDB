package app_kvServer;

import common.utils.Utilities;
import org.apache.log4j.Logger;

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
    private ArrayList<KVRequestHandler> currentRunnables;
    private static Logger logger = Logger.getLogger(KVConnectionHandler.class);


    /**
     *
     * @param server the SocketServer instance that carries this handler
     */
    public KVConnectionHandler(SocketServer server) {
        this.server = server;
        this.currentRunnables = new ArrayList<>();
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
        currentRunnables.add(rr);
//        server.addListener(rr);
        threadpool.submit(rr);
    }

    /**
     * Shutdown method for killing runnable client handlers and closing all connections
     */
    @Override
    public void shutDown() {
        logger.info("Shutting down all runnables");
        for (KVRequestHandler r : currentRunnables) {
            r.stop = true;
            ConnectionHelper.connectionTearDown(r.inputStream, r.outputStream, r.clientSocket, logger);
            currentRunnables.remove(r);
        }
        threadpool.shutdownNow();
    }

    /**
     * Unsubscribes a runnable client handler from the list of client handlers
     * @param r the runnable client to be unsubscribed
     */
    public void unsubscribe(KVRequestHandler r) {
        currentRunnables.remove(r);
    }

}


