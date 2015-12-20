package app_kvServer;


import common.ServerInfo;
import common.utils.KVRange;
import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class of KVServer including main()
 * function setting up and running the Server
 */
public class KVServer {

    private ServerInfo info;
    private final Integer numberOfThreads = 10;
    SocketServer server = null;
    private static Logger logger = Logger.getLogger(KVServer.class);

    /**
     * Constructor of the Server
     * @param address
     * @param port
     */
    public KVServer(String address, Integer port) {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
        this.info = new ServerInfo(address, port);
        // TODO: Add info name??
        this.server = new SocketServer(this.info);

        ConnectionHandler handler = new KVConnectionHandler(server);
        server.addConnectionHandler(handler);

        try {
            server.connect();

            server.run();
        } catch (IOException e) {
            logger.error("Cannot open socket... Terminating", e);
        }
    }

    public KVServer(String address, Integer port, Integer cacheSize, String displacementStrategy) {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
        this.info = new ServerInfo(address, port);
        this.server = new SocketServer(this.info);

//        ArrayList<ServerInfo> metadata = new ArrayList<>();
//        metadata.add(new ServerInfo(address, port, new KVRange(0, Long.MAX_VALUE)));

        ConnectionHandler handler = new KVConnectionHandler(server);
        server.addConnectionHandler(handler);
//        server.initKVServer(metadata, cacheSize, displacementStrategy);


        try {
            server.connect();
            server.startServing();
            server.run();
            //server.cleanUp();
        } catch (IOException e) {
            logger.error("Cannot open socket... Terminating", e);
        }
    }

    public KVServer(String address, Integer port, Integer cacheSize, String displacementStrategy, String testing) {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
        this.info = new ServerInfo(address, port);
        this.server = new SocketServer(this.info);

//        ArrayList<ServerInfo> metadata = new ArrayList<>();
//        metadata.add(new ServerInfo(address, port, new KVRange(0, Long.MAX_VALUE)));

        ConnectionHandler handler = new KVConnectionHandler(server);
        server.addConnectionHandler(handler);

        ArrayList<ServerInfo> metadata = new ArrayList<>();
        metadata.add(new ServerInfo(server.getInfo().getAddress(), server.getInfo().getServerPort(), new KVRange(0, 0)));
        server.initKVServer(metadata, cacheSize, displacementStrategy);


        try {
            server.connect();
            server.startServing();
            server.run();
            //server.cleanUp();
        } catch (IOException e) {
            logger.error("Cannot open socket... Terminating", e);
        }
    }




    /**
     * @param args <Port> <Cachesize> <CachePolicy>
     * @throws IOException
     */
    public static void main(final String[] args) {

        if( args.length == 1 ) {
            try {
                new Thread(new Runnable() {
                    public void run() {
                        new KVServer("127.0.0.1", Integer.parseInt(args[0]), 10, "FIFO");
                    }
                }).start();

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                /*
                 * informing the ProcessInvoker of the ECS Machine that the
                 * KVServer process started successfully
                 */
                System.out.write("SUCCESS".getBytes());
                System.out.flush();
            }
            catch (IOException e) {

                e.printStackTrace();

            }

        }
        else {
            printHelp();
        }


    }

    /**
     * Initialize the KVServer with the meta-data, it’s local cache size, and the
     * cache displacement strategy, and block it for client requests, i.e., all client
     * requests are rejected with an SERVER_STOPPED error message; ECSImpl requests have
     * to be processed.
     *
     * @param metadata the initial metadata
     * @param cacheSize the cache size required
     * @param displacementStrategy the cache displacement strategy to adopt
     */
    public synchronized void initKVServer(List<ServerInfo> metadata, Integer cacheSize, String displacementStrategy){
        if ((displacementStrategy.equals("FIFO") || displacementStrategy.equals("LRU") || displacementStrategy.equals("LFU"))
                && cacheSize > 0 && metadata != null) {
            System.out.println("Binding KVServer:");
            server.initKVServer(metadata, cacheSize, displacementStrategy);
        } else {
            System.out.println("Please give a valid cache displacement strategy");
            printHelp();
        }
    }

    /**
     * Starts the KVServer, all client requests and all ECSImpl requests are processed.
     *
     */
    public void start() {
        server.startServing();
    }

    /**
     * Stops the KVServer, all client requests are rejected and only ECSImpl requests are processed.
     *
     */
    public void stop() {
        server.stopServing();
    }

    /**
     * Exits the KVServer application.
     *
     */
    public void shutDown() {
        server.shutDown();
    }

    /**
     * Lock the KVServer for write operations.
     *
     */
    public void lockWrite() {
        server.writeLock();
    }

    /**
     * Unlock the KVServer for write operations.
     *
     */
    public void unLockWrite() {
        server.writeUnlock();
    }

    /**
     * Transfer a subset (range) of the KVServer’s data to another KVServer
     * (reallocation before removing this server or adding a new KVServer to
     * the ring); send a notification to the ECSImpl, if data transfer is completed.
     *
     * @param range the range where the moved data should belong to
     * @param serverInfo the server to move the data to
     */
    public void moveData(KVRange range, ServerInfo serverInfo) {
        server.moveData(range, serverInfo);
    }

    /**
     * Update the meta-data repository of this server
     *
     * @param metadata the new metadata
     */
    public void update(List<ServerInfo> metadata) {
        server.update(metadata);
    }

    public ServerInfo getInfo() {
        return info;
    }

    /**
     * Checks if a given port is a valid socket port
     * @param hostPort Port Server is running on
     * @return true if port is valid, else false
     */
    private boolean isPortValid(Integer hostPort) {
        return ((info.getServerPort() >= 0) && (info.getServerPort() <= 65535));
    }

    /**
     *
     * @param cacheSize the size of the LRU || FIFO Cache
     * @return  false if given Cache size <0 else True
     */
    private boolean isCacheSizeValid(Integer cacheSize) {
        return (cacheSize>=0);
    }

    /**
     * Execution usage help
     */
    private static void printHelp() {
        System.out.println("Usage: KVServer <port> <cache size> <displacement strategy>\n" +
                "    Strategy can be: FIFO, LRU, LFU");
    }
}