package app_kvServer;


import app_kvEcs.ServerInfos;
import common.utils.KVMetadata;
import common.utils.KVRange;
import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;

/**
 * Base class of KVServer including main()
 * function setting up and running the Server
 */
public class KVServer {

    private static ServerInfos info;
    private static Integer cacheSize;
    private static String displacementStrategy;
    private static final Integer numberOfThreads = 10;
    static KVCache kvCache = null;
    static SocketServer server = null;
    private static Logger logger = Logger.getLogger(KVServer.class);

    /**
     * Constructor of the Server
     * @param port
     */
    public KVServer(Integer port) {//, Integer cacheSize, String cacheStrategy) {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
        this.info = new ServerInfos(null, port);
        this.server = new SocketServer("localhost", port);

        ConnectionHandler handler = new KVConnectionHandler(server, numberOfThreads);//kvCache
        server.addHandler(handler);

        try {
            server.connect();
            server.run();
        } catch (IOException e) {
            logger.error("Cannot open socket... Terminating", e);
        }


//        this.cacheSize = cacheSize;
//        try {
//
//            if (cacheStrategy.equals("FIFO") || cacheStrategy.equals("LRU") || cacheStrategy.equals("LFU")) {
//                displacementStrategy = cacheStrategy;
//
//                System.out.println("Binding KVServer:");
//                this.kvCache = new KVCache(cacheSize, displacementStrategy);
//                this.server = new SocketServer("localhost", port);
//
//                ConnectionHandler handler = new KVConnectionHandler(kvCache, numberOfThreads);
//                server.addHandler(handler);
//
//                server.connect();
//                System.out.println("Starting the KeyValue KVServer ...");
//
//                server.run();
//            } else {
//                System.out.println("Please give a valid cache displacement strategy");
//                printHelp();
//            }
//        } catch (IOException e) {
//            logger.error("Cannot open socket... Terminating", e);
//        } catch (StorageException e) {
//            logger.error(e.getErrorMessage(), e);
//            e.printStackTrace();
//        }
    }



    /**
     * @param args <Port> <Cachesize> <CachePolicy>
     * @throws IOException
     */
    public static void main(String[] args) {

        if( args.length == 1 ) {
            try {
                Integer port = Integer.parseInt(args[0]);
                KVServer kvServer = new KVServer(port);
            } catch (NumberFormatException e) {
                logger.error("Cannot parse port number or cache size", e);
                printHelp();
            }
        }
        else {
            printHelp();
        }


    }

    /**
     * Initialize the KVServer with the meta-data, it’s local cache size, and the
     * cache displacement strategy, and block it for client requests, i.e., all client
     * requests are rejected with an SERVER_STOPPED error message; ECS requests have
     * to be processed.
     *
     * @param metadata
     * @param cacheSize
     * @param displacementStrategy
     */
    public void initKVServer(KVMetadata metadata, Integer cacheSize, String displacementStrategy){


    }

    /**
     * Starts the KVServer, all client requests and all ECS requests are processed.
     *
     */
    public void start() {

    }

    /**
     * Stops the KVServer, all client requests are rejected and only ECS requests are processed.
     *
     */
    public void stop() {

    }

    /**
     * Exits the KVServer application.
     *
     */
    public void shutDown() {

    }

    /**
     * Lock the KVServer for write operations.
     *
     */
    public void lockWrite() {

    }

    /**
     * Unlock the KVServer for write operations.
     *
     */
    public void unLockWrite() {

    }

    /**
     * Transfer a subset (range) of the KVServer’s data to another KVServer
     * (reallocation before removing this server or adding a new KVServer to
     * the ring); send a notification to the ECS, if data transfer is completed.
     *
     * @param range
     * @param server
     */
    public void moveData(KVRange range, ServerInfos server) {

    }

    /**
     * Update the meta-data repository of this server
     *
     * @param metadata
     */
    public void update(KVMetadata metadata) {

    }

    /**
     *
     * @param hostPort Port Server is running on
     * @return
     */
    private static boolean isPortValid(Integer hostPort) {
        return ((info.getHostPort() >= 0) && (info.getHostPort() <= 65535));
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