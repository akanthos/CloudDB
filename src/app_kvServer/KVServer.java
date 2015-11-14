package app_kvServer;


import common.ServerInfo;
import common.utils.KVMetadata;
import common.utils.KVRange;
import helpers.Constants;
import helpers.StorageException;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.util.HashMap;

/**
 * Base class of KVServer including main()
 * function setting up and running the Server
 */
public class KVServer {

    private static ServerInfo info;
    private static final Integer numberOfThreads = 10;
    static SocketServer server = null;
    private static Logger logger = Logger.getLogger(KVServer.class);

    /**
     * Constructor of the Server
     * @param port
     */
    public KVServer(String address, Integer port) {//, Integer cacheSize, String cacheStrategy) {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
        this.info = new ServerInfo(address, port);
        this.server = new SocketServer("localhost", port, this.info);

        ConnectionHandler handler = new KVConnectionHandler(server, numberOfThreads);//kvCache
        server.addHandler(handler);

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
        this.server = new SocketServer("localhost", port, this.info);

        HashMap<ServerInfo, KVRange> map = new HashMap<>();
        map.put(this.getInfo(), new KVRange(0, Long.MAX_VALUE));
        KVMetadata metadata = new KVMetadata( map  );

        ConnectionHandler handler = new KVConnectionHandler(server, numberOfThreads);//kvCache
        server.addHandler(handler);
        server.initKVServer(metadata, cacheSize, displacementStrategy);
        server.startServing();

        try {
            server.connect();
            server.run();
        } catch (IOException e) {
            logger.error("Cannot open socket... Terminating", e);
        }
    }



    /**
     * @param args <Port> <Cachesize> <CachePolicy>
     * @throws IOException
     */
    public static void main(String[] args) {

//        if( args.length == 1 ) {
//            try {
//                Integer port = Integer.parseInt(args[0]);
                new Thread(new Runnable() {
                    public void run() {
                        new KVServer("localhost", 50000, 10, "FIFO");
                    }
                }).start();

//            } catch (NumberFormatException e) {
//                logger.error("Cannot parse port number or cache size", e);
//                printHelp();
//            }
//        }
//        else {
//            printHelp();
//        }


    }

    /**
     * Initialize the KVServer with the meta-data, it’s local cache size, and the
     * cache displacement strategy, and block it for client requests, i.e., all client
     * requests are rejected with an SERVER_STOPPED error message; ECSImpl requests have
     * to be processed.
     *
     * @param metadata
     * @param cacheSize
     * @param displacementStrategy
     */
    public synchronized void initKVServer(KVMetadata metadata, Integer cacheSize, String displacementStrategy){
        if ((displacementStrategy.equals("FIFO") || displacementStrategy.equals("LRU") || displacementStrategy.equals("LFU"))
                && cacheSize > 0 && metadata != null) {
            System.out.println("Binding KVServer:");
            try {
                KVCache kvCache = new KVCache(cacheSize, displacementStrategy);
                server.initKVServer(metadata, cacheSize, displacementStrategy);
            } catch (StorageException e) {
                logger.error("Cannot create KVCache instance", e);
            }
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
     * @param range
     * @param serverInfo
     */
    public void moveData(KVRange range, ServerInfo serverInfo) {
        server.moveData(range, serverInfo);
    }

    /**
     * Update the meta-data repository of this server
     *
     * @param metadata
     */
    public void update(KVMetadata metadata) {
        server.update(metadata);
    }

    public ServerInfo getInfo() {
        return info;
    }

    /**
     *
     * @param hostPort Port Server is running on
     * @return
     */
    private static boolean isPortValid(Integer hostPort) {
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