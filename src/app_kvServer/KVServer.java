package app_kvServer;


import helpers.StorageException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base class of KVServer including main()
 */
public class KVServer {

    private static Integer port, cacheSize;
    private static String displacementStrategy;
    private static final Integer numberOfThreads = Runtime.getRuntime().availableProcessors();
    static KVCache kvCache = null;
    static SocketServer server = null;
    private static Logger logger = Logger.getLogger(KVServer.class);

    public KVServer(Integer port, Integer cacheSize, String cacheStrategy) {
        this.port = port;
        this.cacheSize = cacheSize;
        try {

            if (cacheStrategy.equals("FIFO") || cacheStrategy.equals("LRU") || cacheStrategy.equals("LFU")) {
                displacementStrategy = cacheStrategy;

                System.out.println("Binding KVServer:");
                this.kvCache = new KVCache(cacheSize, displacementStrategy);
                this.server = new SocketServer("localhost", port);

                ConnectionHandler handler = new KVConnectionHandler(kvCache, numberOfThreads);
                server.addHandler(handler);

                server.connect();
                System.out.println("Starting the KeyValue KVServer ...");

                server.run();
            } else {
                System.out.println("Please give a valid cache displacement strategy");
                printHelp();
            }
        } catch (IOException e) {
            logger.error("Cannot open socket... Terminating", e);
        } catch (StorageException e) {
            logger.error(e.getErrorMessage(), e);
            e.printStackTrace();
        }
    }



    /**
     * @param args <Port> <Cachesize> <CachePolicy>
     * @throws IOException
     */
    public static void main(String[] args) {

        //Primary
        // Parsing port
        if( args.length == 3 ) {
            try {
                port = Integer.parseInt(args[0]);
                cacheSize = Integer.parseInt(args[1]);
                if (args[2].equals("FIFO") || args[2].equals("LRU") || args[2].equals("LFU")) {
                    displacementStrategy = args[2];

                    System.out.println("Binding KVServer:");
                    kvCache = new KVCache(cacheSize, displacementStrategy);
                    server = new SocketServer("localhost", port);

                    ConnectionHandler handler = new KVConnectionHandler(kvCache, numberOfThreads);
                    server.addHandler(handler);

                    server.connect();
                    System.out.println("Starting the KeyValue KVServer ...");

                    server.run();
                } else {
                    System.out.println("Please give a valid cache displacement strategy");
                    printHelp();
                }
            } catch (NumberFormatException e) {
                logger.error("Cannot parse port number or cache size", e);
                printHelp();
            } catch (IOException e) {
                logger.error("Cannot open socket... Terminating", e);
            } catch (StorageException e) {
                logger.error(e.getErrorMessage(), e);
                e.printStackTrace();
            }
        }
        else {
            printHelp();
        }


    }

    /**
     *
     * @param hostPort Port Server is running on
     * @return
     */
    private static boolean isPortValid(Integer hostPort) {
        return ((port >= 0) && (port <= 65535));
    }

    /**
     *
     * @param cacheSize
     * @return  False if given Cache size <0 else True
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