package app_kvServer;


import helpers.StorageException;
import org.apache.log4j.Logger;

import java.io.IOException;

public class Server {

    private static Integer port, cacheSize;
    private static String displacementStrategy;
    private static final Integer numberOfThreads = 10;
    static KVCache kvCache = null;
    static SocketServer server = null;
    private static Logger logger = Logger.getLogger(Server.class);

    /**
     * @param args
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

                    System.out.println("Binding Server:");
                    kvCache = new KVCache(cacheSize, displacementStrategy);
                    server = new SocketServer("localhost", port);

                    ConnectionHandler handler = new KVConnectionHandler(kvCache, numberOfThreads);
                    server.addHandler(handler);

                    server.connect();
                    System.out.println("Starting the KeyValue Server ...");

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

    private static void printHelp() {
        System.out.println("Usage: Server <port> <cache size> <displacement strategy>\n" +
                "    Strategy can be: FIFO, LRU, LFU");
    }
}