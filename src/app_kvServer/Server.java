package app_kvServer;


import java.io.IOException;

public class Server {

    private static Integer port, cacheSize;
    private static String displacementStrategy;
    static KVCache keyValue_server = null;
    static SocketServer server = null;

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
                    keyValue_server = new KVCache(cacheSize, displacementStrategy);
                    server = new SocketServer("localhost", port);

                    ConnectionHandler handler = new KVConnectionHandler(keyValue_server, 10);
                    server.addHandler(handler);

                    server.connect();
                    System.out.println("Starting the KeyValue Server ...");

                    server.run();
                } else {
                    System.out.println("Please give a valid cache displacement strategy");
                    printHelp();
                }
            } catch (NumberFormatException e) {
                System.out.println("Cannot parse port number or cache size");
                printHelp();
            } catch (IOException e) {
                System.out.println("Cannot open socket... Terminating");
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