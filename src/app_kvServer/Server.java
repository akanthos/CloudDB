package app_kvServer;


import java.io.IOException;

public class Server {

    static KVCache keyValue_server = null;
    static SocketServer server = null;

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        //Primary
        // TODO: Read command line parameters for 1) port number, 2) cache size, and 3) displacement strategy
        System.out.println("Binding Server:");
        keyValue_server = new KVCache(3, "fifo");
        server = new SocketServer("localhost", 5673);
        ConnectionHandler handler = new KVConnectionHandler(keyValue_server, 10);

        server.addHandler(handler);
        server.connect();
        System.out.println("Starting the KeyValue Server ...");
        server.run();
    }

}