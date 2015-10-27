package app_kvServer;


import java.io.IOException;

public class Server {

    static KVServerNew keyValue_server = null;
    static SocketServer server = null;

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        System.out.println("Binding Server:");
        keyValue_server = new KVServerNew(5550, 100, "fifo");
        server = new SocketServer("localhost", 8080);
        ConnectionHandler handler = new KVConnectionHandler(keyValue_server, 10);

        server.addHandler(handler);
        server.connect();
        System.out.println("Starting the KeyValue Server ...");
        server.run();
    }

}