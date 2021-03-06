package app_kvServer;

import java.io.IOException;
import java.net.Socket;

/**
 * Connection Handler interface
 */
public interface ConnectionHandler {

    void handle(Socket client, int numOfClients) throws IOException;
    void shutDown();
    void unsubscribe(KVRequestHandler r);

}
