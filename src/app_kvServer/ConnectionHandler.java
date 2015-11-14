package app_kvServer;

import java.io.IOException;
import java.net.Socket;

/**
 * Connection Handler interface
 */
public interface ConnectionHandler {

    public void handle(Socket client, int numOfClients) throws IOException;
    public void shutDown();
    public void setCache(KVCache cache);

}
