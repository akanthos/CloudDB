package client;

import common.messages.KVMessageImpl;
import common.utils.Utilities;
import helpers.CannotConnectException;
import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by sreenath on 14.01.16.
 */
public class NotificationListener implements Runnable {

    private ConcurrentHashMap<String, String> memoryCache;
    private ServerSocket serverSocket;
    private static Logger logger = Logger.getLogger(KVMessageImpl.class);

    static {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
    }

    public NotificationListener(ConcurrentHashMap<String, String> memoryCache) throws IOException {
        this.memoryCache = memoryCache;
        serverSocket = new ServerSocket();
        serverSocket.bind(new InetSocketAddress("127.0.0.1", Constants.NOTIFICATION_LISTEN_PORT));
    }

    @Override
    public void run() {
        logger.info("NotificationListener running...");
        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                logger.debug(String.format("Received notification from %s", clientSocket.getInetAddress().getHostAddress()));
                byte[] msgBytes = Utilities.receive(clientSocket.getInputStream());
                // Call serializer to deserialize the notification message
                // Modify the hashmap
            } catch (IOException e) {
                logger.error("NotificationListener: IOException while receiving notifications", e);
            } catch (CannotConnectException e) {
                logger.error("NotificationListener: Unrecognized exception while receiving notifications", e);
            }
        }
    }
}
