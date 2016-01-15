package client;

import common.Serializer;
import common.messages.KVMessage;
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

    public NotificationListener(ConcurrentHashMap<String, String> memoryCache) throws IOException {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
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
                KVMessageImpl kvMessage = (KVMessageImpl) Serializer.toObject(msgBytes);
                if (kvMessage.getStatus().equals(KVMessage.StatusType.NOTIFICATION)) {
                    if (memoryCache.containsKey(kvMessage.getKey()) && isEmpty(kvMessage.getValue())) {
                        memoryCache.remove(kvMessage.getKey());
                    } else {
                        memoryCache.put(kvMessage.getKey(), kvMessage.getValue());
                    }
                    logger.debug(String.format("NotificationListener: Updated memory cache. key: %s, value: %s", kvMessage.getKey(), kvMessage.getValue()));
                } else {
                    logger.error(String.format("NotificationListener: Unexpected message type: %s. Key: %s, Value: %s", kvMessage.getStatus(), kvMessage.getKey(), kvMessage.getValue()));
                }
            } catch (IOException e) {
                logger.error("NotificationListener: IOException while receiving notifications", e);
            } catch (CannotConnectException e) {
                logger.error("NotificationListener: Unrecognized exception while receiving notifications", e);
            } catch (ClassCastException e) {
                logger.error("NotificationListener: Unexpected message from coordinator", e);
            }
        }
    }

    private boolean isEmpty(String value) {
        return ((value == null) || (value.isEmpty()));
    }
}