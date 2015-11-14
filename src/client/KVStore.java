package client;

import app_kvEcs.ServerInfos;
import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import common.utils.KVMetadata;
import common.utils.KVRange;
import common.utils.Utilities;
import hashing.MD5Hash;
import helpers.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;

public class KVStore implements KVCommInterface {

    private static Logger logger = Logger.getLogger(KVStore.class);
    private KVMetadata metadata;
    private HashMap<KVRange, ServerConnection> connections;
    private LinkedList<KVRange> ranges;
    private MD5Hash hash;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
        ranges = new LinkedList<>();
        connections = new HashMap<>();
        metadata = new KVMetadata();
        hash = new MD5Hash();
        // All key requests going to a single server initially.
        KVRange range = new KVRange(0, Integer.MAX_VALUE);
        ranges.add(range);
        ServerInfos serverInfo = new ServerInfos(address, port);
        metadata.addServer(range, serverInfo);
	}

    /**
     * Puts an entry into the server.
     *
     * @param key the key that identifies the given value.
     * @param value the value that is indexed by the given key.
     * @return
     * @throws Exception
     */
	@Override
	public KVMessage put(String key, String value) throws Exception {
        // TODO: Perform key validation.
        boolean resendRequest = true;
        KVMessageImpl kvMessage = new KVMessageImpl(key, value, KVMessage.StatusType.PUT);;
        try {
            while (resendRequest) {
                ServerConnection connection = getServerConnection(key);
                logger.debug(String.format("Sending message: %s", kvMessage.toString()));
                String response = send(kvMessage.toString(), connection);
                KVMessageImpl kvMessageFromServer = new KVMessageImpl(response);
                if (kvMessageFromServer.getStatus().equals(KVMessage.StatusType.PUT_SUCCESS)) {
                    resendRequest = false;
                    kvMessage = kvMessageFromServer;
                } else if (retryRequest(kvMessageFromServer)) {
                    // Message needs to be retried with new metadata
                    continue;
                } else {
                    logger.error(String.format("Server not able to service the request. Status: %s. Request: PUT <%s, %s>", kvMessageFromServer.getStatus(), kvMessageFromServer.getKey(), kvMessageFromServer.getValue()));
                    kvMessage.setStatus(KVMessage.StatusType.PUT_ERROR);
                    resendRequest = false;
                }
            }
        } catch (CannotConnectException e) {
            kvMessage.setStatus(KVMessage.StatusType.PUT_ERROR);
            logger.error(e);
        }
        return kvMessage;
	}

    /**
     * Gets an entry from the server.
     *
     * @param key the key that identifies the value.
     * @return
     * @throws Exception
     */
	@Override
	public KVMessage get(String key) throws Exception {
        // TODO: Perform key validation
        boolean resendRequest = true;
        KVMessageImpl kvMessage = new KVMessageImpl(key, "", KVMessage.StatusType.GET);
        try {
            while (resendRequest) {
                ServerConnection connection = getServerConnection(key);
                logger.debug(String.format("Sending message: %s", kvMessage.toString()));
                String response = send(kvMessage.toString(), connection);
                KVMessageImpl kvMessageFromServer = new KVMessageImpl(response);
                if (kvMessageFromServer.getStatus().equals(KVMessage.StatusType.PUT_SUCCESS)) {
                    resendRequest = false;
                    kvMessage = kvMessageFromServer;
                } else if (retryRequest(kvMessageFromServer)) {
                    continue;
                } else {
                    logger.error(String.format("Server not able to service the request. Status: %s. Request: GET <%s>", kvMessageFromServer.getStatus(), kvMessageFromServer.getKey()));
                    kvMessage.setStatus(KVMessage.StatusType.GET_ERROR);
                    resendRequest = false;
                }
            }
        } catch (CannotConnectException e) {
            kvMessage.setStatus(KVMessage.StatusType.GET_ERROR);
            logger.error(e);
        }
        return kvMessage;
	}

    /**
     * This function sends a message to the server using the established connection.
     *
     * @param msg
     * @throws CannotConnectException
     */
    public String send(String msg, ServerConnection serverConnection) throws CannotConnectException, IOException {
        Utilities.send(msg, serverConnection.getOutStream());
        byte[] answer = Utilities.receive(serverConnection.getInStream());
        System.out.println("I am the client and received message SIZE: " + answer.length);
        try {
            String msgFromServer = new String(answer, "US-ASCII").trim();
            logger.info("Message received from server: " + msgFromServer);
            return msgFromServer;
        } catch (UnsupportedEncodingException e) {
            logger.error("Unsupported Encoding in message from server", e);
            throw new CannotConnectException(ErrorMessages.ERROR_INVALID_MESSAGE_FROM_SERVER);
        }
    }

    /**
     * This function sets the loglevel for the logger object passed on the argument passed to it.
     *
     * @param level Logging Level defined by the client using the CLI
     */
    public void logLevel(String level) {
        try {
            LogLevels currentLevel = LogLevels.valueOf(level.toUpperCase());
            switch (currentLevel) {
                case ALL:
                    logger.setLevel(Level.ALL);
                    logger.info("Loglevel changed to: ALL");
                    break;
                case DEBUG:
                    logger.setLevel(Level.DEBUG);
                    logger.info("Loglevel changed to: DEBUG");
                    break;
                case INFO:
                    logger.setLevel(Level.INFO);
                    logger.info("Loglevel changed to: INFO");
                    break;
                case WARN:
                    logger.setLevel(Level.WARN);
                    logger.info("Loglevel changed to: WARN");
                    break;
                case ERROR:
                    logger.setLevel(Level.ERROR);
                    logger.info("Loglevel changed to: ERROR");
                    break;
                case FATAL:
                    logger.setLevel(Level.FATAL);
                    logger.info("Loglevel changed to: FATAL");
                    break;
                case OFF:
                    logger.setLevel(Level.OFF);
                    logger.info("Loglevel changed to: OFF");
                    break;
                default:
                    break;
            }
            System.out.println("Log status: " + level);
        } catch (IllegalArgumentException e) {
            System.out.println("Please give a valid log level. Options:\n"
                    + "ALL, DEBUG, INFO, WARN, ERROR, FATAL, OFF");
        }
    }

    /**
     *
     * @return Logger's Loglevel
     */
    public Level getLogLevel() {
        return logger.getLevel();
    }

    /**
     * This function decides whether to retry a request or not.
     * If the server is not responsible for that request, the new metadata obtained is updated.
     *
     * @param messageFromServer
     * @return
     */
    private boolean retryRequest(KVMessageImpl messageFromServer) {
        if (messageFromServer.getStatus().equals(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)) {
            metadata = messageFromServer.getMetadata();
            ranges = new LinkedList<>(metadata.getMap().keySet());
            closeConnections();
            connections = new HashMap<>();
            return true;
        } else {
            // All other errors, request should not be retried and response send to client.
            return false;
        }
    }

    /**
     * Close existing open connections.
     */
    private void closeConnections() {
        for (ServerConnection connection: connections.values()) {
            connection.closeConnections();
        }
    }

    /**
     * For a given key, this function returns the serverconnection object.
     *
     * @param key
     * @return
     * @throws CannotConnectException
     */
    private ServerConnection getServerConnection(String key) throws CannotConnectException {
        // TODO: Rework this.
        Integer keyValue = (int) hash.hash(key);
        KVRange range = getRange(keyValue);
        ServerConnection returnConnection;
        if (connections.containsKey(range)) {
            returnConnection = connections.get(range);
        } else {
            ServerInfos info = metadata.getServer(range);
            returnConnection = new ServerConnection(info.getServerIP(), info.getHostPort());
            connections.put(range, returnConnection);
        }
        return returnConnection;
    }

    /**
     * For a given key, get the server range
     *
     * @param keyValue
     * @return
     */
    private KVRange getRange(Integer keyValue) {
        for (KVRange range: ranges) {
            if (range.isInRange(keyValue)) {
                return range;
            }
        }
        return null;
    }
}
