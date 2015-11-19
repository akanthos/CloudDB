package client;

import common.Serializer;
import common.ServerInfo;
import common.messages.KVMessage;
import common.messages.KVMessageImpl;
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
import java.util.List;

public class KVStore implements KVCommInterface {

    private static Logger logger = Logger.getLogger(KVStore.class);
    private HashMap<ServerInfo, ServerConnection> connections;
    private List<ServerInfo> metadataFromServer;
    private MD5Hash hash;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
        metadataFromServer = new LinkedList<>();
        connections = new HashMap<>();
        hash = new MD5Hash();
        // All key requests going to a single server initially.
        ServerInfo serverInfo = new ServerInfo(address, port, new KVRange(0L, Long.MAX_VALUE));
        metadataFromServer.add(serverInfo);
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
                byte[] response = send(kvMessage.getMsgBytes(), connection);
                KVMessageImpl kvMessageFromServer = (KVMessageImpl) Serializer.toObject(response);
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
        } catch (Exception e) {
            kvMessage.setStatus(KVMessage.StatusType.PUT_ERROR);
            logger.error(String.format("Put request cannot be performed. Key: %s, Value: %s", key, value));
            throw new Exception("Put request not successful");
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
                byte[] response = send(kvMessage.getMsgBytes(), connection);
                KVMessageImpl kvMessageFromServer = (KVMessageImpl) Serializer.toObject(response);
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
        } catch (Exception e) {
            kvMessage.setStatus(KVMessage.StatusType.GET_ERROR);
            logger.error(String.format("GET request cannot be performed. Key: %s", key));
            throw new Exception("GET request not successful");
        }
        return kvMessage;
	}

    /**
     * This function sends a message to the server using the established connection.
     *
     * @param msg
     * @throws CannotConnectException
     */
    public byte[] send(byte[] msg, ServerConnection serverConnection) throws CannotConnectException, IOException {
        Utilities.send(msg, serverConnection.getOutStream());
        byte[] answer = Utilities.receive(serverConnection.getInStream());
        return answer;
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
            metadataFromServer = messageFromServer.getMetadata();
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
        Long keyValue = hash.hash(key);
        ServerInfo serverInfo = getRange(keyValue);
        if (connections.containsKey(serverInfo)) {
            return connections.get(serverInfo);
        } else {
            ServerConnection serverConnection = new ServerConnection(serverInfo.getAddress(), serverInfo.getServerPort());
            connections.put(serverInfo, serverConnection);
            return serverConnection;
        }
    }

    /**
     * For a given key, get the server range
     * TODO: Change this to bubble sort
     *
     * @param keyValue
     * @return
     */
    private ServerInfo getRange(Long keyValue) {
        for (ServerInfo serverInfo: metadataFromServer) {
            if (serverInfo.getServerRange().isIndexInRange(keyValue)) {
                return serverInfo;
            }
        }
        return null;
    }
}
