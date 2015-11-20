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
import java.util.*;

public class KVStore implements KVCommInterface {

    private static Logger logger = Logger.getLogger(KVStore.class);
    private HashMap<ServerInfo, ServerConnection> connections;
    private List<ServerInfo> metadataFromServer;
    private MD5Hash hash;
    private SearchComparator searchComparator;
    private ServerInfo currentServer;
    private ServerConnection currentConnection;
    boolean connected;

	/**
	 * Initialize KVStore
	 *
	 */
	public KVStore() {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
        metadataFromServer = new LinkedList<>();
        connections = new HashMap<>();
        hash = new MD5Hash();
        // All key requests going to a single server initially.
        searchComparator = new SearchComparator();
        connected = false;
	}

    /**
     * Initialize KVStore with address and port of KVServer
     * and connect to the server
     * @param hostAddress the address of the KVServer
     * @param port the port of the KVServer
     * @throws Exception
     */
    @Override
    public void connect(String hostAddress, Integer port) throws Exception {
        if (connected) {
            disconnect();
        }
        currentServer = new ServerInfo(hostAddress, port, new KVRange(0L, Long.MAX_VALUE));
//        ServerInfo serverInfo = new ServerInfo(address, port, new KVRange(0L, Long.MAX_VALUE));
        metadataFromServer.add(currentServer);
        currentConnection = new ServerConnection(currentServer.getAddress(), currentServer.getServerPort());
        connections.put(currentServer, currentConnection);
        setIsConnected(true);
    }

    @Override
    public void disconnect() {
        currentConnection.closeConnections();
        for (ServerInfo server : connections.keySet()) {
            ServerConnection conn = connections.get(server);
            conn.closeConnections();
        }
        connections.clear();
        setIsConnected(false);
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
        if (!isConnected()) {
            throw new Exception("Client not Connected to server");
        }
        // TODO: Perform key validation.
        boolean resendRequest = true;
        KVMessageImpl kvMessage = new KVMessageImpl(key, value, KVMessage.StatusType.PUT);
        try {
            while (resendRequest) {
                ServerConnection connection = getServerConnection(key);
                if (connection == null) {
                    logger.error(String.format("Put request cannot be performed. Key: %s, Value: %s", key, value));
                    throw new Exception("Client is disconnected");
                }
                logger.debug(String.format("Sending message: %s", kvMessage.toString()));
                byte[] response = send(kvMessage.getMsgBytes(), connection);
                KVMessageImpl kvMessageFromServer = (KVMessageImpl) Serializer.toObject(response);
                if (kvMessageFromServer.getStatus().equals(KVMessage.StatusType.PUT_SUCCESS)
                        || kvMessageFromServer.getStatus().equals(KVMessage.StatusType.PUT_UPDATE)
                        || kvMessageFromServer.getStatus().equals(KVMessage.StatusType.DELETE_SUCCESS)) {
                    resendRequest = false;
                    kvMessage = kvMessageFromServer;
                } else if (kvMessageFromServer.getStatus().equals(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)) {
                    retryRequest(kvMessageFromServer);
                    resendRequest = true;
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
        if (!isConnected()) {
            throw new Exception("Client not Connected to server");
        }
        // TODO: Perform key validation
        boolean resendRequest = true;
        KVMessageImpl kvMessage = new KVMessageImpl(key, "", KVMessage.StatusType.GET);
        try {
            while (resendRequest) {
                ServerConnection connection = getServerConnection(key);
                if (connection == null) {
                    logger.error(String.format("Get request cannot be performed. Key: %s", key));
                    throw new Exception("Client is disconnected");
                }
                logger.debug(String.format("Sending message: %s", kvMessage.toString()));
                byte[] response = send(kvMessage.getMsgBytes(), connection);
                KVMessageImpl kvMessageFromServer = (KVMessageImpl) Serializer.toObject(response);
                if (kvMessageFromServer.getStatus().equals(KVMessage.StatusType.GET_SUCCESS)) {
                    resendRequest = false;
                    kvMessage = kvMessageFromServer;
                } else if (kvMessageFromServer.getStatus().equals(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)) {
                    retryRequest(kvMessageFromServer);
                    resendRequest = true;
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
     * This function re provisions the data structures based on the new metadata
     *
     * @param messageFromServer
     * @return
     */
    private void retryRequest(KVMessageImpl messageFromServer) {
        closeConnections();
        connections = new HashMap<>();
        metadataFromServer = messageFromServer.getMetadata();
        Collections.sort(metadataFromServer);
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
     * @throws Exception
     */
    private ServerConnection getServerConnection(String key) throws Exception {
        Long keyValue = hash.hash(key);
        // Passing the key in the form of a dummy object
        // TODO: Is there a cleaner way to do this?
        int index = Collections.binarySearch(metadataFromServer, new ServerInfo("", 0, new KVRange(keyValue, 0L)), searchComparator);
        ServerInfo serverInfo = metadataFromServer.get(index);
        if (currentServer.equals(serverInfo)) {
            return currentConnection;
        }
        else {
            connect(serverInfo.getAddress(), serverInfo.getServerPort());
            return currentConnection;
//            ServerConnection serverConnection = new ServerConnection(serverInfo.getAddress(), serverInfo.getServerPort());
//            connections.put(serverInfo, serverConnection);
//            return serverConnection;
        }

    }



    class SearchComparator implements Comparator<ServerInfo> {

        @Override
        public int compare(ServerInfo serverInfo, ServerInfo t1) {
            // t1 is a dummy object. key is the low value of the range.
            long key = t1.getServerRange().getLow();
            KVRange range = serverInfo.getServerRange();
            if (range.isIndexInRange(key)) {
                return 0;
            } else {
                long low = range.getLow();
                long high = range.getHigh();
                // Last node of the ring
                if (low > high) {
                    // If the key is not in the range, the key has to be lesser.
                    return 1;
                } else if (low > key) {
                    // Key towards the left.
                    return 1;
                } else {
                    // Key towards the right.
                    return -1;
                }
            }
        }
    }

    /**
     *
     * @return True if connected else False
     */
    public boolean isConnected() {
        return connected;
    }

    /**
     * Set connection Status
     * @param connected
     */
    public void setIsConnected(boolean connected) {
        this.connected = connected;
    }


}
