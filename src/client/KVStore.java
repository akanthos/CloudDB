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
import java.net.ConnectException;
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
//        connections = new HashMap<>();
        hash = new MD5Hash();
        // All key requests going to a single server initially.
//        searchComparator = new SearchComparator();
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
//        connections.put(currentServer, currentConnection);
        setIsConnected(true);
        logger.info("Switched to server " + currentServer.getAddress() +
                ":" + currentServer.getServerPort());
    }

    /**
     * Initialize KVStore with an instance of ServerInfo
     * and connect to the server
     * @param sererInfo
     * @throws Exception
     */
    public void connect(ServerInfo sererInfo) throws Exception {
        if (connected) {
            disconnect();
        }
        currentServer = new ServerInfo(sererInfo.getAddress(), sererInfo.getServerPort(), sererInfo.getServerRange());
//        ServerInfo serverInfo = new ServerInfo(address, port, new KVRange(0L, Long.MAX_VALUE));
//        metadataFromServer.add(currentServer);
        currentConnection = new ServerConnection(currentServer.getAddress(), currentServer.getServerPort());
//        connections.put(currentServer, currentConnection);
        setIsConnected(true);
        logger.info("Switched to server " + currentServer.getAddress() +
                ":" + currentServer.getServerPort());
    }

    @Override
    public void disconnect() {
        if (currentConnection != null) {
            currentConnection.closeConnections();
        }
//        closeConnections();
//        connections.clear();
        currentConnection = null;
        currentServer = null;
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
                ServerConnection connection = getServerConnection(key, false);
                if (connection == null) {
                    logger.error(String.format("Put request cannot be performed. Key: %s, Value: %s", key, value));
                    throw new Exception("Client is disconnected");
                }
                logger.debug(String.format("Sending message: %s", kvMessage.toString()));
                byte[] response;
                try {
                    response = send(kvMessage.getMsgBytes(), connection);
                    if (response[0] == -1) {
                        disconnect();
                        continue;
                    }
                }
                catch (Exception e) {
                    disconnect();
                    continue;
                }
                logger.info("Sent PUT message to : " + connection.getAddress() + ":" + connection.getServerPort());
                KVMessageImpl kvMessageFromServer = (KVMessageImpl) Serializer.toObject(response);
                if (kvMessageFromServer.getStatus().equals(KVMessage.StatusType.PUT_SUCCESS)
                        || kvMessageFromServer.getStatus().equals(KVMessage.StatusType.PUT_UPDATE)
                        || kvMessageFromServer.getStatus().equals(KVMessage.StatusType.DELETE_SUCCESS)) {
                    resendRequest = false;
                    kvMessage = kvMessageFromServer;
                } else if (kvMessageFromServer.getStatus().equals(KVMessage.StatusType.DELETE_ERROR)) {
                    logger.info("Server responded with DELETE_ERROR");
                    kvMessage.setStatus(KVMessage.StatusType.DELETE_ERROR);
                    resendRequest = false;
                } else if (kvMessageFromServer.getStatus().equals(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)) {
                    retryRequest(kvMessageFromServer);
                    resendRequest = true;
                } else if (kvMessageFromServer.getStatus().equals(KVMessage.StatusType.SERVER_STOPPED)) {
                    logger.info("Server responded STOPPED");
                    kvMessage.setStatus(KVMessage.StatusType.SERVER_STOPPED);
                    resendRequest = false;
                } else if (kvMessageFromServer.getStatus().equals(KVMessage.StatusType.SERVER_WRITE_LOCK)) {
                    logger.info("Server responded WRITE_LOCKED");
                    kvMessage.setStatus(KVMessage.StatusType.SERVER_WRITE_LOCK);
                    resendRequest = false;
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
                ServerConnection connection = getServerConnection(key, true);
                if (connection == null) {
                    logger.error(String.format("Get request cannot be performed. Key: %s", key));
                    throw new Exception("Client is disconnected");
                }
                logger.debug(String.format("Sending (GET) message: %s to %s:%s", kvMessage.toString(), connection.getAddress(), connection.getServerPort()));
                byte[] response;
                try {
                    response = send(kvMessage.getMsgBytes(), connection);
                    if (response[0] == -1) {
                        disconnect();
                        continue;
                    }
                }
                catch (Exception e) {
                    disconnect();
                    continue;
                }

                KVMessageImpl kvMessageFromServer = (KVMessageImpl) Serializer.toObject(response);
                if (kvMessageFromServer.getStatus().equals(KVMessage.StatusType.GET_SUCCESS)) {
                    resendRequest = false;
                    kvMessage = kvMessageFromServer;
                } else if (kvMessageFromServer.getStatus().equals(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)) {
                    logger.info("He was not responsible!!! Oh god!");
                    retryRequest(kvMessageFromServer);
                    resendRequest = true;
                } else if (kvMessageFromServer.getStatus().equals(KVMessage.StatusType.SERVER_STOPPED)) {
                    logger.info("Server responded stopped");
                    kvMessage.setStatus(KVMessage.StatusType.SERVER_STOPPED);
                    resendRequest = false;
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
//        closeConnections();
//        connections = new HashMap<>();
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
     * For a given key, this function returns the serverconnection object responsible for that key
     *
     * @param key: The key to be queried
     * @param randomizeReplicas: If true, a randomly choosen node from the coordinator and replicas are returned. If false, the coordinator is returned.
     * @return
     * @throws Exception
     */
    private ServerConnection getServerConnection(String key, boolean randomizeReplicas) throws Exception {
        Long keyValue = hash.hash(key);
        // Passing the key in the form of a dummy object
        // TODO: Is there a cleaner way to do this?
        for (ServerInfo m : metadataFromServer) {
            if (m.getServerRange().isIndexInRange(keyValue)) {
                if (randomizeReplicas) {
                    m = getRandomReplica(m);
                }
                if (currentServer != null && m.getAddress().equals(currentServer.getAddress()) && m.getServerPort().equals(currentServer.getServerPort())) {
                    return currentConnection;
                } else {
                    disconnect();
                    try {
                        logger.info("Trying server: " + m.getAddress()+":"+m.getServerPort()+", range:" + m.getFromIndex() + ":" + m.getToIndex() );
                        connect(m);
                        return currentConnection;
                    } catch (IOException e) {
                        logger.info("Connection caught..." );
                        return tryOtherNodes(metadataFromServer, m);
                    }

                }
            }
        }
        return null;
//        int index = Collections.binarySearch(metadataFromServer, new ServerInfo("", 0, new KVRange(keyValue, 0L)), searchComparator);
//        ServerInfo serverInfo = metadataFromServer.get(index);
//        if (currentServer.equals(serverInfo)) {
//            return currentConnection;
//        }
//        else {
//            connect(serverInfo.getAddress(), serverInfo.getServerPort());
//            return currentConnection;
////            ServerConnection serverConnection = new ServerConnection(serverInfo.getAddress(), serverInfo.getServerPort());
////            connections.put(serverInfo, serverConnection);
////            return serverConnection;
//        }

    }

    private ServerConnection tryOtherNodes(List<ServerInfo> metadataFromServer, ServerInfo m) {
        List<ServerInfo> newMetadata = new ArrayList<>(metadataFromServer) ;
        newMetadata.remove(m);
        logger.info("Trying remaining servers");
        for (ServerInfo s : newMetadata) {
            disconnect();
            try {
                logger.info("Trying server: " + s.getAddress()+":"+s.getServerPort()+", range:" + s.getFromIndex() + ":" + s.getToIndex() );
                connect(s);
                return currentConnection;
            } catch (Exception e) {
            }
        }
        return null;
    }

    /**
     * Returns a randomly chosen node from a coordiator and its replicas
     *
     * @param m
     * @return
     */
    private ServerInfo getRandomReplica(ServerInfo m) {
        List<ServerInfo> replicas = Utilities.getReplicas(metadataFromServer, m);
        return replicas.get(new Random().nextInt(replicas.size()));
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
