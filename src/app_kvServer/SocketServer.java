package app_kvServer;

import common.Serializer;
import common.ServerInfo;
import common.messages.*;
import common.utils.KVRange;
import common.utils.Utilities;
import helpers.CannotConnectException;
import helpers.StorageException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Class handling all TCP  connections
 * arriving on a given (host, port).
 * The connection handling lives in a KVRequestHandler
 */
public class SocketServer {
    private ServerInfo info;
    private volatile ServerState state;
    private ConnectionHandler handler;
    private KVCache kvCache;
    private ServerSocket server;
    private int numOfClients;
    private List<ServerInfo> metadata;
//    private CopyOnWriteArraySet<ServerActionListener> runnableListeners;
    private static Logger logger = Logger.getLogger(SocketServer.class);


    /**
     * Constructor of Socket server
     * @param info
     */
    public SocketServer(ServerInfo info) {
        this.info = info;
        this.state = new ServerState(
                /*init*/ false,
                /*open*/ false,
                /*writeLock*/ false,
                /*stop*/ true
        );
//        this.runnableListeners = new CopyOnWriteArraySet<>();//Collections.synchronizedList(new ArrayList<>());
    }

    /**
     * Creation of unbound server socket
     * @throws IOException
     */
    public void connect() throws IOException {
        if (state.isOpen()) return;
        server = new ServerSocket();
        server.bind(new InetSocketAddress(info.getAddress(), info.getServerPort()));
        state.setIsOpen(true);
    }

    /**
     * While the servers is open, accept requests and service them asynchronously.
     * @throws IOException if there is a network error (for instance if the socket is inadvertently closed)
     */
    public void run() throws IOException {
        if (!state.isOpen()) {
            throw new IOException();
        }
        while (state.isOpen()) {
            numOfClients++;
            handler.handle(server.accept(), numOfClients);
        }
    }

    /**
     * Add the connection handler for the current socket server
     * @param handler is logic for servicing a network connection
     */
    public void addHandler(ConnectionHandler handler) { this.handler = handler; }

    /**
     * Stop the ServerSocket
     */
    public void stop() {
        state.setIsOpen(false);
        this.closeSocket();
    }

    /**
     * close Server Socket
     */
    private void closeSocket() {
        try {
            server.close();
            System.out.println("socket closed");
        } catch (IOException e) {
            return;
        }
    }

    /********************************************************************/
    /*                      Administrative Commands                     */
    /********************************************************************/
    /**
     * Initializes the server
     * @param metadata metadata for initialization
     * @param cacheSize cache size for initialization
     * @param displacementStrategy displacement strategy for initialization
     * @return a status message
     */
    public synchronized KVAdminMessageImpl initKVServer(List<ServerInfo> metadata, Integer cacheSize, String displacementStrategy){
        try {
            this.kvCache = new KVCache(cacheSize, displacementStrategy, info);
        } catch (StorageException e) {
            logger.error("Cannot create KVCache", e);
            return new KVAdminMessageImpl(KVAdminMessage.StatusType.GENERAL_ERROR);
        }
        setMetadata(metadata);
        state.setInitialized(true);
        info.setLaunched(true);
//        logger.info("Just initialized myself!!!");
//        logger.info("My Address is: " + this.info.getAddress());
//        logger.info("My Port is: " + this.info.getServerPort());
//        logger.info("My Range is: " + this.info.getFromIndex() + ":" + this.info.getToIndex());
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_SUCCESS);
    }

    /**
     * Starts the server
     * @return a status message
     */
    public synchronized KVAdminMessageImpl startServing() {
        state.setStopped(false);
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_SUCCESS);
    }

    /**
     * Stops the server
     * @return a status message
     */
    public synchronized KVAdminMessageImpl stopServing() {
        state.setStopped(true);
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_SUCCESS);
    }

    /**
     * Locks the server for writes
     * @return a status message
     */
    public synchronized KVAdminMessageImpl writeLock() {
        state.setWriteLock(true);
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_SUCCESS);
    }

    /**
     * Unlocks the sever for writes
     * @return a status message
     */
    public synchronized KVAdminMessageImpl writeUnlock() {
        state.setWriteLock(false);
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_SUCCESS);
    }

    /**
     * Shuts down the server
     * @return a status message
     */
    public synchronized KVAdminMessageImpl shutDown() {
        state.setIsOpen(false);
        this.closeSocket();
        this.handler.shutDown();
//        logger.info("SHUTTING DOWN: ");
//        logger.info("My Address is: " + this.info.getAddress());
//        logger.info("My Port is: " + this.info.getServerPort());
//        logger.info("My Range is: " + this.info.getFromIndex() + ":" + this.info.getToIndex());
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_SUCCESS);
    }

    public synchronized KVAdminMessageImpl moveData(KVRange range, ServerInfo server) {
//        logger.info("My Address is: " + this.info.getAddress());
//        logger.info("My Port is: " + this.info.getServerPort());
//        logger.info("My Range is: " + this.info.getFromIndex() + ":" + this.info.getToIndex());
//        logger.info("Move data called");
        ArrayList<KVPair> pairsToSend = kvCache.getPairsInRange(range);
        return sendToServer(pairsToSend, server);
    }

    private KVAdminMessageImpl sendToServer(ArrayList<KVPair> pairsToSend, ServerInfo server) {
        // Send ServerMessage "MOVE_DATA" message to "server" and wait for answer from that server
        // If it's MOVE_DATA_SUCCESS => send back OPERATION_SUCCESS
        // If it's MOVE_DATA_FAILURE => send back OPERATION_FAILED
        KVAdminMessageImpl reply;
        InputStream inStream = null;
        OutputStream outStream = null;
        Socket clientSocket = null;
        try {
            /***************************/
            /* Connect to other server */
            /***************************/

            InetAddress address = InetAddress.getByName(server.getAddress());
            clientSocket = new Socket(address, server.getServerPort());
            inStream = clientSocket.getInputStream();
            outStream = clientSocket.getOutputStream();

            /*****************************************************/
            /* Send MOVE_DATA server message to the other server */
            /*****************************************************/

            KVServerMessageImpl bulkPutMessage = new KVServerMessageImpl(pairsToSend, KVServerMessage.StatusType.MOVE_DATA);
            Utilities.send(bulkPutMessage, outStream);
            byte[] bulkPutAnswerBytes = Utilities.receive(inStream);
            KVServerMessageImpl bulkPutAnswer = (KVServerMessageImpl) Serializer.toObject(bulkPutAnswerBytes);
            if (bulkPutAnswer.getStatus().equals(KVServerMessage.StatusType.MOVE_DATA_SUCCESS)) {
                reply = new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_SUCCESS);
            }
            else {
                reply = new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_FAILED);
            }

        } catch (UnknownHostException e) {
            logger.error("KVServer hostname cannot be resolved", e);
            reply = new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_FAILED);
        } catch (IOException e) {
            logger.error("Error while connecting to the server for bulk put.", e);
            reply = new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_FAILED);
        } catch (CannotConnectException e) {
            logger.error("Error while connecting to the server.", e);
            reply = new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_FAILED);
        } finally {
            /****************************************/
            /* Tear down connection to other server */
            /****************************************/
            try {
                if (inStream != null
                    && outStream != null
                    && clientSocket != null) {
                    inStream.close();
                    outStream.close();
                    clientSocket.close();
                }
            } catch(IOException ioe){
                logger.error("Error! Unable to tear down connection for bulk put!", ioe);
            }
        }
        return reply;
    }

    public synchronized KVAdminMessageImpl update(List<ServerInfo> metadata) {
        setMetadata(metadata);
        return new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_SUCCESS);
    }

    /********************************************************************/
    /*                          Server Requests                         */
    /********************************************************************/
    public synchronized KVServerMessageImpl insertNewDataToCache(List<KVPair> kvPairs) {
//        logger.info("My Address is: " + this.info.getAddress());
//        logger.info("My Port is: " + this.info.getServerPort());
//        logger.info("My Range is: " + this.info.getFromIndex() + ":" + this.info.getToIndex());
//        logger.info("Inserting new data");
        for (KVPair kv : kvPairs) {
            kvCache.put(kv.getKey(), kv.getValue());
        }
        return new KVServerMessageImpl(KVServerMessage.StatusType.MOVE_DATA_SUCCESS);
        // If it fails, respond with MOVE_FAILURE ???
    }


    /********************************************************************/
    /*                          State Getters                           */
    /********************************************************************/
    public synchronized ServerState getState() {
        return state;
    }
    public boolean isInitialized() {
        return state.isInitialized();
    }
    public boolean isOpen() { return state.isOpen(); }
    public boolean isWriteLocked() {
        return state.isWriteLock();
    }
    public boolean isStopped() {
        return state.isStopped();
    }

    /********************************************************************/
    /*                       State Setters                              */
    /********************************************************************/
    public synchronized void setState(ServerState state) { this.state = state; }
    public void setInitialized(boolean initialized) { state.setInitialized(initialized); }
    public void setIsOpen(boolean open) { state.setIsOpen(open); }
    public void setWriteLock(boolean wl) { state.setWriteLock(wl); }
    public void setStopped(boolean stopped) { state.setStopped(stopped); }

    /********************************************************************/
    /*                     Metadata Setters/Getters                     */
    /********************************************************************/
    public void setMetadata(List<ServerInfo> metadata) {
        this.metadata = metadata;
        for (ServerInfo info : metadata) {
            logger.info("Server " + info.getAddress() + ":" + info.getServerPort() +
                    " range : " + info.getFromIndex() + ":" + info.getToIndex());
            if (info.getAddress().equals(this.info.getAddress()) && info.getServerPort().equals(this.info.getServerPort())) {
                this.info.setServerRange(info.getServerRange());
                logger.info("Update my range to: " + this.info.getFromIndex() +":"+ this.info.getToIndex());
            }
        }
    }

    public synchronized List<ServerInfo> getMetadata() { return metadata; }

    public ServerInfo getInfo() {
        return this.info;
    }

    public KVCache getKvCache() {
        return kvCache;
    }

    public void setKvCache(KVCache kvCache) {
        this.kvCache = kvCache;
    }


    /**
     * Clears the cache
     */
    public void cleanUp() {
        this.kvCache.cleanUp();
    }
}