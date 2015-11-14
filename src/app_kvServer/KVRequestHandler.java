package app_kvServer;

import common.messages.KVAdminMessage;
import common.messages.KVAdminMessage.StatusType;
import common.messages.KVAdminMessageImpl;
import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import common.utils.KVMetadata;
import common.utils.Utilities;
import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;


/**
 * Logic for handling client requests.
 * This is the runnable class that will be running inside the threadpool when assigned to a thread
 */
public class KVRequestHandler implements Runnable, ServerActionListener {
    KVConnectionHandler myHandler;
    SocketServer server;
    Socket clientSocket;
    KVCache kvCache;
    int clientNumber;
    InputStream inputStream;
    OutputStream outputStream;
    private ServerState state;
    private KVMetadata metadata;
    private static Logger logger = Logger.getLogger(KVRequestHandler.class);

    public KVRequestHandler(KVConnectionHandler handler, SocketServer server, Socket clientSocket, int clientNumber, KVCache kvCache) throws IOException {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
        this.myHandler = handler;
        this.server = server;
        this.state = server.getState();
        this.clientSocket = clientSocket;
        this.clientNumber = clientNumber;
        this.kvCache = kvCache;

        try {
            inputStream = clientSocket.getInputStream();
            outputStream = clientSocket.getOutputStream();
            this.state.setIsOpen(true);
        } catch (IOException e) {
            logger.error(String.format("Client: %d. Unable to initialize streams.", clientNumber), e);
            throw new IOException("Unable to initialize streams from socket");
        }
        logger.info(String.format("Client: %d connected", clientNumber));
    }

    /**
     * run function handles the incoming client's request
     */
    @Override
    public void run() {
        try {
            KVMessage kvMessage;
            KVAdminMessage kvAdminMessage;
            KVMessageImpl kvResponse;
            KVAdminMessageImpl kvAdminResponse;
            byte[] byteMessage;
            String stringMessage;
            boolean isOpen;
            synchronized (state) {
                /*
                 * Checking state in synchronized block.
                 * No client serving in synchronized block
                 * because that could block the thread that updates
                 * the state forever, essentially blocking the response
                 * to the ECS forever.
                 */
                isOpen = state.isOpen();
            }
            while (isOpen) {
                try {
                    // Get a new message
                    byteMessage = Utilities.receive(inputStream);

                    if (byteMessage[0] == -1) {
                        state.setIsOpen(false);
                    } else {
                        stringMessage = new String(byteMessage, Constants.DEFAULT_ENCODING).trim();
                        kvMessage = extractKVMessage(stringMessage);

                        // If it fails, it returns a GENERAL_ERROR
                        if (kvMessage.getStatus() == KVMessage.StatusType.GENERAL_ERROR) {
                            // It may be an admin message
                            kvAdminMessage = extractKVAdminMessage(stringMessage);
                            kvAdminResponse = processAdminMessage(kvAdminMessage);
                            // Send appropriate response according to the above backend actions
                            Utilities.send(kvAdminResponse.getMsgBytes(), outputStream);
                        } else {
                            kvResponse = processMessage(kvMessage);
                            // Send appropriate response according to the above backend actions
                            Utilities.send(kvResponse.getMsgBytes(), outputStream);
                        }
                    }
                } catch (IOException ioe) {
                    /* connection either terminated by the client or lost due to
                     * network problems*/
                    logger.error("Error! Connection lost!");
                    state.setIsOpen(false);
                } catch (Exception e) {
                    logger.error("Unable to parse string message from client");
                    state.setIsOpen(false);
                }
            }

        } catch (Exception e) {
            logger.error(e);
        } finally {
            try {
                if (clientSocket != null) {
                    inputStream.close();
                    outputStream.close();
                    clientSocket.close();
                    server.removeListener(this);
                }
            }catch(IOException ioe){
                logger.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }

    /**
     * Admin Message Unmarshaller
     * @param messageString message to be unmarshalled to KVMessage type
     * @return resulting KVMessage
     */
    private KVAdminMessage extractKVAdminMessage(String messageString) {
        KVAdminMessage kvAdminMessage = null;
        try {
            kvAdminMessage = new KVAdminMessageImpl(messageString);
        } catch (Exception e) {
            logger.error(String.format("Unable to process message from client %d.", clientNumber), e);
            kvAdminMessage = new KVAdminMessageImpl(StatusType.GENERAL_ERROR);
        }
        return kvAdminMessage;
    }

    /**
     * Client Message Unmarshaller
     * @param messageString message to be unmarshalled to KVMessage type
     * @return resulting KVMessage
     */
    private KVMessage extractKVMessage(String messageString) {
        KVMessage kvMessage = null;
        try {
            kvMessage = new KVMessageImpl(messageString);
        } catch (Exception e) {
            logger.error(String.format("Unable to process message from client %d.", clientNumber), e);
            kvMessage = new KVMessageImpl("", "", KVMessage.StatusType.GENERAL_ERROR);
        }
        return kvMessage;
    }

    /**
     * Process of the KVAdminMessage and communication with the cache
     * @param kvAdminMessage KVAdminMessage representation of the ECSImpl request
     * @return resulting KVAdminMessageImpl
     */
    private KVAdminMessageImpl processAdminMessage(KVAdminMessage kvAdminMessage) {
        KVAdminMessageImpl response;
        if (kvAdminMessage.getStatus().equals(StatusType.INIT)) {
            return server.initKVServer(kvAdminMessage.getMetadata(), kvAdminMessage.getCacheSize(), kvAdminMessage.getDisplacementStrategy());
        } else if (kvAdminMessage.getStatus().equals(StatusType.START)) {
            return server.startServing(this);
        } else if (kvAdminMessage.getStatus().equals(StatusType.STOP)) {
            return server.stopServing(this);
        } else if (kvAdminMessage.getStatus().equals(StatusType.SHUT_DOWN)) {
            return server.shutDown(this);
        } else if (kvAdminMessage.getStatus().equals(StatusType.LOCK_WRITE)) {
            return server.writeLock(this);
        } else if (kvAdminMessage.getStatus().equals(StatusType.UNLOCK_WRITE)) {
            return server.writeUnlock(this);
        } else if (kvAdminMessage.getStatus().equals(StatusType.MOVE_DATA)) {
            return server.moveData(kvAdminMessage.getRange(), kvAdminMessage.getServerInfo());
        } else if (kvAdminMessage.getStatus().equals(StatusType.UPDATE_METADATA)) {
            return server.update(kvAdminMessage.getMetadata(), this);
        } else {
            logger.error(String.format("ECSImpl: Invalid message from ECSImpl: %s", kvAdminMessage.toString()));
            response = new KVAdminMessageImpl(StatusType.GENERAL_ERROR);
        }
        return response;
    }


    /**
     * Process of the KVMessage and communication with the cache
     * @param kvMessage KVMessage representation of the client request
     * @return resulting KVMessageImpl
     */
    private KVMessageImpl processMessage(KVMessage kvMessage) {
        if (state.isStopped()) {
            return new KVMessageImpl("", "", KVMessage.StatusType.SERVER_STOPPED);
        }
        if (server.isInitialized()) {
            KVMetadata meta;
            synchronized (this.metadata) {
                meta = new KVMetadata(this.metadata);
            }
            if (kvMessage.getStatus().equals(KVMessage.StatusType.GET)) {
                // Do the GET
                if (meta.getMap().get(server.info).isIndexInRange(kvMessage.getHash())) {
                    return kvCache.get(kvMessage.getKey());
                }
                else {
                    return new KVMessageImpl("", "", KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
                }
            } else if (kvMessage.getStatus().equals(KVMessage.StatusType.PUT)) {
                if (meta.getMap().get(server.info).isIndexInRange(kvMessage.getHash())) {
                    if (state.isWriteLock()) {
                        // Cannot proceed PUT request
                        return new KVMessageImpl("", "", KVMessage.StatusType.SERVER_WRITE_LOCK);
                    } else {
                        // Do the PUT
                        return kvCache.put(kvMessage.getKey(), kvMessage.getValue());
                    }
                }
                else {
                    return new KVMessageImpl("", "", KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
                }
            } else {
                logger.error(String.format("Client: %d. Invalid message from client: %s", clientNumber, kvMessage.toString()));
                return new KVMessageImpl("", "", KVMessage.StatusType.GENERAL_ERROR);
            }
        } else {
            return new KVMessageImpl("", "", KVMessage.StatusType.GENERAL_ERROR);
        }
    }


    @Override
    public void updateState(ServerState s) {
        synchronized (state) {
            state = s;
        }
    }

    @Override
    public void updateMetadata(KVMetadata m) {
        synchronized (metadata) {
            this.metadata = m;
        }
    }

}
