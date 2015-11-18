package app_kvServer;

import common.Serializer;
import common.messages.*;
import common.messages.KVAdminMessage.StatusType;
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
public class KVRequestHandler implements Runnable/*, ServerActionListener*/ {
    KVConnectionHandler myHandler;
    SocketServer server;
    Socket clientSocket;
    int clientNumber;
    InputStream inputStream;
    OutputStream outputStream;
    private static Logger logger = Logger.getLogger(KVRequestHandler.class);

    public KVRequestHandler(KVConnectionHandler handler, SocketServer server, Socket clientSocket, int clientNumber) throws IOException {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
        this.myHandler = handler;
        this.server = server;
        this.clientSocket = clientSocket;
        this.clientNumber = clientNumber;

        try {
            inputStream = clientSocket.getInputStream();
            outputStream = clientSocket.getOutputStream();
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
            KVServerMessage kvServerMessage;
            KVMessageImpl kvResponse;
            KVAdminMessageImpl kvAdminResponse;
            KVServerMessageImpl kvServerResponse;
            byte[] byteMessage;
            boolean clientConnected = true;
            while (clientConnected && server.isOpen()) {
                try {
                    // Get a new message
                    byteMessage = Utilities.receive(inputStream);

                    if (byteMessage[0] == -1) {
                        clientConnected = false;
                    } else {
                        AbstractMessage abstractMessage = Serializer.toObject(byteMessage);
                        if (abstractMessage.getMessageType().equals(AbstractMessage.MessageType.CLIENT_MESSAGE)) {
                            kvMessage = (KVMessageImpl) abstractMessage;
                            kvResponse = processMessage(kvMessage);
                            Utilities.send(kvResponse, outputStream);
                        } else if (abstractMessage.getMessageType().equals(AbstractMessage.MessageType.ECS_MESSAGE)) {
                            kvAdminMessage = (KVAdminMessageImpl) abstractMessage;
                            kvAdminResponse = processAdminMessage(kvAdminMessage);
                            Utilities.send(kvAdminResponse, outputStream);
                        } else if (abstractMessage.getMessageType().equals(AbstractMessage.MessageType.SERVER_MESSAGE)) {
                            kvServerMessage = (KVServerMessageImpl) abstractMessage;
                            kvServerResponse = processServerMessage(kvServerMessage);
                            Utilities.send(kvServerResponse, outputStream);
                        }
                        else {
                            Utilities.send(new KVMessageImpl("", "", KVMessage.StatusType.GENERAL_ERROR), outputStream);
                        }
                    }
                } catch (IOException ioe) {
                    /* connection either terminated by the client or lost due to
                     * network problems*/
                    logger.error("Error! Connection lost!");
                    clientConnected = false;
                } catch (Exception e) {
                    logger.error("Unable to parse string message from client");
                    clientConnected = false;
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
//                    server.removeListener(this);
                }
            }catch(IOException ioe){
                logger.error("Error! Unable to tear down connection!", ioe);
            }
        }
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
            return server.startServing();
        } else if (kvAdminMessage.getStatus().equals(StatusType.STOP)) {
            return server.stopServing();
        } else if (kvAdminMessage.getStatus().equals(StatusType.SHUT_DOWN)) {
            return server.shutDown();
        } else if (kvAdminMessage.getStatus().equals(StatusType.LOCK_WRITE)) {
            return server.writeLock();
        } else if (kvAdminMessage.getStatus().equals(StatusType.UNLOCK_WRITE)) {
            return server.writeUnlock();
        } else if (kvAdminMessage.getStatus().equals(StatusType.MOVE_DATA)) {
            return server.moveData(kvAdminMessage.getRange(), kvAdminMessage.getServerInfo());
        } else if (kvAdminMessage.getStatus().equals(StatusType.UPDATE_METADATA)) {
            return server.update(kvAdminMessage.getMetadata());
        } else {
            logger.error(String.format("ECSImpl: Invalid message from ECSImpl: %s", kvAdminMessage.toString()));
            response = new KVAdminMessageImpl(StatusType.GENERAL_ERROR);
        }
        return response;
    }

    /**
     *
     * @param kvServerMessage
     * @return
     */
    private KVServerMessageImpl processServerMessage(KVServerMessage kvServerMessage) {
        KVServerMessageImpl response;
        if (kvServerMessage.getStatus().equals(KVServerMessage.StatusType.MOVE_DATA)) {
            return server.insertNewDataToCache(kvServerMessage.getKVPairs());
        } /*else if (kvServerMessage.getStatus().equals(KVServerMessage.StatusType.MOVE_DATA_SUCCESS)) {
            return server.insertNewDataToCache(kvServerMessage.getKVPairs());
        } */else {
            logger.error(String.format("Server: Invalid message from ECSImpl: %s", kvServerMessage.toString()));
            response = new KVServerMessageImpl(KVServerMessage.StatusType.MOVE_DATA_FAILURE);
        }
        return response;
    }


    /**
     * Process of the KVMessage and communication with the cache
     * @param kvMessage KVMessage representation of the client request
     * @return resulting KVMessageImpl
     */
    private KVMessageImpl processMessage(KVMessage kvMessage) {
        if (server.isStopped()) {
            return new KVMessageImpl("", "", KVMessage.StatusType.SERVER_STOPPED);
        }
        if (server.isInitialized()) {
            if (kvMessage.getStatus().equals(KVMessage.StatusType.GET)) {
                // Do the GET
                if (server.getInfo().getServerRange().isIndexInRange(kvMessage.getHash())) {
                    return server.getKvCache().get(kvMessage.getKey());
                }
                else {
                    // TODO: Populate with new metadata
                    return new KVMessageImpl("", "", KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
                }
            } else if (kvMessage.getStatus().equals(KVMessage.StatusType.PUT)) {
                if (server.getInfo().getServerRange().isIndexInRange(kvMessage.getHash())) {
                    if (server.isWriteLocked()) {
                        // Cannot proceed PUT request
                        return new KVMessageImpl("", "", KVMessage.StatusType.SERVER_WRITE_LOCK);
                    } else {
                        return server.getKvCache().put(kvMessage.getKey(), kvMessage.getValue());
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
}
