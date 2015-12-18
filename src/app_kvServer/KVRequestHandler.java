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
            byte[] byteMessage = new byte[0];
            boolean clientConnected = true;
            while (clientConnected && server.isOpen()) {
                try {
                    // Get a new message
                    logger.error("\n\n########################################\n\n");
                    byteMessage = Utilities.receive(inputStream);
                    if (!Thread.currentThread().isInterrupted()) {

                        if (byteMessage[0] == -1) {
                            clientConnected = false;
                        } else {
                            AbstractMessage abstractMessage = Serializer.toObject(byteMessage);
                            if (abstractMessage.getMessageType().equals(AbstractMessage.MessageType.CLIENT_MESSAGE)) {

                                kvMessage = (KVMessageImpl) abstractMessage;


                                kvResponse = processMessage(kvMessage);
                                Utilities.send(kvResponse, outputStream);
                            } else if (abstractMessage.getMessageType().equals(AbstractMessage.MessageType.ECS_MESSAGE)) {
                                server.registerECS(clientSocket.getInetAddress());
                                kvAdminMessage = (KVAdminMessageImpl) abstractMessage;
                                kvAdminResponse = processAdminMessage(kvAdminMessage);
                                Utilities.send(kvAdminResponse, outputStream);
                            } else if (abstractMessage.getMessageType().equals(AbstractMessage.MessageType.SERVER_MESSAGE)) {
                                kvServerMessage = (KVServerMessageImpl) abstractMessage;
                                kvServerResponse = processServerMessage(kvServerMessage);
                                if (kvServerResponse != null) {
                                    Utilities.send(kvServerResponse, outputStream);
                                }
                            } else {
                                Utilities.send(new KVMessageImpl(KVMessage.StatusType.GENERAL_ERROR), outputStream);
                            }
                        }
                    }
                } catch (IOException ioe) {
                    /* connection either terminated by the client or lost due to
                     * network problems*/
                    logger.error("Error! Connection lost!");
                    clientConnected = false;
                } catch (Exception e) {
                    logger.error("Unable to parse string "+ new String(byteMessage,"UTF-8") +" message from client" +e);
                    clientConnected = false;
                }
            }

        } catch (Exception e) {
            logger.error(e);
        } finally {
            ConnectionHelper.connectionTearDown(inputStream, outputStream, clientSocket, logger);
        }
    }

    /**
     * Process of the KVAdminMessage (message from the ECS) and configure the server
     * accordingly
     * @param kvAdminMessage KVAdminMessage representation of the ECSImpl request
     * @return resulting KVAdminMessageImpl
     */
    private KVAdminMessageImpl processAdminMessage(KVAdminMessage kvAdminMessage) {
        KVAdminMessageImpl response;
        if (kvAdminMessage.getStatus().equals(StatusType.INIT)) {
            logger.info("Got INIT message from ECS!!");
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
        } else if (kvAdminMessage.getStatus().equals(StatusType.REPLICATE_DATA)) {
            return server.replicateData(kvAdminMessage.getRange(), kvAdminMessage.getServerInfo());
        } else if (kvAdminMessage.getStatus().equals(StatusType.REMOVE_DATA)) {
            return server.removeReplicatedData(kvAdminMessage.getRange(), kvAdminMessage.getServerInfo());
        } else if (kvAdminMessage.getStatus().equals(StatusType.RESTORE_DATA)) {
            return server.restoreData(kvAdminMessage.getRange(), kvAdminMessage.getServerInfo());
        } else if (kvAdminMessage.getStatus().equals(StatusType.UPDATE_METADATA)) {
            return server.update(kvAdminMessage.getMetadata());
        } else {
            logger.error(String.format("ECSImpl: Invalid message from ECSImpl: %s", kvAdminMessage.toString()));
            response = new KVAdminMessageImpl(StatusType.GENERAL_ERROR);
        }
        return response;
    }

    /**
     * Process of the KVServerMessage (message from another server in the ring)
     * and configure the server accordingly
     * @param kvServerMessage
     * @return
     */
    private KVServerMessageImpl processServerMessage(KVServerMessage kvServerMessage) {
        KVServerMessageImpl response;
        if (kvServerMessage.getStatus().equals(KVServerMessage.StatusType.MOVE_DATA)) {
            return server.insertNewDataToCache(kvServerMessage.getKVPairs());
        } else if (kvServerMessage.getStatus().equals(KVServerMessage.StatusType.REPLICATE)) {
            return server.newReplicatedData(kvServerMessage.getKVPairs());
        } else if (kvServerMessage.getStatus().equals(KVServerMessage.StatusType.GOSSIP)) {
            return server.updateReplicatedData(kvServerMessage.getKVPairs());
        } else if (kvServerMessage.getStatus().equals(KVServerMessage.StatusType.HEARTBEAT)) {
            server.heartbeatReceived(kvServerMessage.getCoordinatorID(), kvServerMessage.getTimeOfSendingMsg());
            return null;
        } else {
            logger.error(String.format("Server: Invalid message from ECSImpl: %s", kvServerMessage.toString()));
            response = new KVServerMessageImpl(KVServerMessage.StatusType.GENERAL_ERROR);
        }
        return response;
    }


    /**
     * Process of the KVMessage and communication with the cache
     * @param kvMessage KVMessage representation of the client request
     * @return resulting KVMessageImpl
     */
    private KVMessageImpl processMessage(KVMessage kvMessage) {
        if (!server.isInitialized()) {
            return new KVMessageImpl(KVMessage.StatusType.GENERAL_ERROR);
        }
        // Server is properly initialized
        if (server.isStopped()) {
            return new KVMessageImpl(KVMessage.StatusType.SERVER_STOPPED);
        }
//        logger.info("Got client message");
//        logger.info("My Address is: " + server.getInfo().getAddress());
//        logger.info("My Port is: " + server.getInfo().getServerPort());
//        logger.info("My Range is: " + server.getInfo().getFromIndex() + ":" + server.getInfo().getToIndex());
//        logger.info("Message: " + kvMessage.getStatus() + ", " + kvMessage.getKey() + " (" + kvMessage.getHash() + ")"
//                + " Value: " + kvMessage.getValue() );
        if (server.getInfo().getServerRange().isIndexInRange(kvMessage.getHash())) {
            logger.info("Index is ours!");
        }
        else {
            logger.info("Index is NOT ours!");
        }
        // Server is not stopped
        logger.info("Got key " + kvMessage.getKey() + " with Hash " + kvMessage.getHash() +".My Range is " + server.getInfo().getFromIndex() + ":" + server.getInfo().getToIndex());
        if (server.getInfo().getServerRange().isIndexInRange(kvMessage.getHash())) {
            // Server IS responsible for key
            if (kvMessage.getStatus().equals(KVMessage.StatusType.GET)) {
                KVMessageImpl answer = server.getKvCache().get(kvMessage.getKey());
                logger.info("Answer: " + answer.getStatus() + ", " + answer.getKey() + " (" + answer.getHash() + ")"
                        + " Value: " + answer.getValue() );
                // Do the GET
                return answer;
            } else if (kvMessage.getStatus().equals(KVMessage.StatusType.PUT)) {
                // Check if WRITE_LOCKED
                if (server.isWriteLocked()) {
                    // Cannot proceed PUT request
                    return new KVMessageImpl(KVMessage.StatusType.SERVER_WRITE_LOCK);
                } else {
                    // Do the PUT
                    KVMessageImpl response = server.getKvCache().put(kvMessage.getKey(), kvMessage.getValue());
                    // TODO: Call enqueuePutEvent to replicationHandler, server.getReplicationHandler()
                    return response;
                }
            } else {
                logger.error(String.format("Client: %d. Invalid message from client: %s", clientNumber, kvMessage.toString()));
                return new KVMessageImpl(KVMessage.StatusType.GENERAL_ERROR);
            }
        }
        else {
            if (kvMessage.getStatus().equals(KVMessage.StatusType.GET) &&
                server.getReplicationHandler().isResponsibleForHash(kvMessage)) {
                return server.getReplicationHandler().get(kvMessage.getKey());
            }

            // Server not responsible for key
            return new KVMessageImpl(server.getMetadata(), KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
        }
    }
}
