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
    volatile boolean stop;
    private static Logger logger = Logger.getLogger(KVRequestHandler.class);

    public KVRequestHandler(KVConnectionHandler handler, SocketServer server, Socket clientSocket, int clientNumber) throws IOException {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
        this.myHandler = handler;
        this.server = server;
        this.clientSocket = clientSocket;
        this.clientNumber = clientNumber;
        this.stop = false;

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
            while (clientConnected && server.isOpen() && !stop) {
                try {
                    // Get a new message
                    byteMessage = Utilities.receive(inputStream);
                    if (!Thread.currentThread().isInterrupted() || !stop) {

                        if (byteMessage[0] == -1) {
                            clientConnected = false;
                        } else {
                            logger.info(server.getInfo().getID() + " : Received message: " + new String(byteMessage).trim());
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
            if (!stop) {
                ConnectionHelper.connectionTearDown(inputStream, outputStream, clientSocket, logger);
                myHandler.unsubscribe(this);
            }
        }
    }


    /**
     * Process of the KVAdminMessage (message from the ECSInterface) and configure the server
     * accordingly
     * @param kvAdminMessage KVAdminMessage representation of the ECSCore request
     * @return resulting KVAdminMessageImpl
     */
    private KVAdminMessageImpl processAdminMessage(KVAdminMessage kvAdminMessage) {
        KVAdminMessageImpl response;
        if (kvAdminMessage.getStatus().equals(StatusType.INIT)) {
            logger.info("Got INIT message from ECSInterface!!");
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
            return server.removeReplicatedData(kvAdminMessage.getRange());
        } else if (kvAdminMessage.getStatus().equals(StatusType.RESTORE_DATA)) {
            return server.restoreData(kvAdminMessage.getRange());
        } else if (kvAdminMessage.getStatus().equals(StatusType.UPDATE_METADATA)) {
            return server.update(kvAdminMessage.getMetadata());
        } else {
            logger.error(String.format("ECSCore: Invalid message from ECSCore: %s", kvAdminMessage.toString()));
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
            return server.heartbeatReceived(kvServerMessage.getReplicaID(), kvServerMessage.getTimeOfSendingMsg());
        } else {
            logger.error(String.format("Server: Invalid message from ECSCore: %s", kvServerMessage.toString()));
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
        logger.info(server.getInfo().getID() + " : Got client message before initialized ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
        if (!server.isInitialized()) {
            return new KVMessageImpl(KVMessage.StatusType.GENERAL_ERROR);
        }
        // Server is properly initialized
        logger.info(server.getInfo().getID() + " : Got client message before stopped ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
        if (server.isStopped()) {
            return new KVMessageImpl(KVMessage.StatusType.SERVER_STOPPED);
        }
        logger.info(server.getInfo().getID() + " : Got client message ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
//        logger.info("My Address is: " + server.getInfo().getAddress());
//        logger.info("My Port is: " + server.getInfo().getServerPort());
//        logger.info("My Range is: " + server.getInfo().getFromIndex() + ":" + server.getInfo().getToIndex());
        logger.info(server.getInfo().getID() + " : Message: " + kvMessage.getStatus() + ", " + kvMessage.getKey() + " (" + kvMessage.getHash() + ")"
                + " Value: " + kvMessage.getValue() );
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
            logger.info("I am responsible for the key");
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
            }

            else if (kvMessage.getStatus().equals(KVMessage.StatusType.SUBSCRIBE_CHANGE)) {
                return server.subscribeUser(kvMessage.getKey(), new ClientSubscription(clientSocket.getInetAddress(), ClientSubscription.Interest.CHANGE));
            }
            else if (kvMessage.getStatus().equals(KVMessage.StatusType.SUBSCRIBE_DELETE)) {
                return server.subscribeUser(kvMessage.getKey(), new ClientSubscription(clientSocket.getInetAddress(), ClientSubscription.Interest.DELETE));
            }
            else if (kvMessage.getStatus().equals(KVMessage.StatusType.SUBSCRIBE_CHANGE_DELETE)) {
                ClientSubscription client = new ClientSubscription(clientSocket.getInetAddress(), ClientSubscription.Interest.CHANGE);
                client.addInterest(ClientSubscription.Interest.DELETE);
                return server.subscribeUser(kvMessage.getKey(), client);
            }

            else if (kvMessage.getStatus().equals(KVMessage.StatusType.UNSUBSCRIBE_CHANGE)) {
                return server.unsubscribeUser(kvMessage.getKey(), new ClientSubscription(clientSocket.getInetAddress(), ClientSubscription.Interest.CHANGE));
            }
            else if (kvMessage.getStatus().equals(KVMessage.StatusType.UNSUBSCRIBE_DELETE)) {
                return server.unsubscribeUser(kvMessage.getKey(), new ClientSubscription(clientSocket.getInetAddress(), ClientSubscription.Interest.DELETE));
            }
            else if (kvMessage.getStatus().equals(KVMessage.StatusType.UNSUBSCRIBE_CHANGE_DELETE)) {
                ClientSubscription client = new ClientSubscription(clientSocket.getInetAddress(), ClientSubscription.Interest.CHANGE);
                client.addInterest(ClientSubscription.Interest.DELETE);
                return server.unsubscribeUser(kvMessage.getKey(), client);
            }

            else {
                logger.error(String.format("Client: %d. Invalid message from client: %s", clientNumber, kvMessage.toString()));
                return new KVMessageImpl(KVMessage.StatusType.GENERAL_ERROR);
            }
        }
        else {
            logger.info(server.getInfo().getID()+" : I am NOT responsible for the key... Trying replicated data...");
            if (kvMessage.getStatus().equals(KVMessage.StatusType.GET) &&
                server.getReplicationHandler().isResponsibleForHash(kvMessage)) {
                logger.info(server.getInfo().getID()+" : Found key in replicated data");
                return server.getReplicationHandler().get(kvMessage.getKey());
            }

            // Server not responsible for key
            return new KVMessageImpl(server.getMetadata(), KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
        }
    }
}
