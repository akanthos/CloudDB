package app_kvServer;

import app_kvServer.replication.Coordinator;
import common.Serializer;
import common.ServerInfo;
import common.messages.*;
import common.utils.Utilities;
import helpers.CannotConnectException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;

/**
 * Class to encapsulate the occasional messaging functionality
 * of the server to other server's and the ECS (for failure reporting)
 */
public class Messenger {
    private SocketServer server;
    private static Logger logger = Logger.getLogger(Messenger.class);

    public Messenger(SocketServer server) {
        this.server = server;
    }

    /**
     * MOVE_DATA admin operation implementation
     * @param pairsToSend the key-value pairs to send to the other server
     * @param server the server to send the data to
     * @return the admin message response (to the admin command MOVE_DATA)
     */
    public KVAdminMessageImpl sendToServer(ArrayList<KVPair> pairsToSend, ServerInfo server) {
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
            ConnectionHelper.connectionTearDown(inStream, outStream, clientSocket, logger);
        }
        return reply;
    }

    /**
     * Replicates a list of key-value pairs to another server
     * @param pairsToSend the pairs to replicate
     * @param server the server to replicate the pairs to
     * @return the admin message response (to the REPLICATE_DATA command)
     */
    public KVAdminMessageImpl replicateToServer(ArrayList<KVPair> pairsToSend, ServerInfo server) {
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
            /* Send REPLICATE server message to the other server */
            /*****************************************************/

            KVServerMessageImpl bulkReplicateMessage = new KVServerMessageImpl(pairsToSend, KVServerMessage.StatusType.REPLICATE);
            Utilities.send(bulkReplicateMessage, outStream);
            byte[] bulkReplicateAnswerBytes = Utilities.receive(inStream);
            KVServerMessageImpl bulkPutAnswer = (KVServerMessageImpl) Serializer.toObject(bulkReplicateAnswerBytes);
            if (bulkPutAnswer.getStatus().equals(KVServerMessage.StatusType.REPLICATE_SUCCESS)) {
                reply = new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_SUCCESS);
            }
            else {
                reply = new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_FAILED);
            }

        } catch (UnknownHostException e) {
            logger.error("KVServer hostname cannot be resolved", e);
            reply = new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_FAILED);
        } catch (IOException e) {
            logger.error("Error while connecting to the server for replication.", e);
            reply = new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_FAILED);
        } catch (CannotConnectException e) {
            logger.error("Error while connecting to the server.", e);
            reply = new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_FAILED);
        } finally {
            /****************************************/
            /* Tear down connection to other server */
            /****************************************/
            ConnectionHelper.connectionTearDown(inStream, outStream, clientSocket, logger);
        }
        return reply;
    }

    /**
     * Reports to the ECSInterface that a specific server has failed
     * @param failedCoordinator the information about the failed server
     * @param ecsInfo the ecs contact information
     */
    public void reportFailureToECS(ServerInfo failedCoordinator, ServerInfo ecsInfo) {
        logger.info(server.getInfo().getID() + " : Reporting failure to ECS for server: " + failedCoordinator.getID());
        logger.info("ECS data" + ecsInfo.getAddress() + ":" + ecsInfo.getServerPort());
        InputStream inStream = null;
        OutputStream outStream = null;
        Socket clientSocket = null;
        try {
            /***************************/
            /*     Connect to ECSInterface      */
            /***************************/

            InetAddress address = InetAddress.getByName(ecsInfo.getAddress());
            clientSocket = new Socket(address, ecsInfo.getServerPort());
            inStream = clientSocket.getInputStream();
            outStream = clientSocket.getOutputStream();

            /*****************************************************/
            /*       Send SERVER_FAILURE message to the ECSInterface      */
            /*****************************************************/

            KVAdminMessageImpl failureMessage = new KVAdminMessageImpl(KVAdminMessage.StatusType.SERVER_FAILURE, failedCoordinator);
            Utilities.send(failureMessage, outStream);

        } catch (UnknownHostException e) {
            logger.error("ECSInterface hostname cannot be resolved", e);
        } catch (IOException e) {
            logger.error("Error while sending failure message to ECSInterface", e);
        } catch (CannotConnectException e) {
            logger.error("Error while connecting to the ECSInterface", e);
        } finally {
            /****************************************/
            /* Tear down connection to other server */
            /****************************************/
            ConnectionHelper.connectionTearDown(inStream, outStream, clientSocket, logger);
        }
    }

    /**
     * Sends a heartbeat response to another server (a coordinator)
     * @param serverInfo the target server info
     * @throws SocketTimeoutException if the server does not respond for a certain timeout
     *                or a connection cannot be established, a timeout exception is raised
     */
    public void askHeartbeatFromServer(ServerInfo serverInfo) throws SocketTimeoutException {
        InputStream inStream = null;
        OutputStream outStream = null;
        Socket clientSocket = null;
        try {
            /***************************/
            /* Connect to other server */
            /***************************/

            InetAddress address = InetAddress.getByName(serverInfo.getAddress());
            clientSocket = new Socket(address, serverInfo.getServerPort());
            inStream = clientSocket.getInputStream();
            outStream = clientSocket.getOutputStream();

            /*****************************************************/
            /*     Send HEARTBEAT message to the other server    */
            /*****************************************************/

            KVServerMessageImpl heartbeatMessage = new KVServerMessageImpl(server.getInfo().getID(), new Date(), KVServerMessage.StatusType.HEARTBEAT);
            Utilities.send(heartbeatMessage, outStream);

            clientSocket.setSoTimeout(5000); // 5 seconds timeout
            logger.info(server.getInfo().getID() + " : Waiting for heartbeat from " + serverInfo.getID());
            byte[] answerBytes = Utilities.receive(inStream);
            if (answerBytes[0] == -1) {
                logger.info("Received HEARTBEAT response -1 !!!!!!!!!!!!!");
                throw new SocketTimeoutException();
            }
            KVServerMessageImpl heartbeatAnswer = (KVServerMessageImpl) Serializer.toObject(answerBytes);
            if (!heartbeatAnswer.getStatus().equals(KVServerMessage.StatusType.HEARTBEAT_RESPONSE)) {
                logger.info("Received HEARTBEAT response =/= HEARTBEAT_RESPONSE !!!!!!!!!!!!!");
                throw new SocketTimeoutException();
            }


        } catch (SocketTimeoutException e) {
                throw e;
        } catch (UnknownHostException e) {
            logger.error("KVServer hostname cannot be resolved", e);
        } catch (IOException e) {
            logger.error("Error while connecting to the server for heartbeat", e);
            throw new SocketTimeoutException();
        } catch (CannotConnectException e) {
            logger.error("Error while connecting to the server.", e);
            throw new SocketTimeoutException();
        } finally {
            /****************************************/
            /* Tear down connection to other server */
            /****************************************/
            ConnectionHelper.connectionTearDown(inStream, outStream, clientSocket, logger);
        }
    }

    /**
     * Gossip some key-value updates to a replica
     * Used by the GOSSIP server command
     * @param replicaInfo the replica to send the data to
     * @param kvPairs the data to gossip to the replica
     * @return a status boolean for successful gossiping.
     *         True if gossip was successful, false otherwise
     */
    public boolean gossipToReplica(ServerInfo replicaInfo, ArrayList<KVPair> kvPairs) {
        InputStream inStream = null;
        OutputStream outStream = null;
        Socket clientSocket = null;
        try {
            /***************************/
            /* Connect to other server */
            /***************************/

            InetAddress address = InetAddress.getByName(replicaInfo.getAddress());
            clientSocket = new Socket(address, replicaInfo.getServerPort());
            inStream = clientSocket.getInputStream();
            outStream = clientSocket.getOutputStream();

            /********************************************************/
            /*    Send GOSSIP_MESSAGE message to the other server   */
            /********************************************************/
            logger.info(server.getInfo().getID() + " : Sending the gossip (messenger) to " + replicaInfo.getID());
            KVServerMessageImpl gossipMessage = new KVServerMessageImpl(kvPairs, KVServerMessage.StatusType.GOSSIP);
            Utilities.send(gossipMessage, outStream);
            logger.info(server.getInfo().getID() + " : Waiting for GOSSIP_SUCCESS from " + replicaInfo.getID());

            byte[] gossipMessageAnswerBytes = Utilities.receive(inStream);
            KVServerMessageImpl gossipMessageAnswer = (KVServerMessageImpl) Serializer.toObject(gossipMessageAnswerBytes);
            if (gossipMessageAnswer.getStatus().equals(KVServerMessage.StatusType.GOSSIP_SUCCESS)) {
                return true;
            }
            else
                return false;

        } catch (UnknownHostException e) {
            logger.error("KVServer hostname cannot be resolved", e);
            return false;
        } catch (IOException e) {
            logger.error("Error while connecting to the server for gossiping", e);
            return false;
        } catch (CannotConnectException e) {
            logger.error("Error while connecting to the server.", e);
            return false;
        } finally {
            /****************************************/
            /* Tear down connection to other server */
            /****************************************/
            ConnectionHelper.connectionTearDown(inStream, outStream, clientSocket, logger);
        }
    }


}
