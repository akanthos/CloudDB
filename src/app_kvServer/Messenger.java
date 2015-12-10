package app_kvServer;

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
import java.net.UnknownHostException;
import java.util.ArrayList;

/**
 * Created by akanthos on 10.12.15.
 */
public class Messenger {
    private static Logger logger = Logger.getLogger(Messenger.class);

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

//    public void sendToECS(String sourceIP, int replicaNumber) {
//        InputStream inStream = null;
//        OutputStream outStream = null;
//        Socket clientSocket = null;
//        try {
//            /***************************/
//            /* Connect to other server */
//            /***************************/
//
//            InetAddress address = InetAddress.getByName(server.getAddress());
//            clientSocket = new Socket(address, server.getServerPort());
//            inStream = clientSocket.getInputStream();
//            outStream = clientSocket.getOutputStream();
//
//            /*****************************************************/
//            /* Send MOVE_DATA server message to the other server */
//            /*****************************************************/
//
//            KVServerMessageImpl bulkPutMessage = new KVServerMessageImpl(pairsToSend, KVServerMessage.StatusType.MOVE_DATA);
//            Utilities.send(bulkPutMessage, outStream);
//            byte[] bulkPutAnswerBytes = Utilities.receive(inStream);
//            KVServerMessageImpl bulkPutAnswer = (KVServerMessageImpl) Serializer.toObject(bulkPutAnswerBytes);
//            if (bulkPutAnswer.getStatus().equals(KVServerMessage.StatusType.MOVE_DATA_SUCCESS)) {
//            }
//            else {
//            }
//
//        } catch (UnknownHostException e) {
//            logger.error("KVServer hostname cannot be resolved", e);
//            reply = new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_FAILED);
//        } catch (IOException e) {
//            logger.error("Error while connecting to the server for bulk put.", e);
//            reply = new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_FAILED);
//        } catch (CannotConnectException e) {
//            logger.error("Error while connecting to the server.", e);
//            reply = new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_FAILED);
//        } finally {
//            /****************************************/
//            /* Tear down connection to other server */
//            /****************************************/
//            ConnectionHelper.connectionTearDown(inStream, outStream, clientSocket, logger);
//
//        }
//        return reply;
//    }


}
