package app_kvServer;

import common.Serializer;
import common.messages.KVAdminMessage;
import common.messages.KVAdminMessageImpl;
import common.messages.KVServerMessage;
import common.messages.KVServerMessageImpl;
import common.utils.Utilities;
import helpers.CannotConnectException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Created by akanthos on 10.12.15.
 */
public class ConnectionHelper {

    public static InOutClientPack connectionSetup(String serverAddress, Integer serverPort, Logger logger, KVAdminMessageImpl failureMessage) {
        // new KVAdminMessageImpl(KVAdminMessage.StatusType.OPERATION_FAILED)
        KVAdminMessageImpl reply;
        InputStream inStream;
        OutputStream outStream;
        Socket clientSocket;
        try {
            /***************************/
            /* Connect to other server */
            /***************************/

            InetAddress address = InetAddress.getByName(serverAddress);
            clientSocket = new Socket(address, serverPort);
            inStream = clientSocket.getInputStream();
            outStream = clientSocket.getOutputStream();

            /*****************************************************/
            /* Send MOVE_DATA server message to the other server */
            /*****************************************************/
            return new InOutClientPack(inStream, outStream, clientSocket);


        } catch (UnknownHostException e) {
            logger.error("KVServer hostname cannot be resolved", e);
            return null;
        } catch (IOException e) {
            logger.error("Error while connecting to the server.", e);
            return null;
        }
    }

    public static void connectionTearDown(InputStream inStream, OutputStream outStream, Socket clientSocket, Logger logger) {
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


}

