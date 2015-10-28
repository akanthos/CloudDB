package app_kvClient;

import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import common.utils.Utilities;
import helpers.CannotConnectException;
import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;

/**
 * Logic for handling client requests.
 * This is the runnable class that will be running inside the threadpool when assigned to a thread
 */
public class KVClient implements Runnable {
    Socket clientSocket;
    int clientNumber;
    InputStream inputStream;
    OutputStream outputStream;
    private static Logger logger = Logger.getLogger(KVClient.class);

    public KVClient(Socket clientSocket, int clientNumber) throws IOException {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
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

    @Override
    public void run() {
        String message = "";
        String msgForClient = "";
        // TODO: Infinite Loop? Seriously?
        while (true) {
            try {
                byte[] recvBytes = Utilities.receive(inputStream);
                message = new String(recvBytes, Constants.DEFAULT_ENCODING).trim();
                if (message==null || message.equalsIgnoreCase(Constants.CLIENT_QUIT_MESSAGE)) {
                    break;
                }
                KVMessage kvMessage = extractMessage(message);
                processMessage(kvMessage);
                logger.debug(String.format("Client: %d. Processed message: %s", clientNumber, kvMessage.toString()));
                msgForClient = kvMessage.toString();
            } catch (CannotConnectException e) {
                logger.error(String.format("Client: %d. Error receiving messages", clientNumber), e);
                msgForClient = "Could not receive message properly.";
            } catch (UnsupportedEncodingException e) {
                logger.error(String.format("Client: %d. Unsupported encoding in messages", clientNumber), e);
                msgForClient = "Unsupported encoding in messages";
            } catch (IOException e) {
                logger.error(String.format("Client: %d. IOException while sending and receiving messages", clientNumber), e);
                msgForClient = "Communication went wrong";
            } finally {
                sendMsgToClient(msgForClient);
            }
        }
        try {
            inputStream.close();
            outputStream.close();
        } catch (IOException e) {
            logger.error(String.format("Client: %d. Unable to close streams", clientNumber), e);
        }
        logger.info(String.format("Client %d left", clientNumber));
    }

    private KVMessage extractMessage(String messageString) {
        KVMessage kvMessage = null;
        try {
            kvMessage = new KVMessageImpl(messageString);
        } catch (Exception e) {
            logger.error(String.format("Unable to process message from client %d. Message: %s", clientNumber, messageString), e);
            kvMessage.setStatus(KVMessage.StatusType.GENERAL_ERROR);
        }
        return kvMessage;
    }

    private void processMessage(KVMessage kvMessage) {
        if (kvMessage.getStatus().equals(KVMessage.StatusType.GET)) {
            // Do the GET
        } else if (kvMessage.getStatus().equals(KVMessage.StatusType.PUT)) {
            // Do the PUT
        } else {
            logger.error(String.format("Client: %d. Invalid message from client: %s", clientNumber, kvMessage.toString()));
            kvMessage.setStatus(KVMessage.StatusType.GENERAL_ERROR);
        }
    }

    private void sendMsgToClient(String message) {
        try {
            Utilities.send(message, outputStream);
        } catch (CannotConnectException e) {
            logger.error(String.format("Client: %d. Unable to send message to client. Message: %s", clientNumber, message), e);
        }
    }
}