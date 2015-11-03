package app_kvClient;

import app_kvServer.KVCache;
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
import java.net.Socket;

/**
 * Logic for handling client requests.
 * This is the runnable class that will be running inside the threadpool when assigned to a thread
 */
public class KVClient implements Runnable {
    Socket clientSocket;
    KVCache kvCache;
    int clientNumber;
    InputStream inputStream;
    OutputStream outputStream;
    private boolean isOpen;
    private static Logger logger = Logger.getLogger(KVClient.class);

    public KVClient(Socket clientSocket, int clientNumber, KVCache kvCache) throws IOException {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
        this.clientSocket = clientSocket;
        this.clientNumber = clientNumber;
        this.kvCache = kvCache;
        try {
            inputStream = clientSocket.getInputStream();
            outputStream = clientSocket.getOutputStream();
            this.isOpen = true;
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
            byte[] byteMessage;
            String stringMessage;
            while (isOpen) {
                try {
                    // Get a new message
                    byteMessage = Utilities.receive(inputStream);

                    if (byteMessage[0] == -1) {
                        isOpen = false;
                    }
                    else {
                        stringMessage = new String(byteMessage, Constants.DEFAULT_ENCODING).trim();
                        kvMessage = extractMessage(stringMessage);
                        // Process the message and do the required backend actions
                        // If it fails, it returns a GENERAL_ERROR KVMessage
                        KVMessageImpl kvResponse = processMessage(kvMessage);
                        // Send appropriate response according to the above backend actions
                        Utilities.send(kvResponse.getMsgBytes(), outputStream);
                    }
                } catch (IOException ioe) {
                    /* connection either terminated by the client or lost due to
                     * network problems*/
                    logger.error("Error! Connection lost!");
                    isOpen = false;
                } catch (Exception e) {
                    logger.error("Unable to parse string message from client");
                    isOpen = false;
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
                }
            }catch(IOException ioe){
                logger.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }

    /**
     * Message Unmarshaller
     * @param messageString message to be unmarshalled to KVMessage type
     * @return resulting KVMessage
     */
    private KVMessage extractMessage(String messageString) {
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
     * Process of the KVMessage and communication with the cache
     * @param kvMessage KVMessage representation of the client request
     * @return resulting KVMessageImpl
     */
    private KVMessageImpl processMessage(KVMessage kvMessage) {
        KVMessageImpl response;
        if (kvMessage.getStatus().equals(KVMessage.StatusType.GET)) {
            // Do the GET
            response = kvCache.get(kvMessage.getKey());
        } else if (kvMessage.getStatus().equals(KVMessage.StatusType.PUT)) {
            // Do the PUT
            response = kvCache.put(kvMessage.getKey(), kvMessage.getValue());
        } else {
            logger.error(String.format("Client: %d. Invalid message from client: %s", clientNumber, kvMessage.toString()));
            response = new KVMessageImpl("", "", KVMessage.StatusType.GENERAL_ERROR);
        }
        return response;
    }

}
