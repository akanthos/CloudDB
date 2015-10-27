package app_kvClient;

import common.utils.Utilities;
import helpers.CannotConnectException;
import helpers.Constants;
import helpers.ErrorMessages;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;

public class KVClient implements Runnable {
    Socket clientSocket;
    int clientNumber;
    InputStream inputStream;
    OutputStream outputStream;
    private static Logger logger = Logger.getLogger(KVClient.class);

    public KVClient(Socket clientSocket, int clientNumber) {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
        this.clientSocket = clientSocket;
        this.clientNumber = clientNumber;
        try {
            inputStream = clientSocket.getInputStream();
            outputStream = clientSocket.getOutputStream();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        String message = "";
        while (message!=null && !message.equalsIgnoreCase(Constants.CLIENT_QUIT_MESSAGE)) {
            try {
                byte[] recvBytes = Utilities.receive(inputStream);
                message = new String(recvBytes, Constants.DEFAULT_ENCODING).trim();
                System.out.println(String.format("Client %d: %s", clientNumber, message));
                // TODO: parse and persist the message

                Utilities.send(recvBytes, outputStream);
            } catch (CannotConnectException e) {
                e.printStackTrace();
                logger.error("Error receiving messages", e);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                logger.error("Unsupported encoding in messages", e);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            inputStream.close();
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info(String.format("Client %d left", clientNumber));
    }
}