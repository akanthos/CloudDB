package app_kvEcs;

import common.Serializer;
import common.messages.*;
import common.utils.Utilities;
import helpers.CannotConnectException;
import org.apache.log4j.Logger;
import org.junit.internal.runners.statements.Fail;

import java.io.*;
import java.net.ServerSocket;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;


/**
 * Responsible for listening to server
 * failure messages regarding the status of
 * Coordinators
 */
public class FailDetection implements Runnable {

    private static Logger logger = Logger.getLogger(FailDetection.class);
    private int port;
    private boolean running = true;
    private OutputStream out;
    private InputStream in;
    private Socket client;

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public FailDetection(int port){
        this.port = port;
        //initialize the newly created socket server
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            logger.info("Server listening on port " + port);
            logger.debug("Start listening for Failure reports from KVStore Servers.");
            client = serverSocket.accept();
            running = true;
        } catch (IOException e) {
            logger.error("Error. Cannot open server socket for Failure detection.");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
        }

    }

    @Override
    public void run() {
        try {
            in = client.getInputStream();
            out = client.getOutputStream();
            byte[] byteMessage = new byte[0];
            boolean clientConnected = true;
            KVAdminMessage kvAdminMessage;

            while(running && clientConnected){
                try {
                    byteMessage = Utilities.receive(in);
                    if (byteMessage[0] == -1) {
                        clientConnected = false;
                    } else {
                        AbstractMessage abstractMessage = Serializer.toObject(byteMessage);
                        if (abstractMessage.getMessageType().equals(AbstractMessage.MessageType.ECS_MESSAGE)) {
                            kvAdminMessage = (KVAdminMessageImpl) abstractMessage;
                            //if kvAdminMessage.getStatus().equals(KVAdminMessage.StatusType.GENERAL_ERROR);
                            //kvAdminResponse = processAdminMessage(kvAdminMessage);
                            //Utilities.send(kvAdminResponse, outputStream);
                        }
                    }
                } catch (CannotConnectException e) {
                    logger.error("Error! Connection lost!");
                }
            }
        }
        catch (IOException e) {
            logger.error("Connection could not be established.");
        }


    }
}
