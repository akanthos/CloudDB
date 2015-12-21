package app_kvEcs;

import common.Serializer;
import common.messages.*;
import common.utils.Utilities;
import helpers.CannotConnectException;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.ServerSocket;
import java.net.BindException;
import java.net.Socket;


/**
 * Responsible for listening to server
 * failure messages regarding the status of
 * Coordinators
 */
public class FailDetection implements Runnable {

    private static Logger logger = Logger.getLogger(FailDetection.class);
    private int port;
    private boolean running;
    private OutputStream out;
    private InputStream in;
    private ECScm ecs;
    Socket clientSocket;

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public FailDetection(int port, Socket SSocket, ECScm ecsServer){
        this.port = port;
        this.ecs = ecsServer;
        clientSocket = SSocket;
    }

    @Override
    public void run() {
        try {
            running = true;
            boolean clientConnected = true;
            in = clientSocket.getInputStream();
            out = clientSocket.getOutputStream();
            byte[] byteMessage = new byte[0];
            KVAdminMessageImpl kvAdminMessage;

            while(running && clientConnected){
                try {
                    byteMessage = Utilities.receive(in);
                    if (byteMessage[0] == -1) {
                        clientConnected = false;
                    }
                    else {
                        AbstractMessage abstractMessage = Serializer.toObject(byteMessage);
                        if (abstractMessage.getMessageType().equals(AbstractMessage.MessageType.ECS_MESSAGE)) {
                            kvAdminMessage = (KVAdminMessageImpl) abstractMessage;
                            if (kvAdminMessage.getStatus().equals(KVAdminMessage.StatusType.SERVER_FAILURE)){
                                ecs.handleFailure(kvAdminMessage.getFailedServerInfo());
                            }
                            //kvAdminResponse = processAdminMessage(kvAdminMessage);
                            //Utilities.send(kvAdminResponse, outputStream);
                        }
                    }
                } catch (CannotConnectException e) {
                    logger.error("Error! Connection lost!");
                    running = false;
                }
            }
        }
        catch (IOException e) {
            logger.error("Connection could not be established.");
        }
    }
}
