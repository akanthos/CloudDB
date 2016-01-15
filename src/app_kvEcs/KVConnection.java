package app_kvEcs;

import common.Serializer;
import common.ServerInfo;
import common.messages.*;
import common.utils.Utilities;
import helpers.CannotConnectException;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Responsible for socket communicating with the servers
 * (send ECSInterface commands receives acknowledgments)
 *
 */
public class KVConnection{
    private static Logger logger = Logger.getLogger(KVConnection.class);
    private Socket connection;
    private OutputStream output;
    private InputStream input;
    private boolean connected;
    private ServerInfo server;

    public KVConnection(ServerInfo serverInfo) {
        this.server = serverInfo;
    }

    public ServerInfo getServer() {
        return server;
    }

    public void setServer(ServerInfo server) {
        this.server = server;
    }

    public InputStream getInput() { return input;}

    public OutputStream getOutput() { return output; }

    public void connect() throws IOException {
        try {
            connection = new Socket(server.getAddress(), server.getServerPort());
            output = connection.getOutputStream();
            input = connection.getInputStream();
            connected = true;
            logger.info("CREATED Socket connection with server");
        } catch (IOException ioe) {
            logger.error("Connection could not be established!");
            throw ioe;
        }
    }

    public void sendMessage(KVAdminMessageImpl msg) throws IOException {
        byte[] msgBytes = Serializer.toByteArray(msg);
//        System.out.println(new String(msgBytes,"UTF-8"));
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("Send message to " + connection.getPort() + ":\t '"
                + msg.getStatus() + "' to : " + server.toString());
    }

    public void disconnect() {
        try {
            if (isConnected())
                tearDownConnection();
        } catch (IOException ioe) {
            logger.error("Unable to close connection!");
        }
    }

    private void tearDownConnection() throws IOException {
        if (connection != null) {
            connected = false;
            input.close();
            output.close();
            connection.close();
            connection = null;
        }
    }

    public synchronized boolean isConnected() {
        return connected;
    }


}

