package client;

import common.ServerInfo;
import helpers.CannotConnectException;
import helpers.Constants;
import helpers.ErrorMessages;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Created by sreenath on 13.11.15.
 */
public class ServerConnection extends ServerInfo {

    private boolean isConnected;
    private InputStream inStream;
    private OutputStream outStream;
    private Socket clientSocket;
    private static Logger logger = Logger.getLogger(ServerConnection.class);

    public ServerConnection(String hostname, Integer port) throws CannotConnectException {
        super(hostname, port);
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
        try {
            InetAddress address = InetAddress.getByName(hostname);
            try {
                clientSocket = new Socket(address, port);
                inStream = clientSocket.getInputStream();
                outStream = clientSocket.getOutputStream();
                logger.info("KVServer connection established");
                isConnected = true;
            } catch (NumberFormatException e) {
                logger.error(String.format("Number Format Exception. Server: %s:%s", address, port), e);
                throw new CannotConnectException(ErrorMessages.ERROR_INTERNAL);
            } catch (IOException e) {
                logger.error(String.format("Error while connecting to the server. Server: %s:%s", hostname, port), e);
                throw new CannotConnectException(e.getMessage());
            }
        }
        catch (UnknownHostException e) {
            logger.error("KVServer hostname cannot be resolved", e);
            throw new CannotConnectException("Hostname cannot be resolved");
        }
    }

    public void closeConnections() {
        if (isConnected) {
            try {
                inStream.close();
                outStream.close();
                clientSocket.close();
                clientSocket = null;
            } catch (IOException e) {
                logger.error(String.format("Cannot close connections. Host: %s. Port: %s", getAddress(), getServerPort()), e);
            } finally {
                isConnected = false;
                setServerPort(0);
                setAddress("");
            }
        }
    }

    public OutputStream getOutStream() {
        return outStream;
    }

    public InputStream getInStream() {
        return inStream;
    }
}
