package client;

import app_kvEcs.ServerInfos;
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
public class ServerConnection extends ServerInfos {

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
                logger.error("Number Format Exception", e);
                throw new CannotConnectException(ErrorMessages.ERROR_INTERNAL);
            } catch (IOException e) {
                logger.error("Error while connecting to the server.", e);
                throw new CannotConnectException(e.getMessage());
            }
        }
        catch (UnknownHostException e) {
            logger.error("KVServer hostname cannot be resolved", e);
            throw new CannotConnectException("Hostname cannot be resolved");
        }
    }

    public void closeConnections() {
        try {
            inStream.close();
            outStream.close();
            clientSocket.close();
            clientSocket = null;
        } catch (IOException e) {
            logger.error(String.format("Cannot close connections. Host: %s. Port: %s", getServerIP(), getHostPort()), e);
        } finally {
            isConnected = false;
            setPort(0);
            setServerIP("");
        }
    }

    public boolean isConnected() {
        return isConnected;
    }

    public void setConnected(boolean connected) {
        isConnected = connected;
    }

    public OutputStream getOutStream() {
        return outStream;
    }

    public void setOutStream(OutputStream outStream) {
        this.outStream = outStream;
    }

    public InputStream getInStream() {
        return inStream;
    }

    public void setInStream(InputStream inStream) {
        this.inStream = inStream;
    }
}
