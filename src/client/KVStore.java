package client;


import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import common.utils.Utilities;
import helpers.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

public class KVStore implements KVCommInterface {

	String host = "";
	int port;
    boolean isConnected;
    InputStream inStream;
    OutputStream outStream;
    Socket clientSocket;
    private static Logger logger = Logger.getLogger(KVStore.class);

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
        host = address;
        this.port = port;
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
	}
	
	@Override
	public void connect() throws Exception {
        try {
            InetAddress address = InetAddress.getByName(host);
            try {
                clientSocket = new Socket(address, port);
                inStream = clientSocket.getInputStream();
                outStream = clientSocket.getOutputStream();
                System.out.println("Server connection established");
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
            logger.error("Server hostname cannot be resolved", e);
            throw new CannotConnectException(ErrorMessages.ERROR_CANNOT_RESOLVE_HOSTNAME);
        }
		
	}

	@Override
	public void disconnect() {
        if (!isConnected) {
            System.out.println("You can't disconnect. You are not connected");
            System.out.println("Try the <connect> command first");
        } else {
            try {
                inStream.close();
                outStream.close();
                clientSocket.close();
                logger.info("Connection with server: " +host+ " terminated");
            } catch (IOException e) {
                logger.error(e);
                System.out.println("Error: " + e.getMessage());
            }
        }
        isConnected = false;
        host = "";
        port = 0;
	}

    /**
     * Puts an entry into the server.
     *
     * @param key
     *            the key that identifies the given value.
     * @param value
     *            the value that is indexed by the given key.
     * @return
     * @throws Exception
     */
	@Override
	public KVMessage put(String key, String value) throws Exception {
        KVMessageImpl kvMessage;
        kvMessage = new KVMessageImpl(key, value, KVMessage.StatusType.PUT);
        try {
            String response = send(kvMessage.toString());
            KVMessageImpl kvMessageFromServer = new KVMessageImpl(response);
            return kvMessageFromServer;
        } catch (CannotConnectException e) {
            kvMessage.setStatus(KVMessage.StatusType.PUT_ERROR);
            logger.error(e);
        }
        return kvMessage;
	}

    /**
     * Gets an entry from the server.
     *
     * @param key
     *            the key that identifies the value.
     * @return
     * @throws Exception
     */
	@Override
	public KVMessage get(String key) throws Exception {
        KVMessageImpl kvMessage;
        kvMessage = new KVMessageImpl(key, "", KVMessage.StatusType.GET);
        try {
            String response = send(kvMessage.toString());
            KVMessageImpl kvMessageFromServer = new KVMessageImpl(response);
            return kvMessageFromServer;
        } catch (CannotConnectException e) {
            kvMessage.setStatus(KVMessage.StatusType.GET_ERROR);
            logger.error(e);
        }
        return kvMessage;
	}

    /**
     * This function sends a message to the server using the established connection.
     *
     * @param msg
     * @throws CannotConnectException
     */
    public String send(String msg) throws CannotConnectException, IOException {
        Utilities.send(msg, outStream);
        byte[] answer = Utilities.receive(inStream);
        System.out.println("I am the client and received message SIZE: " + answer.length);
        try {
            String msgFromServer = new String(answer, "US-ASCII").trim();
            logger.info("Message received from server: " + msgFromServer);
            System.out.println("EchoClient> " + msgFromServer);
            return msgFromServer;
        } catch (UnsupportedEncodingException e) {
            logger.error("Unsupported Encoding in message from server", e);
            throw new CannotConnectException(ErrorMessages.ERROR_INVALID_MESSAGE_FROM_SERVER);
        }
    }

    /**
     * This function sets the loglevel for the logger object bassed on the argument passed to it.
     *
     * @param level Logging Level defined by the client using the CLI
     */
    public void logLevel(String level) {
        try {
            LogLevels currentLevel = LogLevels.valueOf(level.toUpperCase());
            switch (currentLevel) {
                case ALL:
                    logger.setLevel(Level.ALL);
                    logger.info("Loglevel changed to: ALL");
                    break;
                case DEBUG:
                    logger.setLevel(Level.DEBUG);
                    logger.info("Loglevel changed to: DEBUG");
                    break;
                case INFO:
                    logger.setLevel(Level.INFO);
                    logger.info("Loglevel changed to: INFO");
                    break;
                case WARN:
                    logger.setLevel(Level.WARN);
                    logger.info("Loglevel changed to: WARN");
                    break;
                case ERROR:
                    logger.setLevel(Level.ERROR);
                    logger.info("Loglevel changed to: ERROR");
                    break;
                case FATAL:
                    logger.setLevel(Level.FATAL);
                    logger.info("Loglevel changed to: FATAL");
                    break;
                case OFF:
                    logger.setLevel(Level.OFF);
                    logger.info("Loglevel changed to: OFF");
                    break;
                default:
                    break;
            }
            System.out.println("Log status: " + level);
        } catch (IllegalArgumentException e) {
            System.out.println("Please give a valid log level. Options:\n"
                    + "ALL, DEBUG, INFO, WARN, ERROR, FATAL, OFF");
        }
    }

    public boolean isConnected() {
        return isConnected;
    }

    public void setIsConnected(boolean isConnected) {
        this.isConnected = isConnected;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Level getLogLevel() {
        return logger.getLevel();
    }
}
