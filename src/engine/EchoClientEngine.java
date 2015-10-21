package engine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import helpers.CannotConnectException;
import helpers.ErrorMessages;
import helpers.LogLevels;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * This class houses the main connection logic.
 */
public class EchoClientEngine implements ClientConnection{
	
	private String host = "";
	private String hostPort = "";
	private boolean isConnected;
	InputStream inStream;
	OutputStream outStream;
	InetAddress address;
	Socket clientSocket;
	private static Logger logger = Logger.getLogger(EchoClientEngine.class);

	/**
     * Constructor Takes no arguments
     */
	public EchoClientEngine() {
		isConnected = false;
		PropertyConfigurator.configure("conf/log.config");
	}

	// Getters and setters that are used.

	public Level getLogLevel() {
		return logger.getLevel();
	}

	public void setHostPort(String hostPort) {
		this.hostPort = hostPort;
	}

	public void setHost(String host) {
		this.host = host;
	}
	
	/**
	 * The connect function. Connects to a given host and port and initialises the input and output streams.
	 *
    * @param host The server/host the client connects to
    * @param hostPort The port of the host
    * @throws CannotConnectException In case of connection Failures
    */
	public void connect(String host, String hostPort) throws CannotConnectException {
		this.setHost(host);
		this.setHostPort(hostPort);
		try {
			address = InetAddress.getByName(host);
			try {
				clientSocket = new Socket(address, Integer.parseInt(hostPort));
				inStream = clientSocket.getInputStream();
				BufferedReader inReader = new BufferedReader(new InputStreamReader(inStream));
				outStream = clientSocket.getOutputStream();
				String initMessage;
				if ((initMessage = inReader.readLine()) != null) {
					logger.info(initMessage);
					System.out.println("EchoClient> " + initMessage);
				}
				isConnected = true;
			} 
			catch (NumberFormatException e) {
				logger.error("Number Format Exception", e);
				throw new CannotConnectException(ErrorMessages.ERROR_INTERNAL);
			} 
			catch (IOException e) {
				logger.error("Error while connecting to the server.", e);
				throw new CannotConnectException(e.getMessage());
			}
		} 
		catch (UnknownHostException e) {
			logger.error("Server hostname cannot be resolved", e);
			throw new CannotConnectException(ErrorMessages.ERROR_CANNOT_RESOLVE_HOSTNAME);
		}
	}
	
	/**
    * @return true if client connected else false
    */
	public boolean isConnected() {
		return this.isConnected;
	}

	/**
	 * This function sends a message to the server using the established connection.
	 *
	 * @param msg
	 * @throws CannotConnectException
	 */
	public void send(String msg) throws CannotConnectException {
		byte[] bytes = new StringBuilder(msg).append(Character.toString((char) 13)).toString().getBytes(StandardCharsets.US_ASCII);
		send(bytes);
		logger.info("Message sent from user: " + msg);
		byte[] answer = receive();
		
        try {
			String msgFromServer = new String(answer, "US-ASCII").trim();
			logger.info("Message received from server: " + msgFromServer);
			System.out.println("EchoClient> " + msgFromServer);
		} 
        catch (UnsupportedEncodingException e) {
			logger.error("Unsupported Encoding in message from server", e);
			throw new CannotConnectException(ErrorMessages.ERROR_INVALID_MESSAGE_FROM_SERVER);
		}
	}

	/**
	 * Helper function to send the bytes over the connection.
	 *
	 * @param bytes: the message bytes to be send.
	 * @throws CannotConnectException
	 */
	public void send(byte[] bytes) throws CannotConnectException {
		try {
			Integer messageLength = bytes.length;
			outStream.write(bytes, 0, messageLength);
			outStream.flush();
		} catch (UnsupportedEncodingException e) {
			logger.error(e);
			throw new CannotConnectException("Unsupported Encoding in message to be send");
		} catch (IOException e) {
			logger.error(e);
			throw new CannotConnectException("Error while sending the message: " + e.getMessage());
		}
	}

	/**
	 * Receives an array bytes over the connection.
	 *
	 * @return
	 * @throws CannotConnectException
	 */
	public byte[] receive() throws CannotConnectException {
		try {
			byte[] answer = new byte[128*1024];
			byte[] buffer = new byte[128*1024]; 
	        Integer count;
	        count = inStream.read(buffer);
	        byte[] finalAnswer = new byte[count];
	        System.arraycopy(answer, 0, finalAnswer, 0, count);
	        return buffer;
		} catch (UnsupportedEncodingException e) {
			logger.error(e);
			throw new CannotConnectException(ErrorMessages.ERROR_INVALID_MESSAGE_FROM_SERVER);
		} catch (IOException e) {
			logger.error(e);
			throw new CannotConnectException("Error while receiving the message: " + e.getMessage());
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
	
	/**
     * close connection with the server
     */
    public void closeConnection() {
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
        setHost("");
        setHostPort("");
    }
}

