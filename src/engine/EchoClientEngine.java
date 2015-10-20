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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class EchoClientEngine {
	private String host = "";
	private String hostPort = "";
	private boolean connected;
	InputStream in;
	OutputStream out;
	InetAddress address;
	Socket client;

	private static Logger logger = Logger.getLogger(EchoClientEngine.class);

	public EchoClientEngine() {
		connected = false;
		PropertyConfigurator.configure("conf/log.config");
	}
	
	public void connect(String host, String hostPort) throws CannotConnectException {
		this.host = host;
		this.hostPort = hostPort;
		try {
			address = InetAddress.getByName(host);
			try {
				client = new Socket(address, Integer.parseInt(hostPort));
				in = client.getInputStream();
				BufferedReader inReader = new BufferedReader(new InputStreamReader(in));
				out = client.getOutputStream();
				String initMessage;
				if ((initMessage = inReader.readLine()) != null) {
					System.out.println("EchoClient> " + initMessage);
				}
				connected = true;
			} catch (NumberFormatException e) {
				logger.error("Number Format Exception", e);
				throw new CannotConnectException(ErrorMessages.ERROR_INTERNAL);
			} catch (IOException e) {
				logger.error("Error while connecting to the server.", e);
				throw new CannotConnectException(e.getMessage());
			}
		} catch (UnknownHostException e) {
			logger.error("Server hostname cannot be resolved", e);
			throw new CannotConnectException(ErrorMessages.ERROR_CANNOT_RESOLVE_HOSTNAME);
		}
	}
	
	public boolean isConnected() {
		return this.connected;
	}

	public void send(String msg) throws CannotConnectException {
		byte[] bytes = new StringBuilder(msg).append(Character.toString((char) 13)).toString().getBytes(StandardCharsets.US_ASCII);
		send(bytes);
		logger.info("Message sent from user: " + msg);
		byte[] answer = receive();
        try {
			String msgFromServer = new String(answer, "US-ASCII").trim();
			logger.info("Message received from server: " + msgFromServer);
			System.out.println("EchoClient> " + msgFromServer);
		} catch (UnsupportedEncodingException e) {
			logger.error("Unsupported Encoding in message from server", e);
			throw new CannotConnectException(ErrorMessages.ERROR_INVALID_MESSAGE_FROM_SERVER);
		}
	}
	private void send(byte[] bytes) throws CannotConnectException {
		try {
			Integer messageLength = bytes.length;
			out.write(bytes, 0, messageLength);
			out.flush();
		} catch (UnsupportedEncodingException e) {
			logger.error(e);
			throw new CannotConnectException("Unsupported Encoding in message to be send");
		} catch (IOException e) {
			logger.error(e);
			throw new CannotConnectException("Error while sending the message: " + e.getMessage());
		}
	}
	private byte[] receive() throws CannotConnectException {
		try {
			byte[] answer = new byte[128*1024];
			byte[] buffer = new byte[128*1024]; 
	        Integer count;
	        count = in.read(buffer);
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

	public void logLevel(String level) {
		switch (level) {
		case "ALL":
			logger.setLevel(Level.ALL);
			break;
		case "DEBUG":
			logger.setLevel(Level.DEBUG);
			break;
		case "INFO":
			logger.setLevel(Level.INFO);
			break;
		case "WARN":
			logger.setLevel(Level.WARN);
			break;
		case "ERROR":
			logger.setLevel(Level.ERROR);
			break;
		case "FATAL":
			logger.setLevel(Level.FATAL);
			break;
		case "OFF":
			logger.setLevel(Level.OFF);
			break;
		default:
			break;
		}
		System.out.println("Log status: " + level);
	}
	
	public void closeConnection() {
		// TODO: Print status report too
		if (in != null && out != null && client != null) {
			try {
				in.close();
				out.close();
				client.close();
			} catch (IOException e) {
				logger.error(e);
				System.out.println("Error: " + e.getMessage());
			}
		}
		connected = false;
		host = "";
		hostPort = "";
	}

	public Level getLogLevel() {
		return logger.getLevel();
	}
	
}

