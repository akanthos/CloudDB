package engine;

import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import helpers.CannotConnectException;

public class echoClientEngine {
	private String host;
	private String hostPort;
	private boolean connected;
	InputStream in;
	OutputStream out;
	InetAddress address;
	Socket client;
	private String message = "Get the message!!";
	private static Logger log;

	public echoClientEngine() {
		this.host = "";
		this.hostPort = "";
		connected = false;
//		log = Logger.getLogger(echoClientEngine.class);
//		TODO: Figure out logging 
//		String logDir = "logs/client.log";
//		String pattern = "%d{ISO8601} %-5p [%t] %c: %m%n";
//
//		PatternLayout pLayout = new PatternLayout(pattern);
//		FileAppender fa = new FileAppender(pLayout, logDir, true );
//		log.addAppender(fa);
	}
	
	public void connect(String host, String hostPort) throws CannotConnectException {
		this.host = host;
		this.hostPort = hostPort;
		try {
			address = InetAddress.getByName(host);
			try {
				client = new Socket(address, Integer.parseInt(hostPort));
				in = client.getInputStream();
				out = client.getOutputStream();
//				TODO: Get initial message
//				byte recvByte = in.read();
			} catch (NumberFormatException e) {
				throw new CannotConnectException(e.getMessage());
			} catch (IOException e) {
				throw new CannotConnectException(e.getMessage());
			}
			
		} catch (UnknownHostException e) {
			throw new CannotConnectException(e.getMessage());
		}
		
//		Connection to MSRG Echo server established: /127.0.0.1 / 50000
	}
	
	public boolean isConnected(){
		return this.connected;
	}
	public void send(String[] message) {
		// TODO: Send message and print response
	}
	public void logLevel(String level) {
		// TODO: Do it
		switch (level) {
		case "ALL":
			break;
		case "DEBUG":
			break;
		case "INFO":
			break;
		case "WARN":
			break;
		case "ERROR":
			break;
		case "FATAL":
			break;
		case "OFF":
			break;
		default:
			break;
		}
		System.out.println("Log status: ...");
	}
	public String getMessage() {
		return message;
	}
	
	public void closeConnection() {
		// TODO: Print status report too
		if (in != null && out != null && client != null) {
			try {
				in.close();
				out.close();
				client.close();
			} catch (IOException e) {
				System.out.println("Error: " + e.getMessage());
			}
		}
	}
	
}

