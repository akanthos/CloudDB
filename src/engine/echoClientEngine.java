package engine;

import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.spi.LoggerContextFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

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
	
//	private final static Logger log = Logger.getLogger(echoClientEngine.class);

	public echoClientEngine() {
		host = "";
		hostPort = "";
		connected = false;
//		log =   //Logger.getLogger(echoClientEngine.class);
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
				BufferedReader inReader = new BufferedReader(new InputStreamReader(in));
				out = client.getOutputStream();
				String initMessage;
				if ((initMessage = inReader.readLine()) != null) {
					System.out.println("EchoClient> " + initMessage);
				}
				connected = true;
			} catch (NumberFormatException e) {
				throw new CannotConnectException(e.getMessage());
			} catch (IOException e) {
				throw new CannotConnectException(e.getMessage());
			}
			
		} catch (UnknownHostException e) {
			throw new CannotConnectException(e.getMessage());
		}
	}
	
	public boolean isConnected() {
		return this.connected;
	}
	public void send(String[] message) throws CannotConnectException {
		// TODO: Send message and print response
		StringBuilder msg = new StringBuilder();
		for (int i=1; i < message.length; i++) {
			msg.append(message[i]);
		}
		byte[] bytes = msg.append("\r").toString().getBytes(StandardCharsets.US_ASCII);
		send(bytes);
		byte[] answer = receive();
        try {
			System.out.println("EchoClient> " + (new String(answer, "US-ASCII").trim()));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private void send(byte[] bytes) throws CannotConnectException {
		try {
			Integer messageLength = bytes.length;
			out.write(bytes, 0, messageLength);
			out.flush();
		} catch (UnsupportedEncodingException e) {
			throw new CannotConnectException(e.getMessage());
		} catch (IOException e) {
			throw new CannotConnectException(e.getMessage());
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
			throw new CannotConnectException(e.getMessage());
		} catch (IOException e) {
			throw new CannotConnectException(e.getMessage());
		}
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
		connected = false;
	}
	
}

