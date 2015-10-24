package app_kvServer;


import app_kvClient.KVClient;
import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class KVServer  {

	int cacheSize, numOfClients;
	String strategy;
	ServerSocket serverSocket;
	private static Logger logger = Logger.getLogger(KVServer.class);

	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed 
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache 
	 *           is full and there is a GET- or PUT-request on a key that is 
	 *           currently not contained in the cache. Options are "FIFO", "LRU", 
	 *           and "LFU".
	 */
	public KVServer(int port, int cacheSize, String strategy) {
		try {
			PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
			serverSocket = new ServerSocket(port);
			this.cacheSize = cacheSize;
			this.strategy = strategy;
			System.out.println("Server starting up!!");
			logger.info("Server starting up!!");
			run();
		} catch (IOException e) {
			e.printStackTrace(); // TODO: Remove this
			logger.error("Unable to initialize server socket.", e);
		}
	}

	private void run() {
		// TODO: Should there be an exit strategy for the server?
		while (true) {
			try {
				Socket clientSocket = serverSocket.accept();
				numOfClients++;
				System.out.println(String.format("Client %d connected", numOfClients));
				KVClient kvClient = new KVClient(clientSocket, numOfClients);
				Thread t = new Thread(kvClient);
				t.start();
			} catch (IOException e) {
				e.printStackTrace();
				logger.error("Unable to get client socket", e);
			}
		}
	}
}
