package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import client.KVStore;
import client.NotificationListener;
import common.messages.KVMessage;
import helpers.Constants;
import helpers.ErrorMessages;
import helpers.CannotConnectException;
import helpers.Commands;

import org.apache.log4j.Logger;
import org.apache.log4j.pattern.IntegerPatternConverter;

/**
 * Main Client class
 * Implementing Client Command Line Interface (CLI)
 */
public class KVClient {

	private static boolean quit = false;
	private static KVStore engine;
	private static Logger logger = Logger.getLogger(KVClient.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		engine = new KVStore(); // Initializing with default values
		BufferedReader consoleReader;
		// Reader for input from stdIn
		consoleReader = new BufferedReader(new InputStreamReader(System.in));

		while (!quit) {
			try {
				// Take and parse the user input
				System.out.print(Constants.CLIENT_PROMPT);
				String input = consoleReader.readLine();
				String[] tokens = input.trim().split("\\s+");
				if (tokens != null) {
					if (tokens[0].equals("help")) {
						switch (tokens.length) {
						case 1:
							printHelp();
							break;
						case 2:
							printHelp(tokens[1]);
							break;
						default:
							printHelp("help");
							break;
						}
					} else if (tokens[0].equals("quit")) {
						switch (tokens.length) {
						case 1:
							quit = true;
							break;
						default:
							printHelp("quit");
							break;
						}
					} else if (tokens[0].equals("disconnect")) {
						switch (tokens.length) {
							case 1:
								engine.disconnect(true);
								System.out.println("Connection to the server has been terminated. Please connect again.");
								break;
							default:
								printHelp("disconnect");
								break;
						}
					} else if (tokens[0].equals("connect")) {
						switch (tokens.length) {
							case 3:
								// Perform the connection
								if (!engine.isConnected()) {
									if (isHostValid(tokens[1], tokens[2])) {
										try {
											engine.connect(tokens[1],Integer.parseInt(tokens[2]) );
										} catch (CannotConnectException e) {
											System.out.println("Connection failed: " + "\nError Message: " + e.getErrorMessage());
										} catch (Exception e) {
											logger.error("Could not establish connection to the server", e);
											System.out.println("Connection failed: " + "\nError Message: " + e.getMessage());
										}
									} else {
										logger.debug(String.format("Invalid host and port entry. Host: %s, Port: %s", tokens[1], tokens[2]));
										System.out.println("Please insert valid IP or Port");
										printHelp("connect");
									}
								} else {
									System.out.println("There is already a connection open, please run disconnect first");
								}
								break;
							default:
								printHelp("connect");
								break;
						}
					} else if (tokens[0].equals("get")) {
						// Get value of key
						if (tokens.length == 2) {
							String key = tokens[1];
							KVMessage response = engine.get(key);
							printGetResponse(response);
						} else {
							printHelp("get");
						}
					} else if (tokens[0].equals("put")) {
						// Puts, updates or deletes value for key
						if (tokens.length == 3) {
							String key = tokens[1];
							String value = tokens[2];
							KVMessage response = engine.put(key, value);
							printPutResponse(response);
						} else {
							printHelp("put");
						}
					} else if (tokens[0].equals("subscribe")) {
						// Subscribe to a key
						if (tokens.length == 2) {
							String key = tokens[1];
							KVMessage response = engine.subscribe(key);
							printSubscribeResponse(response);
						} else {
							printHelp("subscribe");
						}
					} else if (tokens[0].equals("unsubscribe")) {
						// Unsubscribe to a key
						if (tokens.length == 2) {
							String key = tokens[1];
							KVMessage response = engine.unsubscribe(key);
							printUnsubscribeResponse(response);
						} else {
							printHelp("subscribe");
						}
					} else if (tokens[0].equals("logLevel")) {
						// Change the loglevel
						switch (tokens.length) {
						case 2:
							engine.logLevel(tokens[1]);
							logger.setLevel(engine.getLogLevel());
							break;
						default:
							printHelp("logLevel");
							break;
						}
					} else {
						System.out.println("Command <<" + tokens[0] + ">> not recognized!");
						printHelp();
					}
				}
			} catch (IOException e) {
				logger.error("IOException occurred", e);
				System.out.println(ErrorMessages.ERROR_INTERNAL);
			} catch (CannotConnectException e) {
				logger.error("Connection error", e);
				System.out.println(ErrorMessages.CONNECTION_ERROR);
			} catch (Exception e) {
				logger.error("Exception occurred", e);
				System.out.println(ErrorMessages.SERVERS_DOWN);
			}
		}
//		try {
//			ServerSocket ss = NotificationListener.serverSocket;
//			Socket cs = NotificationListener.clientSocket;
//			if (ss != null) ss.close();
//			if (cs != null) cs.close();
//
//			logger.debug("Closed both sockets on notification listener!!!");
//		} catch (IOException e) {
//			e.printStackTrace();
//		}

		engine.getNotificationListener().stop();
		System.out.println("KVClient exit!");
	}

	private static void printPutResponse(KVMessage response) {
		if (response.getStatus().equals(KVMessage.StatusType.PUT_SUCCESS)) {
			System.out.println("Put operation successful");
		} else if (response.getStatus().equals(KVMessage.StatusType.PUT_UPDATE)) {
			System.out.println("Update operation successful");
		} else if (response.getStatus().equals(KVMessage.StatusType.DELETE_SUCCESS)) {
			System.out.println("Delete operation successful");
		} else if (response.getStatus().equals(KVMessage.StatusType.PUT_ERROR)) {
			System.out.println("Put operation failed");
		} else if (response.getStatus().equals(KVMessage.StatusType.SERVER_STOPPED)) {
			System.out.println("Server is stopped, please try again later.");
		} else if (response.getStatus().equals(KVMessage.StatusType.SERVER_WRITE_LOCK)) {
			System.out.println("Server is locked for put operations, please try again later.");
		} else if (response.getStatus().equals(KVMessage.StatusType.DELETE_ERROR)) {
			System.out.println("Delete operation failed");
		} else {
			System.out.println("Unknown error occurred. Please try again.");
		}
	}

	/**
	 * Helper function for printing the result of a get response to the client CLI
	 *
	 * @param response
     */
	private static void printGetResponse(KVMessage response) {
		if (response.getStatus().equals(KVMessage.StatusType.GET_SUCCESS) && response.getValue() != null) {
			System.out.println(response.getValue());
		} else if (response.getStatus().equals(KVMessage.StatusType.GET_ERROR)) {
			System.out.println("Value could not be retrieved");
		} else if (response.getStatus().equals(KVMessage.StatusType.SERVER_STOPPED)) {
			System.out.println("Server is stopped, please try again later.");
		} else {
			System.out.println("Unknown error occurred. Please try again.");
		}
	}

	private static void printSubscribeResponse(KVMessage response) {
		if (response.getStatus().equals(KVMessage.StatusType.SUBSCRIBE_SUCCESS)) {
			System.out.println("Subscription successful");
		} else if (response.getStatus().equals(KVMessage.StatusType.SUBSCRIBE_ERROR)) {
			System.out.println("Subscription unsuccessful");
		} else {
			System.out.println("Unknown error occurred. Please try again.");
		}
	}

	private static void printUnsubscribeResponse(KVMessage response) {
		if (response.getStatus().equals(KVMessage.StatusType.UNSUBSCRIBE_SUCCESS)) {
			System.out.println("Unsubscription successful");
		} else if (response.getStatus().equals(KVMessage.StatusType.UNSUBSCRIBE_ERROR)) {
			System.out.println("Unsubscription unsuccessful");
		} else {
			System.out.println("Unknown error occurred. Please try again.");
		}
	}

	/**
	 * Helper function for the checking the validity of the host. It does this by performing a pattern match.
	 *
	 * @param host
	 * @param hostPort
	 * @return
	 */
	private static boolean isHostValid(String host, String hostPort) {
		try {
			int port = Integer.parseInt(hostPort);
			if ((port < 0) || (port > 65535)) {
				return false;
			}
		} catch (NumberFormatException e) {
			return false;
		} catch (NullPointerException e) {
			return false;
		}
		final String IPADDRESS_PATTERN = "((?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?))|localhost";
		final String DOMAIN_START_END_PATTERN_STRING = "^([a-zA-Z0-9]+\\.)+[a-zA-Z][a-zA-Z]";
		final String PATTERN = IPADDRESS_PATTERN + "|" + DOMAIN_START_END_PATTERN_STRING;

		Pattern pattern = Pattern.compile(PATTERN);
		Matcher matcher = pattern.matcher(host);

		if (!matcher.matches()) {
			return false;
		}
		return true;
	}

	/**
	 * Helper function for printing the general help to the user.
	 */
	private static void printHelp() {
		System.out.println("This program is a simple client application to a set of data servers.\n");
		System.out.println("\nget:");
		printHelp("get");
		System.out.println("\nput:");
		printHelp("put");
		System.out.println("\nsubscribe:");
		printHelp("subscribe");
		System.out.println("\nunsubscribe:");
		printHelp("unsubscribe");
		System.out.println("\nlogLevel:");
		printHelp("logLevel");
		System.out.println("\nhelp:");
		printHelp("help");
		System.out.println("\nquit:");
		printHelp("quit");
	}

	/**
	 * Helper function for printing the help of a command.
	 *
	 * @param command
	 */
	private static void printHelp(String command) {
		try {
			Commands currentCommand = Commands.valueOf(command.toUpperCase());
			switch (currentCommand) {
			case GET:
				System.out.println("Syntax: get <key>\n " + "Retrieves the value for the given key from the server");
				break;
			case PUT:
				System.out.println("Syntax: put <key> <value>\n " +
						"Inserts a key-value pair into the storage server.\n " +
						"Updates (overwrites) the current value with the given value if the server already contains the specified key.\n " +
						"Deletes the entry for the given key if <value> equals null.");
				break;
			case LOGLEVEL:
				System.out.println("Syntax: logLevel <level>\n " + "Sets the logger to the specified log level");
				break;
			case HELP:
				System.out.println("Syntax: help [<command name>]\n " + "Prints help message");
				break;
			case QUIT:
				System.out.println("Syntax: quit\n " + "Quits the client");
				break;
			case SUBSCRIBE:
				System.out.println("Syntax: subscribe <key>\n" + "Subscribes to notifications to a key" );
				break;
			case UNSUBSCRIBE:
				System.out.println("Syntax: unsubscribe <key>\n" + "unsubscribes to notifications to a key" );
				break;
			default:
				System.out.println("No such command: \"" + command + "\"\n" + "Run \"help\" for more");
				break;
			}
		} catch (IllegalArgumentException e) {
			System.out.println("Unknown command");
			printHelp("help");
		}
	}
}
