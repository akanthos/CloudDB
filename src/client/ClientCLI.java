package client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import helpers.ErrorMessages;
import helpers.CannotConnectException;
import helpers.Commands;

import org.apache.log4j.Logger;

/**
 * Main class
 */
public class ClientCLI {

	private static boolean quit = false;
	private static KVStore engine;
	private static Logger logger = Logger.getLogger(ClientCLI.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		engine = new KVStore("", 0); // Initializing with default values
		BufferedReader consoleReader;
		// Reader for input from stdIn
		consoleReader = new BufferedReader(new InputStreamReader(System.in));

		while (!quit) {
			try {
				// Take and parse the user input
				System.out.print("EchoClient> ");
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
							engine.disconnect();
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
                                        engine.setHost(tokens[1]);
                                        engine.setPort(Integer.parseInt(tokens[2]));
										engine.connect();
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
				System.out.println(ErrorMessages.ERROR_INTERNAL);
			}
		}

		// Got the quit command. Close connections and gracefully exit
		if (engine.isConnected()) {
			engine.disconnect();
		}
		System.out.println("ClientCLI exit!");
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
		} else if (response.getStatus().equals(KVMessage.StatusType.DELETE_ERROR)) {
			System.out.println("Delete operation failed");
		}
	}

	private static void printGetResponse(KVMessage response) {
		if (response.getStatus().equals(KVMessage.StatusType.GET_SUCCESS) && response.getValue() != null) {
			System.out.println(response.getValue());
		}
		else if (response.getStatus().equals(KVMessage.StatusType.GET_ERROR)) {
			System.out.println("Value could not be retrieved");
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
		System.out.println("This program is a simple client that is able to establish a TCP connection\n"
				+ " to a given echo server and exchange text messages with it.");
		System.out.println("\nconnect:");
		printHelp("connect");
		System.out.println("\ndisconnect:");
		printHelp("disconnect");
		System.out.println("\nget:");
		printHelp("get");
		System.out.println("\nput:");
		printHelp("put");
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
			case CONNECT:
				System.out.println("Syntax: connect <hostname or IP address> <port>\n "
						+ "Tries to create a TCP connection with the echo server");
				break;
			case DISCONNECT:
				System.out.println("Syntax: disconnect\n " + "Tries to disconnect from the connected server");
				break;
			case GET:
				System.out.println("Syntax: get <key>\n " + "Retrieves the value for the given key from the " +
						"currently connected storage server");
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
