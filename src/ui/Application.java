package ui;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import engine.EchoClientEngine;
import helpers.ErrorMessages;
import helpers.CannotConnectException;
import helpers.Commands;

import org.apache.log4j.Logger;

/**
 * Main class
 */
public class Application {

	private static boolean quit = false;
	private static EchoClientEngine engine;
	private static Logger logger = Logger.getLogger(Application.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		engine = new EchoClientEngine();
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
							engine.closeConnection();
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
										engine.connect(tokens[1], tokens[2]);
									} catch (CannotConnectException e) {
										System.out.println("Connection failed: " + "\nError Message: " + e.getErrorMessage());
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
					} else if (tokens[0].equals("send")) {
						// Send the message
						if (tokens.length > 1) {
							String arr[] = input.split(" ", 2);
							String msg = arr[1];
							sendStuff(msg);
						} else {
							printHelp("send");
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
			}
		}

		// Got the quit command. Close connections and gracefully exit
		if (engine.isConnected()) {
			engine.closeConnection();
		}
		System.out.println("Application exit!");
	}

	/**
	 * This function calls the engine's send function while handling any exceptions.
	 *
	 * @param msg: message to be send
	 */
	private static void sendStuff(String msg) {
		if (engine.isConnected()) {
			try {
				engine.send(msg);
			} catch (CannotConnectException e) {
				System.out.println("Error: " + e.getErrorMessage());
			}
		} else {
			System.out.println("No connection to server yet :-(\n" + "Try the <connect> command first");
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
		final String IPADDRESS_PATTERN = "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
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
		System.out.println("\nsend:");
		printHelp("send");
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
			case SEND:
				System.out.println("Syntax: send <text message>\n " + "Will send the given text message to the"
						+ " currently connected echo server");
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
