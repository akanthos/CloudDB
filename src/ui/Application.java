/**
 * 
 */
package ui;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import engine.echoClientEngine;
import helpers.CannotConnectException;


/**
 * @author akanthos
 *
 */
public class Application {

	private static boolean quit = false;
	private static echoClientEngine engine;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		engine = new echoClientEngine();
		BufferedReader cons;
		cons = new BufferedReader(new InputStreamReader(System.in));

		while (!quit) {
			try {
				System.out.print("EchoClient> ");
				String input = cons.readLine();
				String[] tokens = input.trim().split("\\s+");
				if (tokens != null) {
					switch (tokens.length) {
					case 1:
						if (tokens[0].equals("help")) {
							printHelp();
						} else if (tokens[0].equals("disconnect")) {
							engine.closeConnection();
						} else if (tokens[0].equals("quit")) {
							quit = true;
						} else {
							printHelp();
						}
						break;
					case 2:
						if (tokens[0].equals("help")) {
							printHelp(tokens[1]);
						} else if (tokens[0].equals("send")) {
							sendStuff(tokens);
						} else if (tokens[0].equals("logLevel")) {
							engine.logLevel(tokens[1]);
						} else {
							System.out.println("Unknown command");
							printHelp();
						}
						break;
					case 3:
						if (tokens[0].equals("send")) {
							sendStuff(tokens);
						}
						else if (tokens[0].equals("connect")) {
							if (!engine.isConnected()) {
								if (isHostValid(tokens[1], tokens[2])) {
									try {
										engine.connect(tokens[1], tokens[2]);
									} catch (CannotConnectException e) {
										System.out.println("Connection failed: " // +
																					// "\nError
																					// Code:
																					// "
																					// +
																					// e.getErrorCode()
												+ "\nError Message: " + e.getErrorMessage());
									}
								} else {
									System.out.println("Please insert valid IP or Port");
									printHelp("connect");
								}
							} else {
								System.out.println("There is already a connection open, please run disconnect first");
							}
						}
						break;
					default:
						printHelp();
						break;
					}

				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		engine.closeConnection();
		System.out.println("Application exit!");
	}

	private static void sendStuff(String[] tokens) {
		if (engine.isConnected()) {
			try {
				engine.send(tokens);
			} catch (CannotConnectException e) {
				System.out.println("Error: " + e.getErrorMessage());
			}
		} else {
			System.out.println(
					"No connection to server yet :-(\n" + "Try the <connect> command first");
		}
	}

	private static boolean isHostValid(String host, String hostPort) {
		try {
			Integer.parseInt(hostPort);
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
		return;
	}

	private static void printHelp(String command) {
		switch (command) {
		case "connect":
			System.out.println("Syntax: connect <hostname or IP address> <port>\n "
					+ "Tries to create a TCP connection with the echo server");
			break;
		case "disconnect":
			System.out.println("Syntax: disconnect\n " + "Tries to disconnect from the connected server");
			break;
		case "send":
			System.out.println("Syntax: send <text message>\n " + "Will send the given text message to the"
					+ " currently connected echo server");
			break;
		case "logLevel":
			System.out.println("Syntax: logLevel <level>\n " + "Sets the logger to the specified log level");
			break;
		case "help":
			System.out.println("Syntax: help [<command name>]\n " + "Prints help message");
			break;
		case "quit":
			System.out.println("Syntax: quit\n " + "Quits the client");
			break;
		default:
			System.out.println("No such command: \"" + command + "\"\n" + "Run \"help\" for more");
			break;
		}
		return;
	}

}
