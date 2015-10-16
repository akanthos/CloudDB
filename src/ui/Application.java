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
import sun.awt.SunGraphicsCallback.PrintHeavyweightComponentsCallback;

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

						} else if (tokens[0].equals("quit")) {
							quit = true;
						} else {
							printHelp();
						}
						break;
					case 2:
						if (tokens[0].equals("help")) {
							printHelp(tokens[1]);
						}
						if (tokens[0].equals("help") && tokens[1].equals("send")) {
						}
						if (tokens[0].equals("help") && tokens[1].equals("send")) {
						}
						// System.out.println("send <message>");
						// System.out.println("logLevel <level>");
						break;
					case 3:
						if (tokens[0].equals("connect")) {
							if (isHostValid(tokens[1], tokens[2])) {
								engine = new echoClientEngine(tokens[1], tokens[2]);
								if (engine != null) {
									System.out.println("Connection successfull");
								}
								else {
									System.out.println("Connection failed");
								}
							}
							else {
								System.out.println("Please insert valid IP or Port");
								printHelp("connect");
							}
						}
						// System.out.println("connect <address> <port>");
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
		

	}
	
	private static boolean isHostValid(String hostIp, String hostPort) {
		if (Integer.parseInt(hostPort) != 50000) {
			return false;
		}
		String IPADDRESS_PATTERN = "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
		Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);
		Matcher matcher = pattern.matcher(hostIp);
		if (!matcher.matches()) {
			return false;
		}
		return true;
	}

	private static void printHelp() {
		System.out.println("This is help");
		return;
	}

	private static void printHelp(String command) {
		switch (command) {
		case "connect":
			System.out.println("MMMM");
			break;
		case "send":
			System.out.println("Syntax: <send <textmessage>\n " + "Will send the given text message to the"
					+ "currently connected echo server");
			break;
		default:
			System.out.println("No such command: \"" + command + "\"");
			printHelp();
			break;
		}
//		if (command.equals("send")) {
//			System.out.println("Syntax: <send <textmessage>\n " + "Will send the given text message to the"
//					+ "currently connected echo server");
//		}
		return;
	}
	

}
