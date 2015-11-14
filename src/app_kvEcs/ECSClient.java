package app_kvEcs;

import common.messages.TextMessage;
import helpers.Constants;
import logger.LogSetup;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ECSClient implements ECSClientListener {

    static {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
    }

    private static Logger logger = Logger.getLogger(ECSClient.class);
    private static final String PROMPT = "ECSClient> ";
    private BufferedReader stdin;
    private ECSImpl ECServer;
    private final String fileName = "ecs.config";
    private boolean stop = false;
    private boolean initialized = false;


    public void run() {
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("ECSImpl Client CLI not responding - Application terminated ");
            }
        }
    }

    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

        if(tokens[0].equals("quit")) {
            stop = true;
            ECServer.shutdown();
            System.out.println(PROMPT + "ECSImpl Client Application exit!");
        }
        /*
          initService command takes 3 arguments: <numberOfNodes> <cacheSize> <displacementStrategy>
         */
        else if (tokens[0].equals("initService")){
            if(tokens.length == 4) {
                if ( isValidPolicy(tokens[3]) )
                        this.ECSinit(tokens[1], tokens[2], tokens[3]);
            } else {
                printError("Invalid number of parameters.");
            }

        } else  if (tokens[0].equals("start")) {
                this.ECSStart();
        } else if( tokens[0].equals("stop") ) {
                this.ECSStop();
        } else if
                ( tokens[0].equals("addNode") ) {
                if(tokens.length == 3) {
                    if ( isValidPolicy(tokens[2]) )
                        this.ECSAddServer(tokens[1], tokens[2]);
                }
                else{
                    printError("Invalid number of parameters.");
                }
        }
        else if ( tokens[0].equals("removeNode") ) {
            this.ECSRemoveServer();
        }
        else if(tokens[0].equals("logLevel")) {
            if(tokens.length == 2) {
                String level = "";
                if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
                    printError("No valid log level!");
                    printPossibleLogLevels();
                } else {
                    System.out.println(PROMPT +
                            "Log level changed to level " + level);
                }
            } else {
                printError("Invalid number of parameters!");
            }

        } else if(tokens[0].equals("help")) {
            printHelp();
        } else {
            //printError("Unknown command");
            printHelp();
        }
    }

    /**
     * Initializes ECSImpl server, check ECSImpl' initService function
     * @param numNodes
     * @param cacheSize
     * @param displacementStrategy
     */
    private void ECSinit(String numNodes,String cacheSize, String displacementStrategy) {
        try {
            ECServer = new ECSImpl( fileName );
        } catch (IOException e) {
            logger.error("Could not initialize ECSImpl Service. Problem accessing ecs.config file");
            System.out.println("Could not initialize ECSImpl Service");
        }
        ECServer.initService(Integer.parseInt(numNodes), Integer.parseInt(cacheSize), displacementStrategy);
    }

    /**
     * Start the ECSImpl service
     */
    private void ECSStart() {
        if ( ECServer == null )
            System.out.println(PROMPT + "ECSImpl Service is not initialized. First initialize the server.");
        if ( ECServer.start() ) {
            logger.info( "ECSImpl Service started." );
            System.out.println(PROMPT + "ECSImpl Service started.");
        }
        else {
            logger.error( "Failed starting the ECSImpl Service." );
            System.out.println(PROMPT + "Failed starting the ECSImpl Service.");

        }
    }

    /**
     * Stop the ECSImpl
     */
    private void ECSStop() {
        if ( ECServer == null )
            System.out.println(PROMPT + "ECSImpl Service is not initialized. First initialize the server.");
        if ( ECServer.stop() ) {
            logger.info( "ECSImpl Service stopped." );
            System.out.println(PROMPT + "ECSImpl Service stopped.");
        }
        else {
            logger.info( "ECSImpl Service could not be stopped." );
            System.out.println(PROMPT + "ECSImpl Service could not be stopped.");
        }
    }

    /**
     * Shutdown ECSImpl
     */
    private void ECSShutDown() {
        if ( ECServer == null )
            System.out.println(PROMPT + "ECSImpl Service is not initialized. First initialize the server.");
        if ( ECServer.shutdown() ) {
            logger.info( "ECSImpl Service shutdown." );
            System.out.println(PROMPT + "ECSImpl Service shutdown.");
            ECServer = null;
        }
        else {
            logger.debug("ECSImpl Service shutdown failed.");
            System.out.println(PROMPT + "ECSImpl Service shutdown failed.");
        }
    }

    /**
     * Add Server arbitrarily selected from ECSImpl configuration file
     * @param cacheSize
     * @param displacementStrategy
     */
    private void ECSAddServer(String cacheSize, String displacementStrategy) {
        if( ECServer.addNode( Integer.parseInt(cacheSize), displacementStrategy)){
            logger.debug("A node has been added! ");
            System.out.println(PROMPT + "A node has been added!");
        }
        else{
            System.out.println(PROMPT + "ECSImpl Service failed adding a new node.");
            logger.debug("ECSImpl Service failed adding a new node.");
        }
    }

    /**
     * remove arbitrarily selected Store server
     */
    private void ECSRemoveServer() {
        if( ECServer.removeNode() ){
            logger.debug("Random Node has been removed.");
            System.out.println(PROMPT + "Random Node has been removed.");
        }
        else{
            System.out.println(PROMPT + "ECSImpl Service failed to remove a node.");
            logger.debug("ECSImpl Service failed to remove a node.");
        }
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECSImpl CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("initService <numNodes> <cacheSize> <displacementStrategy>");
        sb.append("\t Initializes the ECSImpl Service. Available Displacement strategis: LRU | LFU | FIFO \n");
        sb.append(PROMPT).append("start");
        sb.append("\t\t Starts the ECSImpl Service \n");
        sb.append(PROMPT).append("stop");
        sb.append("\t\t\t Stops the ECSImpl Service \n");
        sb.append(PROMPT).append("addNode <cacheSize> <displacementStrategy>");
        sb.append("\t Add new StoreServer. Available Displacement strategis: LRU | LFU | FIFO  \n");
        sb.append(PROMPT).append("removeNode");
        sb.append("\t\t Remove randomly selected StoreServer \n");
        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t Changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t Exits the program");
        System.out.println(sb.toString());
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    public static void main(String[] args) {
        new ECSClient().run();
    }

    /**
     * check if give valid policy
     * @param policy
     * @return
     */
    private boolean isValidPolicy( String policy ){
        String Dpolicy = policy.toUpperCase();
        return ( Dpolicy.equals("FIFO") || Dpolicy.equals("LRU") || Dpolicy.equals("LFU") );
    }

    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }

    @Override
    public void handleNewMessage(TextMessage msg) {

    }

    @Override
    public void handleStatus(Status status) {

    }
}
