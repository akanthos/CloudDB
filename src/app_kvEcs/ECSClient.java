package app_kvEcs;

import common.messages.TextMessage;
import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ECSClient {

    static {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
    }
    private static Logger logger = Logger.getLogger(ECSClient.class);
    private static final String PROMPT = "ECSClient> ";
    private BufferedReader stdin;
    private ECSCore ECServer;
    private String fileName;
    private boolean stop = false;
    private boolean initialized = false;

    public ECSClient (String fileName) {
        this.fileName = fileName;
    }

    public void run() {
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("ECSCore Client CLI not responding - Application terminated.");
            }
        }
    }

    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");
        ECSUI command = ECSUI.fromString(tokens[0]);
        Validator validator = Validator.getInstance();
        switch (command) {
            case START:
                this.ECSStart();
                break;
            case INIT:
                if(tokens.length == 4) {
                    if ( validator.isValidPolicy(tokens[3]) )
                        this.ECSinit(tokens[1], tokens[2], tokens[3]);
                    else
                        printError("Invalid replacement policy.");
                } else {
                    printError("Invalid number of parameters.");
                }
                break;
            case STOP:
                this.ECSStop();
                break;
            case SET_WRITE_LOCK:
                ECServer.lockWrite();
                break;
            case UNLOCK_WRITE:
                ECServer.unlockWrite();
                break;
            case ADD:
                if(tokens.length == 3) {
                    if ( validator.isValidPolicy(tokens[2]) )
                        this.ECSAddServer(tokens[1], tokens[2]);
                }
                else{
                    printError("Invalid number of parameters.");
                }
                break;

            case REMOVE:
                this.ECSRemoveServer();
                break;

            case SHUT_DOWN:{
                this.ECSShutDown();
            }
            break;
            case LOG_LEVEL:
                try {
                    if (validator.isValidLogLevel(tokens)) {
                        logger.info("Log Level Set to: " + tokens[1]);
                        System.out.println("Log Level Set to: " + tokens[1]);
                    }
                }
                catch (IllegalArgumentException e){
                    System.out.println(PROMPT + e.getMessage());
                }
                break;
            case HELP:
                System.out.println(UIInteractMsg.ECS_HELP_TEXT);
                logger.info("Help Text provided to user.");
                break;
            case QUIT:
                stop = true;
                this.ECSShutDown();
                System.out.println("Quit program based on user request.");
                logger.info("Quit program based on user request.");
                System.exit(1);
                break;

            default:
                System.out.println(UIInteractMsg.UNSUPPORTED_COMMAND);
                break;
        }

    }

    /**
     * Initializes ECSCore server, check ECSCore' initService function
     * @param numNodes
     * @param cacheSize
     * @param displacementStrategy
     */
    private void ECSinit(String numNodes,String cacheSize, String displacementStrategy) {
        try {
            ECServer = new ECSCore(fileName);
            if (ECServer.initService(Integer.parseInt(numNodes), Integer.parseInt(cacheSize), displacementStrategy)) {
                initialized = true;
            }
            else{
                System.out.println(PROMPT + "Failed initializing the ECSInterface Service.");
            }
        } catch (IOException e) {
            logger.error("Could not initialize ECSCore Service. Problem accessing ecs.config file");
            System.out.println("Could not initialize ECSCore Service");
            return;
        }
    }

    /**
     * Start the ECSCore service
     */
    private void ECSStart() {
        if ( !initialized ) {
            System.out.println(PROMPT + "ECSInterface Service is not initialized. First initialize the server.");
            return;
        }
        if (ECServer.start()) {
            logger.info("ECSCore Service started.");
            System.out.println(PROMPT + "ECSInterface Service started.");
        }
        else {
            logger.error( "Failed starting the ECSCore Service." );
            System.out.println(PROMPT + "Failed starting the ECSInterface Service.");

        }
    }

    /**
     * Stop the ECSCore
     */
    private void ECSStop() {
        if ( !initialized )
            System.out.println(PROMPT + "ECSInterface Service is not initialized. First initialize the server.");
        if (ECServer.stop()) {
            logger.info("ECSCore Service stopped.");
            System.out.println(PROMPT + "ECSInterface Service stopped.");
        }
        else {
            logger.info( "ECSCore Service could not be stopped." );
            System.out.println(PROMPT + "ECSInterface Service could not be stopped.");
        }
    }

    /**
     * Shutdown ECSCore
     */
    private void ECSShutDown() {
        if ( !initialized ) {
            System.out.println(PROMPT + "ECSI Service is not initialized. First initialize the server.");
            return;
        }
        if ( ECServer != null && ECServer.shutdown()) {
            logger.info( "ECSCore Service shutdown." );
            System.out.println(PROMPT + "ECSInterface Service shutdown successfully.");
            ECServer = null;
        }
        else {
            logger.debug("ECSCore Service shutdown failed.");
            System.out.println(PROMPT + "ECSInterface Service shutdown failed.");
        }
    }

    /**
     * Add Server arbitrarily selected from ECSCore configuration file
     * @param cacheSize
     * @param displacementStrategy
     */
    private void ECSAddServer(String cacheSize, String displacementStrategy) {
        if (ECServer.addNode( Integer.parseInt(cacheSize), displacementStrategy )) {
            logger.debug("A node has been added! ");
            System.out.println(PROMPT + "A node has been added!");
        }
        else{
            System.out.println(PROMPT + "ECSCore Service failed adding a new node.");
            logger.debug("ECSCore Service failed adding a new node.");
        }
    }

    /**
     * remove arbitrarily selected Store server
     */
    private void ECSRemoveServer() {
        if (ECServer.getSize() == 1){
            System.out.println(PROMPT + "Only one server on the Ring. Cannot be removed. \n" +
                    "If you want to delete ring please use the shutdown functionality.");
            return;
        }
        if( ECServer.removeNode() ){
            logger.debug("Random Node has been removed.");
            System.out.println(PROMPT + "Random Node has been removed.");
        }
        else{
            System.out.println(PROMPT + "ECSCore Service failed to remove a node.");
            logger.debug("ECSCore Service failed to remove a node.");
        }
    }

    private void printError(String error){
        System.out.println(PROMPT + "Error. " +  error);
    }

}
