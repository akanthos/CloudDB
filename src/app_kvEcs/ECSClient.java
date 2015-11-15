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
        ECSCommand command = ECSCommand.fromString(tokens[0]);
        Validator validator = Validator.getInstance();

        switch (command) {
            case START:
                this.ECSStart();
                break;
            case INIT:
                if(tokens.length == 4) {
                    if ( validator.isValidPolicy(tokens[3]) )
                        this.ECSinit(tokens[1], tokens[2], tokens[3]);
                } else {
                    printError("Invalid number of parameters.");
                }
                break;
            case STOP:
                this.ECSStop();
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
        if ( ECServer == null ) {
            System.out.println(PROMPT + "ECSImpl Service is not initialized. First initialize the server.");
            return;
        }
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
        if ( ECServer == null ) {
            System.out.println(PROMPT + "ECSImpl Service is not initialized. First initialize the server.");
            return;
        }
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

    public static void main(String[] args) {
        new ECSClient().run();
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
