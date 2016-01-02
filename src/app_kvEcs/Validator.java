package app_kvEcs;

import java.util.Arrays;
import java.util.List;


/**
 * The class contains utility functions
 * validating user input using Singleton.
 *
 */
public class Validator {

    private static Validator VALIDATION_INSTANCE = null;

    /**
     * A list that contains all the options of log Levels
     *
     */
    public static final List< String > LOG_LEVELS = Arrays.asList("ALL",
            "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "OFF");

    /**
     * Singleton provider.
     *
     * @return
     */
    public static Validator getInstance () {
        if ( VALIDATION_INSTANCE == null ) {
            VALIDATION_INSTANCE = new Validator ();
        }
        return VALIDATION_INSTANCE;
    }

    /**
     * avoid class instantiation
     */
    private Validator () {
    }



    /**
     * Validates the log level input by user.
     * Validates the number of parameters and
     * the validity of the new log level to set.
     *
     * @param tokens the user input
     * @return true/false status
     * @throws IllegalArgumentException
     */
    public boolean isValidLogLevel ( String [] tokens ) throws IllegalArgumentException  {
        if ( tokens == null ) {
            throw new IllegalArgumentException (
                    UIInteractMsg.GENERAL_ILLEGAL_ARGUMENT );
        }

        if ( tokens.length < 2 ) {
            throw new IllegalArgumentException (
                    UIInteractMsg.ILLEGAL_PARAM_NUM );
        }

        if ( ! LOG_LEVELS.contains ( tokens [ 1 ].toUpperCase () ) ) {
            throw new IllegalArgumentException ( "LogLevel"
                    + UIInteractMsg.ILLEGAL_PARAM );
        }
        return true;
    }

    /**
     * check if give valid policy
     * @param policy
     * @return
     */
    public boolean isValidPolicy( String policy ){
        String Dpolicy = policy.toUpperCase();
        return ( Dpolicy.equals("FIFO") || Dpolicy.equals("LRU") || Dpolicy.equals("LFU") );
    }


}
