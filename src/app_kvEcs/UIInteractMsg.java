package app_kvEcs;

/**
 * The class contains all the user
 * interacting with the application.
 */
public class UIInteractMsg {


    public static final String ECS_ECHO_PROMPT = "ECS> ";

    public static final String SPLIT_DLMT = "\\s+";

    public static final String ILLIGAL_PARAM = " is not as expected, Please use the help command to view an example.";

    public static final String ILLIGAL_PARAM_NUMBER = "Illegal number of parameters," +
            " Please use the help command to see an example.";


    public static final String ECS_HELP_TEXT = "ECS:  Cloud databases MS3 ECS Service."
            + "\nUsage:"
            + "\nStart: Sends a start signal to all the servers using ECS Service.\n"
            + "Stop:  Sends a stop signal to all the servers using ECS Service.\n"
            + "Shutdown:  Sends a Shutdown signal to all the servers using ECS Service.\n"
            + "Add:  Adds a Store Server to the Ring and performs respective data Re-Arrangements.\n"
            + "Remove:  Removes a Store Server from the Ring and performs related data Re-Arrangements.\n"
            + "\nlogLevel <level>: Sets the logger to the desired Logging Level."
            + "\nHelp: Prints the help guide."
            + "\nquit: Shuts down servers and exit application.";

    public static final String GENERAL_ILLIGAL_ARGUMENT = "Please enter a valid command. \n"+ ECS_HELP_TEXT;

    public static final String UNSUPPORTED_COMMAND = "Unknown command.\n"+ ECS_HELP_TEXT;
}
