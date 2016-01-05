package app_kvEcs;

/**
 * Contains all the user
 * interacting handling.
 */
public class UIInteractMsg {

    public static final String ILLEGAL_PARAM = " is not as expected, Please use the help command to view an example.";

    public static final String ILLEGAL_PARAM_NUM = "Illegal number of parameters," +
            " Please use the help command to see an example.";


    public static final String ECS_HELP_TEXT = "ECSInterface:  Cloud databases MS3 ECSInterface Service."
            + "\nUsage:"
            + "\nInit <NumberOfNodes> <cacheSize> <displacementStrategy>: Initialize the service with N random nodes"
            + "\nStart: Sends a start signal to all the servers using ECSInterface Service.\n"
            + "Stop:  Sends a stop signal to all the servers using ECSInterface Service.\n"
            + "Shutdown:  Sends a Shutdown signal to all the servers using ECSInterface Service.\n"
            + "Add <cacheSize> <displacementStrategy>:  Adds a Store Server to the Ring and performs respective data Re-Arrangements.\n"
            + "Remove:  Removes a Store Server from the Ring and performs related data Re-Arrangements.\n"
            + "\nlogLevel <level>: Sets the logger to the desired Logging Level."
            + "\nHelp: Prints the help guide."
            + "\nquit: Shuts down servers and exit application.";

    public static final String GENERAL_ILLEGAL_ARGUMENT = "Please enter a valid command. \n"+ ECS_HELP_TEXT;

    public static final String UNSUPPORTED_COMMAND = "Unknown command.\n"+ ECS_HELP_TEXT;
}
