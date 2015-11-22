package app_kvEcs;

public enum ECSCommand {

    INIT("init"),  START("start"), STOP("stop"), SHUT_DOWN("shutDown"), ACK("ack"),  SET_WRITE_LOCK("setWriteLock"), UNLOCK_WRITE("setUnlock"),
    MOVE_DATA("moveData"), SEND_METADATA("sendMetadata"), ADD("add"), REMOVE("remove"),
    LOG_LEVEL("logLevel"), HELP("help"), QUIT("quit"), REMOVE_DATA("removeData"), UNSUPPORTED( "unSupported"),;

    private String commandText;

    /**
     * Enum constructor initializing commandText
     *
     * @param commandText
     */
    private ECSCommand(String commandText) {
        this.commandText = commandText;
    }

    /**
     * @return commandText
     */
    public String getCommandText() {
        return commandText;
    }

    /**
     * @param commandText
     * @return
     */
    public static ECSCommand fromString(String commandText) {
        if (commandText != null) {
            for (ECSCommand command : ECSCommand.values()) {
                if (commandText.equalsIgnoreCase(command.commandText)) {
                    return command;
                }
            }
        }
        return ECSCommand.UNSUPPORTED;
    }

}