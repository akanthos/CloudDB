package app_kvEcs;

public enum ECSUI {

    INIT("init"),  START("start"), STOP("stop"),
    SHUT_DOWN("shutDown"), SET_WRITE_LOCK("setWriteLock"), UNLOCK_WRITE("setUnlock"),
    ADD("add"), REMOVE("remove"), UNSUPPORTED( "unSupported"),
    LOG_LEVEL("logLevel"), HELP("help"), QUIT("quit"),;

    private String cmd;

    /**
     * initializing cmd
     *
     * @param cmd
     */
    private ECSUI(String cmd) {
        this.cmd = cmd;
    }

    /**
     * @return commandText
     */
    public String getCommandText() {
        return cmd;
    }

    /**
     * @param c1
     * @return
     */
    public static ECSUI fromString(String c1) {
        if (c1 != null) {
            for (ECSUI command : ECSUI.values()) {
                if (c1.equalsIgnoreCase(command.cmd)) {
                    return command;
                }
            }
        }
        return ECSUI.UNSUPPORTED;
    }

}