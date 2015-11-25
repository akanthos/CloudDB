package app_kvServer;

/**
 * Represents the server state (started, stopped, etc)
 */
public class ServerState {
    private boolean initialized;
    private boolean isOpen;
    private boolean writeLock;
    private boolean stopped;

    /**
     * Constructor
     * @param init initialization state
     * @param open open/closed state
     * @param writeLock write_lock state
     * @param stop stopped state
     */
    public ServerState(boolean init, boolean open, boolean writeLock, boolean stop) {
        this.initialized = init;
        this.isOpen = open;
        this.writeLock = writeLock;
        this.stopped = stop;
    }

    /**
     * Initialized state setter
     * @param init
     */
    public void setInitialized(boolean init) {
        this.initialized = init;
    }

    /**
     * Open/closed state setter
     * @param isOpen
     */
    public void setIsOpen(boolean isOpen) { this.isOpen = isOpen; }

    /**
     * Write_lock state setter
     * @param wl
     */
    public void setWriteLock(boolean wl) {
        this.writeLock = wl;
    }

    /**
     * Stopped state setter
     * @param s
     */
    public void setStopped(boolean s) {
        this.stopped = s;
    }

    /**
     * Initialized state getter
     * @return
     */
    public boolean isInitialized() {
        return initialized;
    }

    /**
     * Open/closed state getter
     * @return
     */
    public boolean isOpen() { return isOpen;  }

    /**
     * Stopped state getter
     * @return
     */
    public boolean isStopped() {
        return stopped;
    }

    /**
     * Write_lock state getter
     * @return
     */
    public boolean isWriteLock() {
        return writeLock;
    }
}
