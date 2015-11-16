package app_kvServer;

/**
 * Created by akanthos on 12.11.15.
 */
public class ServerState {
    private boolean initialized;
    private boolean isOpen;
    private boolean writeLock;
    private boolean stopped;

    public ServerState(boolean init, boolean open, boolean writeLock, boolean stop) {
        this.initialized = init;
        this.isOpen = open;
        this.writeLock = writeLock;
        this.stopped = stop;
    }

    public void setInitialized(boolean init) {
        this.initialized = init;
    }
    public void setIsOpen(boolean isOpen) { this.isOpen = isOpen; }
    public void setWriteLock(boolean wl) {
        this.writeLock = wl;
    }
    public void setStopped(boolean s) {
        this.stopped = s;
    }

    public boolean isInitialized() {
        return initialized;
    }
    public boolean isOpen() { return isOpen;  }
    public boolean isStopped() {
        return stopped;
    }
    public boolean isWriteLock() {
        return writeLock;
    }
}
