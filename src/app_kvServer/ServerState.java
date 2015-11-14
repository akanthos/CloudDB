package app_kvServer;

/**
 * Created by akanthos on 12.11.15.
 */
public class ServerState {
    private boolean initialized;
    private boolean isOpen;
    private boolean writeLock;
    private boolean stopped;

    public ServerState() {

    }

    public synchronized void setInitialized(boolean init) {
        this.initialized = init;
    }
    public synchronized void setIsOpen(boolean isOpen) { this.isOpen = isOpen; }
    public synchronized void setWriteLock(boolean wl) {
        this.writeLock = wl;
    }
    public synchronized void setStopped(boolean s) {
        this.stopped = s;
    }

    public synchronized boolean isInitialized() {
        return initialized;
    }
    public synchronized boolean isOpen() { return isOpen;  }
    public synchronized boolean isStopped() {
        return stopped;
    }
    public synchronized boolean isWriteLock() {
        return writeLock;
    }
}
