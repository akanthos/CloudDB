package app_kvServer;

/**
 * Created by akanthos on 11.11.15.
 */
public interface ServerActionListener {
    void serverStarted();
    void serverStopped();
    void serverWriteLocked();
    void serverWriteUnlocked();
    void serverShutDown();
}
