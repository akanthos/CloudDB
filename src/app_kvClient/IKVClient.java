package app_kvClient;

import helpers.CannotConnectException;

/**
 * Interface class for connection libs
 */
public interface IKVClient {

    public void connect(String host, String hostPort) throws CannotConnectException;

    public boolean isConnected();

    public void send(byte[] bytes) throws CannotConnectException;

    public byte[] receive() throws CannotConnectException;

    public void closeConnection();

}
