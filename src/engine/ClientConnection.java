package engine;

import helpers.CannotConnectException;

public interface ClientConnection {

    public void connect(String host, String hostPort) throws CannotConnectException;

    public boolean isConnected();

    public void send(byte[] bytes) throws CannotConnectException;

    public byte[] receive() throws CannotConnectException;

    public void closeConnection();

}
