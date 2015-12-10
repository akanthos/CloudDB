package app_kvServer;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class InOutClientPack {
    private InputStream inputStream;
    private OutputStream outputStream;
    private Socket clientSocket;

    public InOutClientPack(InputStream inputStream,
            OutputStream outputStream,
            Socket clientSocket) {
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        this.clientSocket = clientSocket;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    public Socket getClientSocket() {
        return clientSocket;
    }
}
