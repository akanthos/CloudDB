package app_kvServer;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;


/**
 * Class that packs a connection socket, the input
 * stream and the output stream together.
 */
public class InOutClientPack {
    private InputStream inputStream;
    private OutputStream outputStream;
    private Socket clientSocket;

    /**
     * Constructor
     * @param inputStream the input stream
     * @param outputStream the output stream
     * @param clientSocket the client socket
     */
    public InOutClientPack(InputStream inputStream,
            OutputStream outputStream,
            Socket clientSocket) {
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        this.clientSocket = clientSocket;
    }

    /**
     * Input stream getter
     * @return the input stream
     */
    public InputStream getInputStream() {
        return inputStream;
    }

    /**
     * Output stream getter
     * @return the output stream
     */
    public OutputStream getOutputStream() {
        return outputStream;
    }

    /**
     * Connection socket getter
     * @return the connection socket
     */
    public Socket getClientSocket() {
        return clientSocket;
    }
}
