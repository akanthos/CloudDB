package app_kvEcs;

import common.Serializer;
import common.ServerInfo;
import common.messages.KVAdminMessage;
import common.messages.KVAdminMessageImpl;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Responsible for communicating with the servers
 * (send ECS commands receives acknowledgments)
 *
 */
public class KVConnection extends Thread {
    private Logger logger = Logger.getRootLogger();
    private Socket connection;
    private OutputStream output;
    private InputStream input;
    private boolean connected;
    private boolean reply;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
    private ServerInfo server;

    public KVConnection(ServerInfo serverInfo) {
        this.server = serverInfo;
    }

    public ServerInfo getServer() {
        return server;
    }

    public void setServer(ServerInfo server) {
        this.server = server;
    }

    public synchronized boolean gotreply() {
        return reply;
    }

    public synchronized void setreply(boolean reply) {
        this.reply = reply;
    }

    public void run() {
        try {
            while (isConnected()) {
                KVAdminMessageImpl e = (KVAdminMessageImpl)receiveMessage();
                if (e != null) {
                    if (e.getStatus() == KVAdminMessage.StatusType.INIT_SUCCESS) {
                        synchronized (this) {
                            setreply(true);
                            notify();
                        }
                        disconnect();
                    }
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            disconnect();
        }
    }

    public synchronized void connect() throws IOException {
        try {
            connection = new Socket(server.getAddress(), server.getServerPort());
            output = connection.getOutputStream();
            input = connection.getInputStream();
            connected = true;
        } catch (IOException ioe) {

            logger.error("Connection could not be established!");
            throw ioe;
        }
    }

    public synchronized void disconnect() {
        try {
            if (isConnected())
                tearDownConnection();
        } catch (IOException ioe) {
            logger.error("Unable to close connection!");
        }
    }

    private synchronized void tearDownConnection() throws IOException {
        if (connection != null) {
            connected = false;
            input.close();
            output.close();
            connection.close();
            connection = null;
        }
    }

    public synchronized boolean isConnected() {
        return connected;
    }

    public KVAdminMessage receiveMessage() throws IOException {
        if (isConnected()) {
            int index = 0;
            byte[] msgBytes = null, tmp = null;
            byte[] bufferBytes = new byte[BUFFER_SIZE];

			/* read first char from stream */
            byte read = (byte) input.read();
            boolean reading = true;

            while (read != 13 && reading) {/* carriage return */
				/* if buffer filled, copy to msg array */
                if (index == BUFFER_SIZE) {
                    if (msgBytes == null) {
                        tmp = new byte[BUFFER_SIZE];
                        System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                    } else {
                        tmp = new byte[msgBytes.length + BUFFER_SIZE];
                        System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                        System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                                BUFFER_SIZE);
                    }

                    msgBytes = tmp;
                    bufferBytes = new byte[BUFFER_SIZE];
                    index = 0;
                }

				/* only read valid characters, i.e. letters and numbers */
                if ((read > 31 && read < 127)) {
                    bufferBytes[index] = read;
                    index++;
                }

				/* stop reading is DROP_SIZE is reached */
                if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                    reading = false;
                }

				/* read next char from stream */
                read = (byte) input.read();
            }

            if (msgBytes == null) {
                tmp = new byte[index];
                System.arraycopy(bufferBytes, 0, tmp, 0, index);
            } else {
                tmp = new byte[msgBytes.length + index];
                System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
            }

            msgBytes = tmp;

			/* build final String */
            KVAdminMessage msg = (KVAdminMessageImpl) Serializer.toObject(msgBytes);
            logger.info("Receive ECSMsg from " + connection.getPort()
                    + ":\t '" + msg.getStatus() + "'");
            return msg;
        } else
            return null;
    }

    public void sendMessage(KVAdminMessageImpl msg) throws IOException {
        byte[] msgBytes = Serializer.toByteArray(msg);
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("Send message to " + connection.getPort() + ":\t '"
                + msg.getStatus() + "' to : " + server.toString());
    }


}

