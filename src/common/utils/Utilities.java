package common.utils;

import app_kvServer.KVServer;
import common.Serializer;
import common.ServerInfo;
import common.messages.*;
import helpers.CannotConnectException;
import helpers.ErrorMessages;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;


public class Utilities {

    private static Logger logger = Logger.getLogger(Utilities.class);
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;

    static {
        PropertyConfigurator.configure("conf/log.config");
    }

    /**
     * This function sends a message to the server using the established connection.
     * @param msg
     * @param outputStream
     * @throws CannotConnectException
     */
    public static void send(KVMessageImpl msg, OutputStream outputStream) throws CannotConnectException {
        send(Serializer.toByteArray(msg), outputStream);
    }

    public static void send(KVAdminMessageImpl msg, OutputStream outputStream) throws CannotConnectException {
        send(Serializer.toByteArray(msg), outputStream);
    }
    public static void send(KVServerMessageImpl msg, OutputStream outputStream) throws CannotConnectException {
        send(Serializer.toByteArray(msg), outputStream);
    }

    /**
     * Helper function to send the bytes over the connection.
     *
     * @param bytes: the message bytes to be send.
     * @throws CannotConnectException
     */
    public static void send(byte[] bytes, OutputStream outputStream) throws CannotConnectException {
        try {
            Integer messageLength = bytes.length;
            outputStream.write(bytes, 0, messageLength);
            outputStream.flush();
        } catch (UnsupportedEncodingException e) {
            logger.error(e);
            throw new CannotConnectException("Unsupported Encoding in message to be send");
        } catch (IOException e) {
            logger.error(e);
            throw new CannotConnectException("Error while sending the message: " + e.getMessage());
        }
    }

    /**
     * Receives an array of bytes over the connection.
     *
     * @return
     * @throws CannotConnectException
     */
    public static byte[] receive(InputStream input) throws CannotConnectException, IOException {

        int index = 0;
        byte[] msgBytes = null, tmp;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        while(read != 13 && reading) {/* carriage return */
            if (read == -1) {
                return new byte[]{-1};
            }
			/* if buffer filled, copy to msg array */
            if(index == BUFFER_SIZE) {
                if(msgBytes == null){
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

			/* only read valid characters, i.e. letters and constants */
            bufferBytes[index] = read;
            index++;

			/* stop reading is DROP_SIZE is reached */
            if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

			/* read next char from stream */
            read = (byte) input.read();
        }
        if(msgBytes == null){
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        return tmp;

    }

    /**
     *
     * @param metadata
     * @param node
     * @return If single node in ring, list with just node
     *          if 2 nodes in ring, list with the other node
     *          if more than 2 nodes in ring, list with the 2 following nodes
     */
    public static List<ServerInfo> getReplicas(List<ServerInfo> metadata, ServerInfo node) {
        // TODO: Use Spyros's algorithm
        return Arrays.asList();
    }

    /**
     *
     * @param metadata
     * @param node
     * @return If single node in ring, list with just node
     *          if 2 nodes in ring, list with the other node
     *          if more than 2 nodes in ring, list with the 2 preceding nodes
     */
    public static List<ServerInfo> getCoordinators(List<ServerInfo> metadata, ServerInfo node) {
        // TODO: Use Spyros's algorithm
        return Arrays.asList();
    }
}
