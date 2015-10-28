package common.utils;

import helpers.CannotConnectException;
import helpers.ErrorMessages;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;


public class Utilities {

    private static Logger logger = Logger.getLogger(Utilities.class);
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    static {
        PropertyConfigurator.configure("conf/log.config");
    }

    /**
     * This function sends a message to the server using the established connection.
     *
     * @param msg
     * @throws CannotConnectException
     */
    public static void send(String msg, OutputStream outputStream) throws CannotConnectException {
        byte[] bytes = new StringBuilder(msg).append(Character.toString((char) 13)).toString().getBytes(StandardCharsets.US_ASCII);
        send(bytes, outputStream);
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
     * Receives an array bytes over the connection.
     *
     * @return
     * @throws CannotConnectException
     */
    public static byte[] receive(InputStream input) throws CannotConnectException, IOException {

        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;
        System.out.print((char)read);

        while(read != 13 && reading) {/* carriage return */
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
        String tt = new String(tmp, "US-ASCII").trim();

        return tmp;

    }


}
