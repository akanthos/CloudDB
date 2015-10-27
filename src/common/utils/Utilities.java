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

/**
 * Created by sreenath on 24/10/15.
 */
public class Utilities {

    private static Logger logger = Logger.getLogger(Utilities.class);

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
    public static byte[] receive(InputStream inputStream) throws CannotConnectException {
        try {
            byte[] answer = new byte[128*1024];
            byte[] buffer = new byte[128*1024];
            Integer count;
            count = inputStream.read(buffer);
            byte[] finalAnswer = new byte[count];
            System.arraycopy(answer, 0, finalAnswer, 0, count);
            return buffer;
        } catch (UnsupportedEncodingException e) {
            logger.error(e);
            throw new CannotConnectException(ErrorMessages.ERROR_INVALID_MESSAGE_FROM_SERVER);
        }
        catch(Exception e){
            System.out.println("An Unknown Error has Occured");
            e.printStackTrace();
        }
        return null;
    }
}
