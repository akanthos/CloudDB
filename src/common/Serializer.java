package common;


import app_kvServer.KVServer;
import common.messages.*;
import common.utils.KVRange;
import common.utils.Utilities;
import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import javax.activation.UnsupportedDataTypeException;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class Serializer {

    // message type
    private static final String ECS_MESSAGE = "0";
    private static final String CLIENT_MESSAGE = "1";
    private static final String SERVER_MESSAGE = "2";
    // delimiters
    private static final String HEAD_DLM = "##";
    private static final String SUB_DLM1 = "&&";
    private static final String SUB_DLM2 = "%%";

    private static final char RETURN = 0x0D;
    private static final DateFormat df = new SimpleDateFormat("dd/MM/yyyy kk:mm:ss.SSS z");

    private static Logger logger = Logger.getLogger(KVMessageImpl.class);

    static {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
    }

    /**
     * Client message object serializer (KVAdminMessageImpl)
     *
     * @param message client message to be sent
     * @return
     */
    public static byte[] toByteArray(KVMessageImpl message) {

        StringBuilder messageStr = new StringBuilder(CLIENT_MESSAGE + HEAD_DLM +message.getStatus().ordinal() + HEAD_DLM
                + message.getKey() + HEAD_DLM + message.getValue());

        if (message.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE){
            // add metadata
            messageStr.append(HEAD_DLM);
            for (ServerInfo server : message.getMetadata()) {
                messageStr.append(server.getAddress()).append(SUB_DLM1).append(server.getServerPort()).append(SUB_DLM1).append(server.getFromIndex()).append(SUB_DLM1).append(server.getToIndex());
                messageStr.append(SUB_DLM2);
            }
        }
        byte[] bytes = messageStr.toString().getBytes();
        byte[] ctrBytes = new byte[] { RETURN };
        byte[] tmp = new byte[bytes.length + ctrBytes.length];
        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
        return tmp;
    }

    /**
     * Server message object serializer (KVAdminMessageImpl)
     *
     * @param message server-to-server message to be sent
     * @return
     */
    public static byte[] toByteArray(KVServerMessageImpl message) {

        StringBuilder messageStr = new StringBuilder(SERVER_MESSAGE + HEAD_DLM + message.getStatus().ordinal());
        if (message.getStatus().equals(KVServerMessage.StatusType.MOVE_DATA)) {
            messageStr.append(HEAD_DLM).append(message.getKVPairs().size());
            for (KVPair pair : message.getKVPairs()) {
                messageStr.append(HEAD_DLM).append(pair.getKey()).append(SUB_DLM1).append(pair.getValue());
            }
        } else if (message.getStatus().equals(KVServerMessage.StatusType.HEARTBEAT)) {
            messageStr.append(HEAD_DLM);
            messageStr.append(message.getCoordinatorID());
            messageStr.append(HEAD_DLM);
            messageStr.append(df.format(message.getTimeOfSendingMsg()));
        } else if (message.getStatus().equals(KVServerMessage.StatusType.REPLICATE)) {
            messageStr.append(HEAD_DLM);
            messageStr.append(message.getCoordinatorID());
            messageStr.append(HEAD_DLM).append(message.getKVPairs().size());
            for (KVPair pair : message.getKVPairs()) {
                messageStr.append(HEAD_DLM).append(pair.getKey()).append(SUB_DLM1).append(pair.getValue());
            }

        }


        byte[] bytes = messageStr.toString().getBytes();
        byte[] ctrBytes = new byte[] { RETURN };
        byte[] tmp = new byte[bytes.length + ctrBytes.length];
        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
        return tmp;
    }

    /**
     * Admin (ECS) message serializer
     *
     * @param message Outgoing admin message
     * @return byte array representation of outgoing admin message
     */
    public static byte[] toByteArray(KVAdminMessageImpl message) {
        // message : <TypeOfMessage>(int)-- <StatusType>(number)--list of metadata
        // data/fromindex--toindex-- to_serverInfo
        StringBuilder msg = new StringBuilder(ECS_MESSAGE + HEAD_DLM + message.getStatus()
                .ordinal());
        // Extra MetaData information for the cases of INIT && MOVE_DATA.
        if (message.getStatus() == KVAdminMessage.StatusType.INIT
                || message.getStatus() == KVAdminMessage.StatusType.UPDATE_METADATA) {
            // add metadata (List)
            msg.append(HEAD_DLM);
            //Message_Data => MetaData(List)
            for (ServerInfo server : message.getMetadata()) {
                msg.append(server.getAddress()).append(SUB_DLM1).append(server.getServerPort()).append(SUB_DLM1).append(server.getFromIndex()).append(SUB_DLM1).append(server.getToIndex()).append(SUB_DLM1).append(SUB_DLM1);
                msg.append(SUB_DLM2);
            }

            if (message.getStatus() == KVAdminMessage.StatusType.INIT) {
                msg.append(HEAD_DLM);
                msg.append(message.getCacheSize());
                msg.append(HEAD_DLM);
                msg.append(message.getDisplacementStrategy());
                msg.append(HEAD_DLM);
            }

        } else if (message.getStatus() == KVAdminMessage.StatusType.MOVE_DATA) {
            // add the from and to and the server info
            ServerInfo server = message.getServerInfo();
            //Message_Data = Information for the move server
            msg.append(HEAD_DLM).append(message.getRange().getLow()).append(HEAD_DLM).append(message.getRange().getHigh()).append(HEAD_DLM).append(server.getAddress()).append(HEAD_DLM).append(server.getServerPort());

        } else if (message.getStatus() == KVAdminMessage.StatusType.SERVER_FAILURE) {
            // add the failed message server details
            ServerInfo server = message.getFailedServerInfo();
            msg.append(HEAD_DLM).append(server.getServerRange().getLow()).append(HEAD_DLM).append(server.getServerRange().getHigh()).append(HEAD_DLM).append(server.getAddress()).append(HEAD_DLM).append(server.getServerPort());
        }
        // in the case of start|stop| etc. messages we just have
        // a message : <TypeOfMessage>(int)-- <StatusType>(number)
        // convert String to bytes
        byte[] bytes = msg.toString().getBytes();
        byte[] ctrBytes = new byte[] { RETURN };
        byte[] tmp = new byte[bytes.length + ctrBytes.length];
        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
        return tmp;
    }


    /**
     * Message deserializer
     * TODO: This is getting pretty big, move this to the constructors?
     *
     * @param objectByteStream the byte array corresponding to the incoming message
     * @return an abstract message that can be downcasted to a more specific message type
     * @throws UnsupportedDataTypeException
     */
    public static AbstractMessage toObject(byte[] objectByteStream) throws UnsupportedDataTypeException {

        String message = new String(objectByteStream).trim();
        String[] tokens = message.split(HEAD_DLM);
        AbstractMessage retrievedMessage = null;
        // tokens[0] => message_type
        if (tokens[0] != null) {
            AbstractMessage.MessageType messageType = toMessageType(tokens[0]);
            switch (messageType) {
                case CLIENT_MESSAGE:
                    retrievedMessage = new KVMessageImpl();
                    if (tokens[1] != null) {// status
                        int statusNum = Integer.parseInt(tokens[1]);
                        ((KVMessageImpl)retrievedMessage).setStatus( KVMessage.StatusType.values()[statusNum] );
                    }
                    if (tokens[2] != null) { // key
                        ((KVMessageImpl)retrievedMessage).setKey(tokens[2]);

                    }
                    if (tokens.length >= 4) {
                        if (tokens[3] != null) { // value
                            ((KVMessageImpl) retrievedMessage).setValue(tokens[3].trim());
                        }
                    }
                    if (tokens.length >= 5) {
                        List<ServerInfo> metaData = getMetaData(tokens[4].trim());
                        ((KVMessageImpl) retrievedMessage).setMetadata(metaData);
                    }
                    break;

                case ECS_MESSAGE:
                    retrievedMessage = new KVAdminMessageImpl();
                    if (tokens.length>= 2 && tokens[1] != null) {
                        int statusNum = Integer.parseInt(tokens[1]);
                        ((KVAdminMessageImpl)retrievedMessage).setStatus(KVAdminMessage.StatusType.values()[statusNum]);
                    }
                    if (((KVAdminMessageImpl)retrievedMessage).getStatus()== (KVAdminMessage.StatusType.INIT)
                            || ((KVAdminMessageImpl)retrievedMessage).getStatus()== (KVAdminMessage.StatusType.UPDATE_METADATA)) {
                        if (tokens.length>= 3 && tokens[2] != null) {// is always the key
                            List<ServerInfo> metaData = getMetaData(tokens[2].trim());
                            ((KVAdminMessageImpl)retrievedMessage).setMetadata(metaData);
                        }
                        if (tokens.length>= 4 && tokens[3] != null) {
                            Integer cacheSize = Integer.parseInt(tokens[3].trim());
                            ((KVAdminMessageImpl)retrievedMessage).setCacheSize(cacheSize);
                        }
                        if (tokens.length>= 5 && tokens[4] != null) {
                            ((KVAdminMessageImpl)retrievedMessage).setDisplacementStrategy(tokens[4]);
                        }
                    } else if (((KVAdminMessageImpl)retrievedMessage).getStatus() == (KVAdminMessage.StatusType.MOVE_DATA)
                            || ((KVAdminMessageImpl)retrievedMessage).getStatus() == (KVAdminMessage.StatusType.REPLICATE)) {
                        if (tokens.length>= 3 && tokens[2] != null) {
                            //((KVAdminMessageImpl) retrievedMessage).setRange(new KVRange());
                            ((KVAdminMessageImpl) retrievedMessage).setLow(Long.valueOf(tokens[2].trim()));
                        }
                        if (tokens.length>= 4 && tokens[3] != null) {
                            ((KVAdminMessageImpl)retrievedMessage).setHigh(Long.valueOf(tokens[3].trim()));
                        }
                        if (tokens.length>= 6 && tokens[4] != null && tokens[5] != null ) {
                            ServerInfo toNode = new ServerInfo(tokens[4],Integer.parseInt(tokens[5]));
                            ((KVAdminMessageImpl)retrievedMessage).setServerInfo(toNode);
                        }
                    } else if (((KVAdminMessageImpl)retrievedMessage).getStatus() == (KVAdminMessage.StatusType.SERVER_FAILURE)) {
                        KVRange range = new KVRange();
                        if (tokens.length>= 3 && tokens[2] != null) {
                            range.setLow(Long.valueOf(tokens[2].trim()));
                        }
                        if (tokens.length>= 4 && tokens[3] != null) {
                            range.setHigh(Long.valueOf(tokens[3].trim()));
                        }
                        if (tokens.length>= 6 && tokens[4] != null && tokens[5] != null ) {
                            ServerInfo toNode = new ServerInfo(tokens[4],Integer.parseInt(tokens[5]));
                            toNode.setServerRange(range);
                            ((KVAdminMessageImpl)retrievedMessage).setFailedServerInfo(toNode);
                        }
                    }
                    break;
                case SERVER_MESSAGE:
                    retrievedMessage = new KVServerMessageImpl();
                    if (tokens[1] != null) {// status
                        int statusNum = Integer.parseInt(tokens[1]);
                        ((KVServerMessage)retrievedMessage).setStatus( KVServerMessage.StatusType.values()[statusNum] );
                    }
                    if ((((KVServerMessageImpl) retrievedMessage).getStatus() == (KVServerMessage.StatusType.HEARTBEAT))) {
                        ((KVServerMessageImpl) retrievedMessage).setCoordinatorID(tokens[2].trim());
                        try {
                            ((KVServerMessageImpl) retrievedMessage).setTimeOfSendingMsg(df.parse(tokens[3].trim()));
                        } catch (ParseException e) {
                            logger.error(String.format("Unsupported date format in message. Complete message: %s", tokens[3].trim()), e);
                            throw new UnsupportedDataTypeException("Unable to parse heartbeat message");
                        }
                    } else if (((((KVServerMessageImpl)retrievedMessage).getStatus() == (KVServerMessage.StatusType.REPLICATE)))) {
                        ((KVServerMessageImpl) retrievedMessage).setCoordinatorID(tokens[2].trim());
                        if (tokens[3] != null) { // Data length and data
                            int dataLength = Integer.parseInt(tokens[3]);
                            ArrayList<KVPair> kvPairs = new ArrayList<>(dataLength);
                            for (int i = 0; i < dataLength; i++) {
                                String[] kv = tokens[i + 4].split(SUB_DLM1);
                                if (kv.length == 2) {
                                    kvPairs.add(new KVPair(kv[0], kv[1]));
                                }
                            }
                            ((KVServerMessage) retrievedMessage).setKVPairs(kvPairs);
                        }
                    }
                    // TODO:Use status codes instead of token length.
                    else if (tokens.length >= 3) {
                        if (tokens[2] != null) { // Data length and data
                            int dataLength = Integer.parseInt(tokens[2]);
                            ArrayList<KVPair> kvPairs = new ArrayList<>(dataLength);
                            if (tokens.length == dataLength + 3) {
                                for (int i = 0; i < dataLength; i++) {
                                    String[] kv = tokens[i + 3].split(SUB_DLM1);
                                    if (kv.length == 2) {
                                        kvPairs.add(new KVPair(kv[0], kv[1]));
                                    }
                                }
                            }
                            ((KVServerMessage) retrievedMessage).setKVPairs(kvPairs);
                        }
                    }
                    break;
                default:
                    retrievedMessage = new KVServerMessageImpl();
                    ((KVServerMessage) retrievedMessage).setStatus(KVServerMessage.StatusType.GENERAL_ERROR);
                    break;

            }
        }
        return retrievedMessage;
    }

    /**
     * Extracts the abstract message type from the string representing it in the
     * incoming message
     *
     * @param msgType the string representing the abstract message type
     * @return the abstract message type extracted
     * @throws UnsupportedDataTypeException
     */
    private static AbstractMessage.MessageType toMessageType(String msgType) throws UnsupportedDataTypeException {

        if (msgType.equals(CLIENT_MESSAGE))
            return AbstractMessage.MessageType.CLIENT_MESSAGE;
        else if (msgType.equals(ECS_MESSAGE))
            return AbstractMessage.MessageType.ECS_MESSAGE;
        else if (msgType.equals(SERVER_MESSAGE))
            return AbstractMessage.MessageType.SERVER_MESSAGE;
        else
            throw new UnsupportedDataTypeException("Unsupported message type");

    }

    /**
     * Extracts the metadata as structured data from the string
     * representation
     *
     * @param metaDataStr the string that was part of the message
     *                    containing the metadata
     * @return A list of ServerInfo
     */
    private static List<ServerInfo> getMetaData(String metaDataStr) {
        if (!metaDataStr.equals("")) {
            List<ServerInfo> KVServerList = new ArrayList<>();
            String[] tokens = metaDataStr.split(SUB_DLM2);
            for (String serverInfoStr : tokens) {
                String[] serverInfoTokens = serverInfoStr.split(SUB_DLM1);
                ServerInfo serverInfo = new ServerInfo(serverInfoTokens[0],
                        Integer.parseInt(serverInfoTokens[1]));
                serverInfo.setServerRange(new KVRange(Long.valueOf(serverInfoTokens[2]), Long.valueOf(serverInfoTokens[3])));
                KVServerList.add(serverInfo);
            }
            return KVServerList;
        }
        else {
            return new ArrayList<>();
        }
    }


    /**
     * Main method for testing purposes
     *
     * @param args
     * @throws UnsupportedEncodingException
     * @throws UnsupportedDataTypeException
     */
    public static void main (String[] args) throws UnsupportedEncodingException, UnsupportedDataTypeException {
        // ServerInfo failedServerInfo = new ServerInfo("1.1.1.1", 1234, new KVRange(0L, 2000L));
//        List<KVPair> pairs = Arrays.asList(new KVPair("foo", "bar"), new KVPair("boo", "far"));
//        KVServerMessageImpl kvServerMessage = new KVServerMessageImpl("1", pairs, KVServerMessage.StatusType.REPLICATE);
//        byte[] bytes = toByteArray(kvServerMessage);

        KVAdminMessageImpl m = new KVAdminMessageImpl(KVAdminMessage.StatusType.SERVER_FAILURE, new ServerInfo("localhost", 500));
        byte[] bytes = toByteArray(m);
        System.out.println(new String(bytes,"UTF-8"));
        System.out.println("PART_2");
        AbstractMessage abstractMessage = Serializer.toObject(bytes);
        if (abstractMessage.getMessageType().equals(AbstractMessage.MessageType.ECS_MESSAGE)) {
            m = (KVAdminMessageImpl) abstractMessage;
        }




//        KVServerMessageImpl kvServerMessage = new KVServerMessageImpl("1", new Date(), KVServerMessage.StatusType.HEARTBEAT);
//        byte[] bytes = toByteArray(kvServerMessage);

        // String hehe = "0####127.0.0.1&&50000&&3706585719&&897794963%%127.0.0.1&&50001&&897794963&&3706585719%%";
        // byte[] arr = hehe.toString().getBytes();
        //byte[] arr = toByteArray(hehe);

//        System.out.println(new String(bytes,"UTF-8"));
//        System.out.println("PART_2");
//        AbstractMessage abstractMessage = Serializer.toObject(bytes);
//        if (abstractMessage.getMessageType().equals(AbstractMessage.MessageType.SERVER_MESSAGE)) {
//            kvServerMessage = (KVServerMessageImpl) abstractMessage;
//        }


    }


}
