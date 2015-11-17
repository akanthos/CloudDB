package common;


import common.messages.*;

import javax.activation.UnsupportedDataTypeException;
import java.util.ArrayList;
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

    public static byte[] toByteArray(KVMessageImpl message) {

        StringBuilder messageStr = new StringBuilder(CLIENT_MESSAGE + HEAD_DLM +message.getStatus().ordinal() + HEAD_DLM
                + message.getKey() + HEAD_DLM + message.getValue());

        if (message.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE){
            // add metadata
            messageStr.append(HEAD_DLM);
            for (ServerInfo server : message.getMetadata()) {
                messageStr.append(server.getAddress()+SUB_DLM1+server.getServerPort()+SUB_DLM1
                        +server.getFromIndex()+SUB_DLM1+server.getToIndex());
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

    public static byte[] toByteArray(KVAdminMessageImpl message) {
        StringBuilder messageStr = null ;
        // TODO: Make a proper serialization like in the client case
//        = new StringBuilder(ECS_MESSAGE + HEAD_DLM +message.getStatus().ordinal() + HEAD_DLM
//                + message.getKey() + HEAD_DLM + message.getValue());
//
//        if (message.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE){
//            // add metadata
//            messageStr.append(HEAD_DLM);
//            for (ServerInfo server : message.getMetadata()) {
//                messageStr.append(server.getAddress()+SUB_DLM1+server.getServerPort()+SUB_DLM1
//                        +server.getFromIndex()+SUB_DLM1+server.getToIndex());
//                messageStr.append(SUB_DLM2);
//            }
//        }
        byte[] bytes = messageStr.toString().getBytes();
        byte[] ctrBytes = new byte[] { RETURN };
        byte[] tmp = new byte[bytes.length + ctrBytes.length];
        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
        return tmp;
    }

    public static byte[] toByteArray(KVServerMessageImpl message) {

        StringBuilder messageStr = new StringBuilder(SERVER_MESSAGE + HEAD_DLM + message.getStatus().ordinal());
        if (message.getStatus().equals(KVServerMessage.StatusType.MOVE_DATA)) {
            messageStr.append(HEAD_DLM + message.getKVPairs().size());
            for (KVPair pair : message.getKVPairs()) {
                messageStr.append(HEAD_DLM).append(pair.getKey() + SUB_DLM1 + pair.getValue());
            }
        }
        if (message.getStatus().equals(KVServerMessage.StatusType.MOVE_DATA_SUCCESS)) {

        }
        byte[] bytes = messageStr.toString().getBytes();
        byte[] ctrBytes = new byte[] { RETURN };
        byte[] tmp = new byte[bytes.length + ctrBytes.length];
        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
        return tmp;
    }

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
                    // TODO: Do a proper deserialization like in the client case
                    retrievedMessage = new KVAdminMessageImpl();
                    break;

                case SERVER_MESSAGE:
                    retrievedMessage = new KVServerMessageImpl();
                    if (tokens[1] != null) {// status
                        int statusNum = Integer.parseInt(tokens[1]);
                        ((KVServerMessage)retrievedMessage).setStatus( KVServerMessage.StatusType.values()[statusNum] );
                    }
                    if (tokens.length >= 3) {
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
                    // TODO: Maybe return an error message instead of null??
                    break;

            }
        }
        return retrievedMessage;
    }

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

    private static List<ServerInfo> getMetaData(String metaDataStr) {
        List<ServerInfo> KVServerList = new ArrayList<>();
        String[] tokens = metaDataStr.split(SUB_DLM2);
        for (String serverInfoStr : tokens) {
            String[] serverInfoTokens = serverInfoStr.split(SUB_DLM1);
            ServerInfo serverInfo = new ServerInfo(serverInfoTokens[0],
                    Integer.parseInt(serverInfoTokens[1]));
            serverInfo.setFromIndex(Long.valueOf(serverInfoTokens[2]));
            serverInfo.setToIndex(Long.valueOf(serverInfoTokens[3]));
            KVServerList.add(serverInfo);
        }
        return KVServerList;
    }
}
