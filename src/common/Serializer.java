package common;


import app_kvEcs.ECSCommand;
import common.messages.*;
import common.utils.KVRange;

import javax.activation.UnsupportedDataTypeException;
import java.io.UnsupportedEncodingException;
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

    /**
     * Convert a ECS Message Object (KVAdminMessageImpl)
     *
     * @param message message to be sent
     * @return
     */
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
                msg.append( server.getAddress() + SUB_DLM1 + server.getServerPort() + SUB_DLM1
                        + server.getFromIndex() + SUB_DLM1 + server.getToIndex() );
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
            msg.append( HEAD_DLM + message.getRange().getLow() + HEAD_DLM
                    + message.getRange().getHigh() + HEAD_DLM
                    + server.getAddress() + HEAD_DLM + server.getServerPort() );

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
                    if (((KVAdminMessageImpl)retrievedMessage).getStatus()== (KVAdminMessage.StatusType.INIT)) {
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
                    } else if (((KVAdminMessageImpl)retrievedMessage).getStatus() == (KVAdminMessage.StatusType.MOVE_DATA)) {
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
                    }
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

//    public static void main(String[] args) {
//
//        ServerInfo si = new ServerInfo("salami", 23, new KVRange());
//        KVRange rangeToMove = new KVRange(0L, Long.MAX_VALUE/2);
//
//        List<ServerInfo> m = new ArrayList<>();
////        m.add(si);
//
//        KVAdminMessageImpl message =
//                new KVAdminMessageImpl(KVAdminMessage.StatusType.INIT,
//                        m, 10, "FIFO");
//
//        byte[] bytesToSend = toByteArray(message);
//        try {
//            KVAdminMessageImpl receivedMessage = (KVAdminMessageImpl) toObject(bytesToSend);
//            System.out.println("Salami " + receivedMessage.getStatus().toString());
//        } catch (UnsupportedDataTypeException e) {
//            e.printStackTrace();
//        }
//
//    }

    public static void main (String[] args) throws UnsupportedEncodingException, UnsupportedDataTypeException {

        ServerInfo server1 = new ServerInfo("127.0.0.1", 500, new KVRange(1111, 22222));
        ServerInfo server2 = new ServerInfo("127.0.0.1", 502, new KVRange(33333, 44444));
        ServerInfo server3 = new ServerInfo("127.0.0.1", 504, new KVRange(33333, 44444));
        List<ServerInfo> list = new ArrayList<ServerInfo>();
        list.add(server1);
        KVAdminMessageImpl msg = new KVAdminMessageImpl();
        msg.setMetadata(list);
        msg.setCacheSize(11);
        msg.setDisplacementStrategy("LRU");
        msg.setStatus(KVAdminMessage.StatusType.INIT);

        byte[] arr = toByteArray(msg);

        System.out.println(new String(arr,"UTF-8"));
        System.out.println("PART_2");
        AbstractMessage abstractMessage = Serializer.toObject(arr);


    }
}
