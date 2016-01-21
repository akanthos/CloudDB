package common;


import app_kvServer.ClientSubscription;
import common.messages.*;
import common.utils.KVRange;
import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import javax.activation.UnsupportedDataTypeException;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class Serializer {
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
        StringBuilder messageStr = new StringBuilder(Constants.CLIENT_MESSAGE + Constants.HEAD_DLM +message.getStatus().ordinal() + Constants.HEAD_DLM
                + message.getKey() + Constants.HEAD_DLM + message.getValue() +Constants.HEAD_DLM + message.getAddress() + Constants.HEAD_DLM + message.getPort().toString());

        if (message.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE){
            // add metadata
            messageStr.append(Constants.HEAD_DLM);
            for (ServerInfo server : message.getMetadata()) {
                messageStr.append(server.getAddress()).append(Constants.SUB_DLM1).append(server.getServerPort()).append(Constants.SUB_DLM1).append(server.getFromIndex()).append(Constants.SUB_DLM1).append(server.getToIndex());
                messageStr.append(Constants.SUB_DLM2);
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
        StringBuilder messageStr = new StringBuilder(Constants.SERVER_MESSAGE + Constants.HEAD_DLM + message.getStatus().ordinal());
        if (message.getStatus().equals(KVServerMessage.StatusType.MOVE_DATA)
                || message.getStatus().equals(KVServerMessage.StatusType.GOSSIP)
                || message.getStatus().equals(KVServerMessage.StatusType.REPLICATE)) {
            messageStr.append(Constants.HEAD_DLM).append(message.getKVPairs().size());
            for (KVPair pair : message.getKVPairs()) {
                messageStr.append(Constants.HEAD_DLM).append(pair.getKey()).append(Constants.SUB_DLM1).append(pair.getValue());
            }
            // non empty list of subscribers
            if (!message.getSubscribers().isEmpty()){
                messageStr.append(Constants.HEAD_DLM);
                for (Map.Entry<String, ArrayList<ClientSubscription>> entry : message.getSubscribers().entrySet()) {
                    String key = entry.getKey();
                    ArrayList<ClientSubscription> subscribers = entry.getValue();
                    messageStr.append(key).append(Constants.SUB_DLM3);
                    for (ClientSubscription sub : subscribers ){
                        messageStr.append(sub.getAddress()).append(":").append(sub.getPort()).append(":").append(sub.getInterestsOrdinal()).append(Constants.SUB_DLM1);
                    }
                    messageStr.append(Constants.SUB_DLM2);
                }
            }
        } /*else if (message.getStatus().equals(KVServerMessage.StatusType.GOSSIP)
                || message.getStatus().equals(KVServerMessage.StatusType.REPLICATE)) {
            messageStr.append(HEAD_DLM).append(message.getKVPairs().size());
            for (KVPair pair : message.getKVPairs()) {
                messageStr.append(HEAD_DLM).append(pair.getKey()).append(SUB_DLM1).append(pair.getValue());
            }
        } */else if (message.getStatus().equals(KVServerMessage.StatusType.HEARTBEAT)) {
            messageStr.append(Constants.HEAD_DLM);
            messageStr.append(message.getReplicaID());
            messageStr.append(Constants.HEAD_DLM);
            messageStr.append(df.format(message.getTimeOfSendingMsg()));
        }
        byte[] bytes = messageStr.toString().getBytes();
        byte[] ctrBytes = new byte[] { RETURN };
        byte[] tmp = new byte[bytes.length + ctrBytes.length];
        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
        return tmp;
    }

    /**
     * Admin (ECSInterface) message serializer
     *
     * @param message Outgoing admin message
     * @return byte array representation of outgoing admin message
     */
    public static byte[] toByteArray(KVAdminMessageImpl message) {
        // message : <TypeOfMessage>(int)-- <StatusType>(number)--list of metadata
        // data/fromindex--toindex-- to_serverInfo
        StringBuilder msg = new StringBuilder(Constants.ECS_MESSAGE + Constants.HEAD_DLM + message.getStatus()
                .ordinal());
        // Extra MetaData information for the cases of INIT && MOVE_DATA.
        if (message.getStatus() == KVAdminMessage.StatusType.INIT
                || message.getStatus() == KVAdminMessage.StatusType.UPDATE_METADATA) {
            // add metadata (List)
            msg.append(Constants.HEAD_DLM);
            //Message_Data => MetaData(List)
            for (ServerInfo server : message.getMetadata()) {
                msg.append(server.getAddress()).append(Constants.SUB_DLM1).append(server.getServerPort()).append(Constants.SUB_DLM1).append(server.getFromIndex()).append(Constants.SUB_DLM1).append(server.getToIndex()).append(Constants.SUB_DLM1).append(Constants.SUB_DLM1);
                msg.append(Constants.SUB_DLM2);
            }

            if (message.getStatus() == KVAdminMessage.StatusType.INIT) {
                msg.append(Constants.HEAD_DLM);
                msg.append(message.getCacheSize());
                msg.append(Constants.HEAD_DLM);
                msg.append(message.getDisplacementStrategy());
                msg.append(Constants.HEAD_DLM);
            }

        } else if (message.getStatus() == KVAdminMessage.StatusType.MOVE_DATA
                    || message.getStatus() == KVAdminMessage.StatusType.REPLICATE_DATA
                    || message.getStatus() == KVAdminMessage.StatusType.RESTORE_DATA
                    || message.getStatus() == KVAdminMessage.StatusType.REMOVE_DATA) {
            // add the from and to and the server info
            ServerInfo server = message.getServerInfo();
            //Message_Data = Information for the move server
            msg.append(Constants.HEAD_DLM).append(message.getRange().getLow()).append(Constants.HEAD_DLM).append(message.getRange().getHigh()).append(Constants.HEAD_DLM).append(server.getAddress()).append(Constants.HEAD_DLM).append(server.getServerPort());

        } else if (message.getStatus() == KVAdminMessage.StatusType.SERVER_FAILURE) {
            // add the failed message server details
            ServerInfo server = message.getFailedServerInfo();
            msg.append(Constants.HEAD_DLM).append(server.getServerRange().getLow()).append(Constants.HEAD_DLM).append(server.getServerRange().getHigh()).append(Constants.HEAD_DLM).append(server.getAddress()).append(Constants.HEAD_DLM).append(server.getServerPort());
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
     *
     * @param objectByteStream the byte array corresponding to the incoming message
     * @return an abstract message that can be downcasted to a more specific message type
     * @throws UnsupportedDataTypeException
     */
    public static AbstractMessage toObject(byte[] objectByteStream) throws UnsupportedDataTypeException {
        String message = new String(objectByteStream).trim();
        String[] tokens = message.split(Constants.HEAD_DLM);
        AbstractMessage retrievedMessage = null;
        logger.info("Decrypting message... : " + message);
        // tokens[0] => message_type
        if (tokens[0] != null) {
            AbstractMessage.MessageType messageType = toMessageType(tokens[0]);
            logger.info("Found message type");
            switch (messageType) {
                case CLIENT_MESSAGE:
                    retrievedMessage = new KVMessageImpl(tokens);
                    break;
                case ECS_MESSAGE:
                    retrievedMessage = new KVAdminMessageImpl(tokens);
                    break;
                case SERVER_MESSAGE:
                    retrievedMessage = new KVServerMessageImpl(tokens);
                    break;
                default:
                    retrievedMessage = new KVServerMessageImpl();
                    ((KVServerMessage) retrievedMessage).setStatus(KVServerMessage.StatusType.GENERAL_ERROR);
                    break;
            }
        }
        logger.info("Returning message");
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
        if (msgType.equals(Constants.CLIENT_MESSAGE))
            return AbstractMessage.MessageType.CLIENT_MESSAGE;
        else if (msgType.equals(Constants.ECS_MESSAGE))
            return AbstractMessage.MessageType.ECS_MESSAGE;
        else if (msgType.equals(Constants.SERVER_MESSAGE))
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
    public static List<ServerInfo> getMetaData(String metaDataStr) {
        if (!metaDataStr.equals("")) {
            List<ServerInfo> KVServerList = new ArrayList<>();
            String[] tokens = metaDataStr.split(Constants.SUB_DLM2);
            for (String serverInfoStr : tokens) {
                String[] serverInfoTokens = serverInfoStr.split(Constants.SUB_DLM1);
                ServerInfo serverInfo = new ServerInfo(serverInfoTokens[0],
                        Integer.parseInt(serverInfoTokens[1]));
                serverInfo.setServerRange(new KVRange( serverInfoTokens[2], serverInfoTokens[3]));
                KVServerList.add(serverInfo);
            }
            return KVServerList;
        } else {
            return new ArrayList<>();
        }
    }

    public static Map<String, ArrayList<ClientSubscription>> getSubscribers(String subscriberStr) {
        Map<String, ArrayList<ClientSubscription>> SubMap = new HashMap<>();
        if (!subscriberStr.equals("")){
            String[] tokens = subscriberStr.split(Constants.SUB_DLM2);
            for (String subPair: tokens) {
                if (subPair.isEmpty())
                    continue;
                String[] keypair = subPair.split(Constants.SUB_DLM3);
                ArrayList<String> pairIPs = new ArrayList<String>(Arrays.asList(keypair[1].split(Constants.SUB_DLM1)) );
                ArrayList<ClientSubscription> clients = new ArrayList<>();
                for (String pair : pairIPs){
                    String[] tmpPair = pair.split(":");
                    ClientSubscription.Interest interest = ClientSubscription.Interest.values()[Integer.parseInt(tmpPair[2])];
                    clients.add( new ClientSubscription(tmpPair[0], Integer.parseInt(tmpPair[1]), interest) );
                }
                SubMap.put(keypair[0], clients);
            }
        }
        return SubMap;
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

//        KVAdminMessageImpl m = new KVAdminMessageImpl(KVAdminMessage.StatusType.RESTORE_DATA, new KVRange(0, 10), new ServerInfo("localhost", 500));
//        byte[] bytes = toByteArray(m);
//        System.out.println(new String(bytes,"UTF-8"));
//        System.out.println("PART_2");
//        AbstractMessage abstractMessage = Serializer.toObject(bytes);
//        if (abstractMessage.getMessageType().equals(AbstractMessage.MessageType.ECS_MESSAGE)) {
//            m = (KVAdminMessageImpl) abstractMessage;
//        }
//        KVServerMessageImpl kvServerMessage = new KVServerMessageImpl("127.0.0.1:50036", new Date(), KVServerMessage.StatusType.HEARTBEAT);

        KVMessageImpl kvMessage = new KVMessageImpl("aeas", "asdf", KVMessage.StatusType.SUBSCRIBE_SUCCESS);


        byte[] bytes = toByteArray(kvMessage);

        AbstractMessage abstractMessage = Serializer.toObject(bytes);
        if (abstractMessage.getMessageType().equals(AbstractMessage.MessageType.CLIENT_MESSAGE)) {
            KVMessageImpl kvMessage2 = (KVMessageImpl) abstractMessage;
            System.out.println("asd");
        }
    }
}
