package common.messages;

import app_kvServer.ClientSubscription;
import common.Serializer;
import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import javax.activation.UnsupportedDataTypeException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by aacha on 11/16/2015.
 */
public class KVServerMessageImpl implements KVServerMessage {

    private List<KVPair> kvPairs;
    private StatusType status;
    private static final DateFormat df = new SimpleDateFormat("dd/MM/yyyy kk:mm:ss.SSS z");
    //Map: <key><List of subscribers IPs>
    private Map<String, ArrayList<ClientSubscription>> subscribers =  new HashMap<>();

    /**
     * Information related to heartbeat messages
     */
    private Date timeOfSendingMsg;

    /**
     * Information related to replica messages.
     * Other variables used: sourceIP (coordinator IP), kvPairs (list of pairs that needs to be replicated)
     */
    private String replicaID;

    private static Logger logger = Logger.getLogger(KVMessageImpl.class);

    static {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
    }

    /**
     * Default constructor
     */
    public KVServerMessageImpl() {

    }

    public KVServerMessageImpl(String[] tokens) throws UnsupportedDataTypeException {
        logger.info("Parsing SERVER message");
        if (tokens[1] != null) {// status
            int statusNum = Integer.parseInt(tokens[1]);
            this.setStatus( KVServerMessage.StatusType.values()[statusNum] );
        }
        if ((this.getStatus() == (KVServerMessage.StatusType.HEARTBEAT))) {
            logger.info("It's a heartbeat message");
            this.setReplicaID(tokens[2].trim());
            try {
                this.setTimeOfSendingMsg(df.parse(tokens[3].trim()));
                logger.info("Parsed heartbeat datetime");
            } catch (ParseException e) {
                logger.error(String.format("Unsupported date format in message. Complete message: %s", tokens[3].trim()), e);
                throw new UnsupportedDataTypeException("Unable to parse heartbeat message");
            }
            logger.info("Parsed heartbeat datetime");
        } else if ((this.getStatus() == (KVServerMessage.StatusType.GOSSIP))
                || (this.getStatus() == (KVServerMessage.StatusType.REPLICATE))
                || (this.getStatus() == (KVServerMessage.StatusType.MOVE_DATA))) {
            if (tokens[2] != null) { // Data length and data
                int dataLength = Integer.parseInt(tokens[2]);
                ArrayList<KVPair> kvPairs = new ArrayList<>(dataLength);
                if (tokens.length >= dataLength + 3) {
                    int i = 0;
                    for (i = 0; i < dataLength; i++) {
                        String[] kv = tokens[i + 3].split(Serializer.SUB_DLM1);
                        if (kv.length == 2) {
                            kvPairs.add(new KVPair(kv[0], kv[1]));
                        }
                    }
                    if (tokens.length == dataLength + 4){
                        this.setSubscribers(Serializer.getSubscribers(tokens[i+3]));
                    }
                }
                this.setKVPairs(kvPairs);
            }
        }
    }

    /**
     * Constructor which sets the status for simple messages
     * like MOVE_DATA_SUCCESS or error message
     *
     * @param status the status of the message
     */
    public KVServerMessageImpl(StatusType status) {
        this.status = status;
    }

    /**
     * Constructor which sets the status and the key-value pairs
     * to be sent - To be used for MOVE_DATA command
     *
     * @param kvPairs the key-value pairs of the message
     * @param status the status of the message
     */
    public KVServerMessageImpl(ArrayList<KVPair> kvPairs, HashMap<String, ArrayList<ClientSubscription>> subscriptions, StatusType status) {
        this.kvPairs = kvPairs;
        this.status = status;
        this.subscribers = subscriptions;
    }

    /**
     * Constructor which sets the status and the relevant information for
     * a HEARTBEAT message
     * @param coordinatorID the server that sends the heartbeat
     * @param timeOfSendingMsg timestamp of sending the message
     * @param status the status of the message
     */
    public KVServerMessageImpl(String coordinatorID, Date timeOfSendingMsg, StatusType status) {
        this.replicaID = coordinatorID;
        this.timeOfSendingMsg = timeOfSendingMsg;
        this.status = status;
    }

    /**
     * Constructor which sets the status and the key-value pairs
     * to be sent - To be used for REPLICATE command
     * @param kvPairs the key-value pairs to be replicated
     * @param status the status of the message (REPLICATE or GOSSIP)
     */
    public KVServerMessageImpl(List<KVPair> kvPairs, StatusType status) {
        this.kvPairs = kvPairs;
        this.status = status;
    }

    @Override
    public List<KVPair> getKVPairs() {
        return kvPairs;
    }

    @Override
    public void setKVPairs(List<KVPair> kvPair) {
        this.kvPairs = kvPair;
    }

    @Override
    public StatusType getStatus() {
        return status;
    }

    @Override
    public void setStatus(StatusType statusType) {
        this.status = statusType;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.SERVER_MESSAGE;
    }

    public List<KVPair> getKvPairs() {
        return kvPairs;
    }

    public void setKvPairs(List<KVPair> kvPairs) {
        this.kvPairs = kvPairs;
    }

    @Override
    public Date getTimeOfSendingMsg() {
        return timeOfSendingMsg;
    }

    @Override
    public void setTimeOfSendingMsg(Date timeOfSendingMsg) {
        this.timeOfSendingMsg = timeOfSendingMsg;
    }

    public String getReplicaID() {
        return replicaID;
    }

    public void setReplicaID(String replicaID) {
        this.replicaID = replicaID;
    }

    @Override
    public Map<String, ArrayList<ClientSubscription>> getSubscriptions() {
        return this.subscribers;
    }

    public Map<String, ArrayList<ClientSubscription>> getSubscribers() {
        return subscribers;
    }

    public void setSubscribers(Map<String, ArrayList<ClientSubscription>> subscribers) {
        this.subscribers = subscribers;
    }
}
