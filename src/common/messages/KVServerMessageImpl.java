package common.messages;

import jdk.internal.util.xml.impl.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by aacha on 11/16/2015.
 */
public class KVServerMessageImpl implements KVServerMessage {

    List<KVPair> kvPairs;
    StatusType status;

    public KVServerMessageImpl() {}

    public KVServerMessageImpl(ArrayList<KVPair> kvPairs) {
        this.kvPairs = kvPairs;
    }

    public KVServerMessageImpl(ArrayList<KVPair> kvPairs, StatusType status) {
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
}
