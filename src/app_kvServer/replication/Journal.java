package app_kvServer.replication;

import common.messages.KVMessage;
import common.messages.KVPair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by akanthos on 17.12.15.
 */
public class Journal {
    private ArrayList<KVPair> pairs;

    public Journal(ArrayList<KVPair> pairs) {
        this.pairs = pairs;
    }

    public ArrayList<KVPair> getPairs() {
        return compactPairs();
    }

    public void addEvent(KVMessage message) {
        this.pairs.add(new KVPair(message.getKey(), message.getValue()));
    }

    private ArrayList<KVPair> compactPairs() {
        HashMap<String, String> compacted = new HashMap<>();
        for (KVPair pair : pairs) {
            compacted.put(pair.getKey(), pair.getValue());
        }
        ArrayList<KVPair> result = new ArrayList<>();
        for (String key : compacted.keySet()) {
            result.add(new KVPair(key, compacted.get(key)));
        }
        return result;
    }

    public void clear() {
        this.pairs.clear();
    }

}
