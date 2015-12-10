package app_kvServer;

import common.messages.KVPair;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Created by akanthos on 10.12.15.
 */
public class ReplicaHandler {

    HashMap<Integer, Replica> replicas;
    HashMap<Integer, KVPersistenceEngine> replicatedData;

    public ReplicaHandler() {
        replicas = new HashMap<>();
        replicatedData = new HashMap<>();
    }

    public synchronized void registerReplica(int replicaNumber,
                                             String sourceIP,
                                             List<KVPair> kvPairs) {
        Replica replica = new Replica(replicaNumber, sourceIP);
        replicas.put(replica.getReplicaNumber(), replica);
        bulkInsert(replicatedData.get(replicaNumber), kvPairs);
    }

    private void bulkInsert(KVPersistenceEngine kvPersistenceEngine, List<KVPair> kvPairs) {
        for (KVPair pair : kvPairs) {
            kvPersistenceEngine.put(pair.getKey(), pair.getValue());
        }
    }

    public synchronized void deregisterReplica(int replicaNumber) {
        replicas.remove(replicaNumber);
        replicatedData.remove(replicaNumber);
    }


    public void heartbeat(String sourceIP, int replicaNumber) {
        replicas.get(replicaNumber).heartbeat(new Date());
    }
}
