package app_kvServer;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

/**
 * Created by akanthos on 10.12.15.
 */
public class ReplicaHandler {

    HashMap<Integer, Replica> replicas;
    KVPersistenceEngine replicatedData;

    public ReplicaHandler() {
        replicas = new HashMap<>();
        replicatedData = new KVPersistenceEngine();
    }

    public synchronized void registerReplica(int replicaNumber, String sourceIP) {
        Replica replica = new Replica(replicaNumber, sourceIP);
        replicas.put(replica.getReplicaNumber(), replica);
    }

    public synchronized void deregisterReplica(int replicaNumber) {
        // TODO: Remove replicated keys from DB
        replicas.remove(replicaNumber);
    }


    public void heartbeat(String sourceIP, int replicaNumber) throws TimeoutException {
        replicas.get(replicaNumber).heartbeat(new Date());
    }
}
