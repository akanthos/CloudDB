package app_kvServer.replication;

import common.messages.KVPair;

import java.util.LinkedList;

/**
 * Created by akanthos on 17.12.15.
 */
public class GossipManager {
    private Updater updater;
    private ReplicationHandler replicationHandler;
    private LinkedList<KVPair> events;

    public GossipManager(ReplicationHandler replicationHandler) {
        this.replicationHandler = replicationHandler;
        this.events = new LinkedList<>();
        this.updater = new Updater(false, this);
        new Thread(updater).start();
    }

    // TODO: Use this method in (successful) put requests
    public synchronized void enqueuePutEvent(KVPair pair) {
        this.events.add(pair);
    }

    private synchronized void triggerUpdateReplicas() {
        LinkedList<KVPair> list = new LinkedList<>();
        for (KVPair element : this.events) {
            list.add(element);
        }
        replicationHandler.gossip(list);
    }

    public synchronized void refresh() {
        shutdown();
        this.updater = new Updater(false, this);
        new Thread(updater).start();
        // TODO: Also restart serial numbers??
    }

    public synchronized void shutdown() {
        this.updater.stop();
        this.events.clear();
    }

    class Updater implements Runnable {
        private volatile boolean stop = false;
        private GossipManager manager;

        public Updater(boolean stop, GossipManager manager) {
            this.stop = stop;
            this.manager = manager;
        }

        public void stop() {
            this.stop = true;
        }

        @Override
        public void run() {
            while (!stop) {
                try {
                    Thread.sleep(60*1000); // sleep for 1 minute
                    // TODO: Also could wait until size of list becomes greater than a number
                } catch (InterruptedException e) { }
                if (!stop)
                    manager.triggerUpdateReplicas();
            }
        }
    }


}
