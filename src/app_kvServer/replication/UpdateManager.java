package app_kvServer.replication;

import common.messages.KVPair;

import java.util.LinkedList;

/**
 * Created by akanthos on 17.12.15.
 */
public class UpdateManager {
    private Updater updater;
    private ReplicationHandler replicationHandler;
    private LinkedList<KVPair> events;

    public UpdateManager(ReplicationHandler replicationHandler) {
        this.replicationHandler = replicationHandler;
        this.events = new LinkedList<>();
        this.updater = new Updater(false, this);
        new Thread(updater).start();
    }

    public synchronized void enqueuePutEvent(KVPair pair) {
        this.events.add(pair);
    }

    private void triggerUpdateReplicas() {
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
    }

    public void shutdown() {
        this.updater.stop();
        this.events.clear();
    }

    class Updater implements Runnable {
        private volatile boolean stop = false;
        private UpdateManager manager;

        public Updater(boolean stop, UpdateManager manager) {
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
                } catch (InterruptedException e) { }
                if (!stop)
                    manager.triggerUpdateReplicas();
            }
        }
    }


}
