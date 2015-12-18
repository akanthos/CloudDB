package app_kvServer.replication;

import common.messages.KVMessage;
import common.messages.KVPair;

import java.util.ArrayList;
import java.util.LinkedList;

/**
 * Created by akanthos on 17.12.15.
 */
public class UpdateManager {
    private Updater updater;
    private ReplicationHandler replicationHandler;
    private final Journal events;

    public UpdateManager(ReplicationHandler replicationHandler) {
        this.replicationHandler = replicationHandler;
        this.events = new Journal(new ArrayList<>());
        this.updater = new Updater(false, this);
        new Thread(updater).start();
    }

    // TODO: Use this method in (successful) put requests
    public void enqueuePutEvent(KVMessage message) {
        synchronized (events) {
            this.events.addEvent(message);
        }
    }

    private void triggerUpdateReplicas() {
        ArrayList<KVPair> list;
        synchronized (events) {
            list = this.events.getPairs();
            this.events.clear();
        }
        replicationHandler.gossipToReplicas(list);
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

    private class Updater implements Runnable {
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
                    Thread.sleep(10*1000); // sleep for 10 sec
                    // TODO: Also could wait until size of list becomes greater than a number
                } catch (InterruptedException e) { }
                if (!stop)
                    manager.triggerUpdateReplicas();
            }
        }
    }


}
