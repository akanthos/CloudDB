package app_kvServer.replication;

import common.messages.KVPair;
import sun.awt.image.ImageWatched;

import java.util.LinkedList;

/**
 * Created by akanthos on 17.12.15.
 */
public class UpdateEvent implements Comparable {
    private Integer serialNumber;
    private LinkedList<KVPair> pairs;

    public UpdateEvent(Integer serialNumber, LinkedList<KVPair> pairs) {
        this.serialNumber = serialNumber;
        this.pairs = pairs;
    }

    public LinkedList<KVPair> getPairs() {
        return pairs;
    }

    public Integer getSerialNumber() {
        return serialNumber;
    }

    @Override
    public int compareTo(Object o) {
        UpdateEvent other = (UpdateEvent) o;
        return Integer.compare(this.serialNumber, other.serialNumber);
    }
}
