package common.utils;

import common.ServerInfo;

import java.util.HashMap;

/**
 * Created by akanthos on 11.11.15.
 */
public class KVMetadata {
    HashMap<ServerInfo, KVRange> map;

    public KVMetadata() {
        this.map = new HashMap<>();
    }

    public KVMetadata(HashMap<ServerInfo, KVRange> map) {
        this.map = map;
    }

    public KVMetadata(KVMetadata another) {
        // Copy constructor
        this.map = new HashMap<>(another.map);
    }

    public String getString() {
        return null;
    }
    public HashMap<ServerInfo, KVRange> getMap() {
        return this.map;
    }

}
