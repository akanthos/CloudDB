package common.utils;

import common.ServerInfo;

import java.util.HashMap;

/**
 * Created by akanthos on 11.11.15.
 */
public class KVMetadata {
    HashMap<KVRange, ServerInfo> map = new HashMap<>();

    public void addServer(KVRange range, ServerInfo ServerInfo) {
        map.put(range, ServerInfo);
    }

    public ServerInfo getServer(KVRange range) {
        return map.get(range);
    }

    public boolean doesServerExist(KVRange range) {
        return map.containsKey(range);
    }

    public HashMap<KVRange, ServerInfo> getMap() {
        return map;
    }

    public void setMap(HashMap<KVRange, ServerInfo> map) {
        this.map = map;
    }

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
