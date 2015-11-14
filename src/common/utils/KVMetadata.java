package common.utils;

import app_kvEcs.ServerInfos;

import java.util.HashMap;

/**
 * Created by akanthos on 11.11.15.
 */
public class KVMetadata {
    HashMap<KVRange, ServerInfos> map = new HashMap<>();

    public void addServer(KVRange range, ServerInfos serverInfos) {
        map.put(range, serverInfos);
    }

    public ServerInfos getServer(KVRange range) {
        return map.get(range);
    }

    public boolean doesServerExist(KVRange range) {
        return map.containsKey(range);
    }

    public HashMap<KVRange, ServerInfos> getMap() {
        return map;
    }

    public void setMap(HashMap<KVRange, ServerInfos> map) {
        this.map = map;
    }

    public String getString() {
        return null;
    }



}
