package app_kvServer;


import common.messages.KVMessage;

public class KVConnectionMessage implements KVMessage {

    private String command;
    private String key;
    private String value;

    public KVConnectionMessage (String message) {

    }

    public void put(String key, String value) throws KVException {

    }

    public String get(String key) throws KVException {
        return null;
    }

    @Override
    public String getKey() {
        return null;
    }

    @Override
    public String getValue() {
        return null;
    }

    @Override
    public StatusType getStatus() {
        return null;
    }

}
