package app_kvEcs;

import common.messages.TextMessage;

public interface ECSClientListener {

    public enum Status{CONNECTED, DISCONNECTED, CONNECTION_LOST};

    public void handleNewMessage(TextMessage msg);

    public void handleStatus(Status status);

}