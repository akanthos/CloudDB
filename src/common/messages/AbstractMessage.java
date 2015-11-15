package common.messages;

public interface AbstractMessage {


    public enum MessageType {
        CLIENT_MESSAGE, SERVER_MESSAGE, ECS_MESSAGE;
    }

    /**
     *
     * @return MessageType representing
     * Message's type
     */
    public abstract MessageType getMessageType ();
}

