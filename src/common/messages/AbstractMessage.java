package common.messages;

public interface AbstractMessage {


    enum MessageType {
        CLIENT_MESSAGE, ECS_MESSAGE, SERVER_MESSAGE;
    }

    /**
     *
     * @return MessageType representing
     * Message's type
     */
    MessageType getMessageType ();
}

