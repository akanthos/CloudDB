package common.messages;

/**
 * Interface representing an abstract message.
 * It can be either a client message, a server-to-server
 * message, or an admin (ECS) message
 */
public interface AbstractMessage {


    enum MessageType {
        CLIENT_MESSAGE, ECS_MESSAGE, SERVER_MESSAGE
    }

    /**
     *
     * @return MessageType representing
     * Message's type
     */
    MessageType getMessageType ();
}

