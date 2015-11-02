package app_kvServer;


import common.messages.KVMessage;

/**
 * Custom Exception for KVServer
 */
public class KVException extends Exception {

    private static final long serialVersionUID = 1L;
    private KVMessage msg = null;

    public final KVMessage getMsg() {
        return msg;
    }

    public KVException(KVMessage msg) {
        this.msg = msg;
    }

}
