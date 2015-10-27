package app_kvServer;


public class KVException extends Exception {

    private static final long serialVersionUID = 1L;
    private KVConnectionMessage msg = null;

    public final KVConnectionMessage getMsg() {
        return msg;
    }

    public KVException(KVConnectionMessage msg) {
        this.msg = msg;
    }

}
