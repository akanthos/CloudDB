package app_kvServer;

/**
 * Created by sreenath on 24/10/15.
 */
public class ServerCLI {

    public static void main(String[] args) {
        // TODO: parse command line args.
        KVServer kvServer = new KVServer(5678, 10, "FIFO");
    }
}
