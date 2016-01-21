package app_kvServer.dataStorage;

import app_kvServer.ClientSubscription;
import app_kvServer.Messenger;
import org.apache.log4j.Logger;

import java.util.ArrayList;

/**
 * This runnable class notifies the subscribers about a change
 */
public class SubscribersNotifier implements Runnable {
    private final String key;
    private final String value;
    private final ArrayList<ClientSubscription> addresses;
    private static Logger logger = Logger.getLogger(SubscribersNotifier.class);

    /**
     * Constructor
     * @param addresses the subscribers' IP addresses
     * @param key the key associated with these subscriptions
     * @param value the updated value
     */
    public SubscribersNotifier(ArrayList<ClientSubscription> addresses, String key, String value) {
        this.addresses = addresses;
        this.key = key;
        this.value = value;
    }

    /**
     * Notifies the subscribers
     */
    @Override
    public void run() {
        for (ClientSubscription address : addresses) {
            logger.debug("Notifying address: " + address);
            Messenger.notifySubscriber(address, key, value);
        }
    }
}
