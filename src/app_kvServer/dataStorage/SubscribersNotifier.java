package app_kvServer.dataStorage;

import app_kvServer.Messenger;

import java.util.ArrayList;

/**
 * Created by akanthos on 15.01.16.
 */
public class SubscribersNotifier implements Runnable {
    private final String key;
    private final String value;
    private final ArrayList<String> addresses;

    public SubscribersNotifier(ArrayList<String> addresses, String key, String value) {
        this.addresses = addresses;
        this.key = key;
        this.value = value;
    }

    @Override
    public void run() {
        for (String address : addresses) {
            Messenger.notifySubscriber(address, key, value);
        }
    }
}
