package common.messages;

/**
 * Created by aacha on 11/16/2015.
 */
public class KVPair {
    private String key, value;
    public KVPair(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
