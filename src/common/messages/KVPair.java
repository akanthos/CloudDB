package common.messages;

/**
 * Represents a key value pair
 * Created by aacha on 11/16/2015.
 */
public class KVPair {
    private String key, value;
    public KVPair(String key, String value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Key getter
     *
     * @return the key of the pair
     */
    public String getKey() {
        return key;
    }

    /**
     * Value getter
     *
     * @return the value of the pair
     */
    public String getValue() {
        return value;
    }
}
