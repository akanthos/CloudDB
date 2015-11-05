package app_kvServer;

/**
 * Value Entry type in an LFU Cache
 * KV pair <Key, <Value,Frequency>>
 */
public class LfuCacheEntry {


    private String value;
    private Integer frequency;

    /**
     *
     * @param value value of the KV pair
     * @param frequency frequency of the respective key
     */
    public LfuCacheEntry(String value, Integer frequency) {
        this.value = value;
        this.frequency = frequency;
    }

    /**
     *
     * @return value of the KV pair
     */
    public String getValue() {
        return value;
    }

    /**
     * Set value field in KV pair
     * @param value
     */
    private void setValue(String value) {
        this.value = value;
    }

    /**
     *
     * @return frequency of the KV pair
     */
    public Integer getFrequency() {
        return frequency;
    }

    /**
     * Set value in frequency
     * @param frequency
     */
    private void setFrequency(Integer frequency) {
        this.frequency = frequency;
    }


}
