package app_kvServer;


public class LfuCacheEntry {


    private String value;
    private Integer frequency;

    public LfuCacheEntry(String value, Integer frequency) {
        this.value = value;
        this.frequency = frequency;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Integer getFrequency() {
        return frequency;
    }

    public void setFrequency(Integer frequency) {
        this.frequency = frequency;
    }


}
