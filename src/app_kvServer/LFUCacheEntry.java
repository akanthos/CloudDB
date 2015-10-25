package app_kvServer;


public class LFUCacheEntry<T> {

    private Class <T> data;
    private int frequency;


    public T getData() {
        return (T) this.data;
    }
    public void setData(Class<T> data) {
        this.data = data;
    }

    public int getFrequency() {
        return frequency;
    }
    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

}
