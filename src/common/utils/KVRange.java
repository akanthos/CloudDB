package common.utils;

/**
 * Created by akanthos on 11.11.15.
 */
public class KVRange {
    Integer low, high;

    public KVRange(Integer low, Integer high){
        this.low = low;
        this.high = high;
    }

    public Integer getLow() {
        return low;
    }
    public Integer getHigh() {
        return high;
    }

    public boolean isInRange(Integer key) {
        if (key > low && key <= high) {
            return true;
        } else {
            return false;
        }
    }
}
