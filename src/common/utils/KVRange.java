package common.utils;

/**
 * Created by akanthos on 11.11.15.
 */
public class KVRange {

    long low, high;

    public KVRange(long low, long high){
        this.low = low;
        this.high = high;
    }

    public long getLow() {
        return low;
    }

    public long getHigh() {
        return high;
    }

    public void setLow(long low) {
        this.low = low;
    }

    public void setHigh(long high) {
        this.high = high;
    }

    public boolean isIndexInRange(long index) {
        // the last node in the ring
        if ( low > high){
            if ( index >= low )
                return true;
            else
                return index <= high;
        }
        // all the other nodes
        else
            return ( (index >= low) && (index <= high) );
    }
}
