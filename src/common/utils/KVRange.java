package common.utils;

import common.messages.KVMessageImpl;
import hashing.MD5Hash;
import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Created by akanthos on 11.11.15.
 */
public class KVRange {

    String low, high;
    private static Logger logger = Logger.getLogger(KVMessageImpl.class);

    static {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
    }

    public KVRange(String messageString) throws Exception {
        try {
            String[] msgParts = messageString.split(",");
            low = msgParts[0];
            high = msgParts[1];
        } catch (Exception e) {
            logger.error(String.format("Unable to construct KVRange from message: %s", messageString), e);
            throw new Exception("Unknown message format");
        }
    }

    public KVRange(String low, String high){
        this.low = low;
        this.high = high;
    }

    public KVRange(){
        this.low = "00000000000000000000000000000000";
        this.high = "00000000000000000000000000000000";
    }

    public String getLow() {
        return low;
    }

    public String getHigh() {
        return high;
    }

    public void setLow(String low) {
        this.low = low;
    }

    public void setHigh(String high) {
        this.high = high;
    }

    public boolean isIndexInRange(String index) {
        // the last node in the ring
        if (MD5Hash.compareIds(low, high) == 0) {
            return true;
        }
        if ( MD5Hash.compareIds(low, high) > 0){
            return (MD5Hash.compareIds(index, low) >= 0 || MD5Hash.compareIds(index, high) <= 0);
//            if ( index >= low )
//                return true;
//            else
//                return index <= high;
        }
        // all the other nodes
        else // low < high
            return ( MD5Hash.compareIds(index, low) >=0 && MD5Hash.compareIds(index, high) <= 0 );
    }

    @Override
    public String toString() {
        return String.valueOf(low) + "," + String.valueOf(high);
    }

    public boolean equals(KVRange other) {
        return (  (MD5Hash.compareIds(low, other.getLow())==0 && (MD5Hash.compareIds(high, other.getHigh())==0)) );
    }



}
