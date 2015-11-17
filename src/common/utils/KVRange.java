package common.utils;

import common.messages.KVMessageImpl;
import helpers.Constants;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Created by akanthos on 11.11.15.
 */
public class KVRange {

    long low, high;
    private static Logger logger = Logger.getLogger(KVMessageImpl.class);

    static {
        PropertyConfigurator.configure(Constants.LOG_FILE_CONFIG);
    }

    public KVRange(String messageString) throws Exception {
        try {
            String[] msgParts = messageString.split(",");
            low = Long.valueOf(msgParts[0]);
            high = Long.valueOf(msgParts[1]);
        } catch (Exception e) {
            logger.error(String.format("Unable to construct KVRange from message: %s", messageString), e);
            throw new Exception("Unknown message format");
        }
    }

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

    @Override
    public String toString() {
        return String.valueOf(low) + "," + String.valueOf(high);
    }
}
