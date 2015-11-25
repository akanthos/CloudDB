package performance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * Picks random keys out of the given map
 * Created by akanthos on 25.11.15.
 */
class RandomKeyValue {
    HashMap<String, String> map;
    Random random;
    List<String> keys;

    /**
     * Constructor of helper class
     * @param map the map from which the keys are picked randomly
     */
    public RandomKeyValue(HashMap<String, String> map) {
        this.map = map;
        random = new Random();
        keys = new ArrayList<>(map.keySet());
    }

    /**
     * Random key picker
     * @return the random picked key
     */
    public String getRandomKey() {
        return keys.get( random.nextInt(keys.size()) );
    }
}
