package performance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * Created by akanthos on 25.11.15.
 */
class RandomKeyValue {
    HashMap<String, String> map;
    Random random;
    List<String> keys;
    public RandomKeyValue(HashMap<String, String> map) {
        this.map = map;
        random = new Random();
        keys = new ArrayList<>(map.keySet());
    }

    public String getRandomKey() {
        return keys.get( random.nextInt(keys.size()) );
    }
}
