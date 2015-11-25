package performance;

import java.util.Random;

/**
 * Random string generator
 * Created by akanthos on 25.11.15.
 */
class RandomString {

    private static final char[] symbols;
    private final char[] buf;
    private final Random random = new Random();

    static {
        StringBuilder tmp = new StringBuilder();
        for (char ch = '0'; ch <= '9'; ++ch)
            tmp.append(ch);
        for (char ch = 'a'; ch <= 'z'; ++ch)
            tmp.append(ch);
        symbols = tmp.toString().toCharArray();
    }

    /**
     * Constructor for this class.
     * @param length The length of the produced strings
     */
    public RandomString(int length) {
        if (length < 1)
            throw new IllegalArgumentException("length < 1: " + length);
        buf = new char[length];
    }

    /**
     * Creates a random string
     * @return the random string that was created
     */
    public String nextString() {
        for (int idx = 0; idx < buf.length; ++idx)
            buf[idx] = symbols[random.nextInt(symbols.length)];
        return new String(buf);
    }
}
