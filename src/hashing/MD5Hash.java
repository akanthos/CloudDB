package hashing;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Hash {
    MessageDigest md5;
    private HexBinaryAdapter adapt;
    private static int base = 16;

    public MD5Hash() {
        try {
            md5 = MessageDigest.getInstance("MD5");
            adapt = new HexBinaryAdapter();
        } catch (NoSuchAlgorithmException e) {
            System.err.println("Not such algorithm.");
        }
    }

    /**
     * this method generates a hash value for a given string using MD5 hashing
     * algorithm
     *
     * @param key
     *            the key to be hashed
     * @return String the hash value
     */
    public String hash(String key) {
        byte[] array = md5.digest(key.getBytes());
        String hex = adapt.marshal(array);
        return hex;
    }

    /*
    public Long hash(String key) {
        md5.reset();
        md5.update(key.getBytes());
        byte[] digest = md5.digest();

        long h = 0;
        for (int i = 0; i < 4; i++) {
            h <<= 8;
            h |= ((int) digest[i]) & 0xFF;
        }
        return h;
    }
    */

    public static int compareIds(String id1, String id2) {
        BigInteger value1 = new BigInteger(id1, base);
        BigInteger value2 = new BigInteger(id2, base);
        return value1.compareTo(value2);
    }

}