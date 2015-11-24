package testing;

import client.KVStore;
import client.ServerConnection;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.logging.Level;

/**
 * Created by sreenath on 24.11.15.
 */
public class ClientTest extends TestCase {

    @Test()
    public void testServerConnectionwthInvalidValues() throws IOException {
        String testHost = "localhost";
        int testPort = 34567;
        Exception ex = null;
        try {
            ServerConnection serverConnection = new ServerConnection(testHost, testPort);
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex instanceof IOException);
    }

    @Test
    public void testLogLevel() {
        KVStore store = new KVStore();
        store.logLevel("info");
        assertEquals(store.getLogLevel().toString(), Level.INFO.toString());
        store.logLevel("somerandomstring");
        assertEquals(store.getLogLevel().toString(), Level.INFO.toString());
    }
}
