package testing;

import app_kvEcs.ECScm;
import client.KVStore;
import common.messages.KVMessage;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.IOException;

/**
 * This test case tests the basic functionalities
 * of the distributed key-value service
 * Created by akanthos on 24.11.15.
 */
public class KVServiceBasicTest extends TestCase {
    private KVStore kvClient;
    private ECScm Ecs;

    public void setUp() {

        try {
            Ecs = new ECScm("ecs.config.small");
        } catch (IOException e) {
            e.printStackTrace();
        }
        Ecs.initService(3, 10, "FIFO");

        kvClient = new KVStore();
        try {
            kvClient.connect("localhost", 60000);
        } catch (Exception e) {
        }
    }

    public void tearDown() {
        kvClient.disconnect();
        Ecs.shutdown();
    }

    @Test
    public void testConnectionToServiceSuccess() {

        Exception ex = null;

        KVStore kvClient = new KVStore();
        try {
            kvClient.connect("localhost", 60000);
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
    }

    @Test
    public void testGetRequestToService() {

        String key = "foo";
        String value = "bar";
        KVMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, value);
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getValue().equals("bar"));
    }

    @Test
    public void testPutRequestToService() {

        String key = "foobar";
        String value = "bar";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.PUT_SUCCESS);
    }

    @Test
    public void testPutRequestToLockedService() {

        String key = "foobar";
        String value = "bar";
        KVMessage response = null;
        Exception ex = null;

        Ecs.lockWrite();

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.SERVER_WRITE_LOCK);
    }

    @Test
    public void testGetRequestToLockedService() {

        String key = "foo";
        String value = "bar";
        KVMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, value);
            Ecs.lockWrite();
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getValue().equals("bar"));
    }

    @Test
    public void testRequestToStoppedService() {

        String key = "foobar";
        String value = "bar";
        KVMessage response = null;
        Exception ex = null;

        Ecs.stop();

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == KVMessage.StatusType.SERVER_STOPPED);
    }

    @Test
    public void testGetToReducedAndAugmentedService() {

        String key = "foo";
        String value = "bar";
        KVMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, value);
            Ecs.removeNode();
            Ecs.removeNode();
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getValue().equals("bar"));

        try {
            Ecs.addNode(10, "FIFO");
            Ecs.addNode(20, "FIFO");
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getValue().equals("bar"));
    }

    @Test
    public void testGetToShutdownService() {

        String key = "foo";
        String value = "bar";
        KVMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, value);
            Ecs.shutdown();
            response = kvClient.get(key);
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertNotNull(ex);
    }


}
