package testing;

import app_kvEcs.ECSImpl;
import client.KVStore;
import common.messages.KVMessage;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

/**
 * This test case is a stress test for the
 * distributed key-value service
 * Created by akanthos on 24.11.15.
 */
public class KVServiceStressTest extends TestCase {
    private KVStore kvClient;
    private ECSImpl Ecs;
    private ExecutorService threadpool;
    private double difference;


    public void setUp() {
        threadpool = Executors.newCachedThreadPool();
        try {
            Ecs = new ECSImpl("ecs.config");
        } catch (IOException e) {
            e.printStackTrace();
        }
        Ecs.initService(50, 50, "FIFO");

    }

    public void tearDown() {
        Ecs.shutdown();
        threadpool.shutdownNow();
        System.out.println("Seconds: " + difference);
    }

    /**
     * This test creates 1000 clients and they all send a put command
     * to the server. The results returned by the server need to be
     * all PUT_SUCCESS.
     */
    @Test
    public void testPutResultManyClients() {
        final Integer NUMBER_OF_CLIENTS = 1000;
        List<Callable<KVMessage>> tasks = new ArrayList<>();
        try {
            for(int i = 0; i < NUMBER_OF_CLIENTS; i++)
            {
                tasks.add(new ClientResult(i));
            }
            // Timer start
            long start_time = System.nanoTime();
            List<Future<KVMessage>> futures = threadpool.invokeAll(tasks);
            for (Future<KVMessage> f : futures) {
                assertTrue(f.get().getStatus() == KVMessage.StatusType.PUT_SUCCESS);
            }
            // Timer stop
            long end_time = System.nanoTime();
            difference = (end_time - start_time)/1e9;

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    class ClientResult implements Callable<KVMessage> {
        protected KVStore kvClient;
        protected Integer i;

        public ClientResult(Integer i) throws Exception{
            this.i = i;
            kvClient = new KVStore();
            try {
                int max = 49;
                int min = 0;
                Random rand = new Random();
                int randomNum = rand.nextInt((max - min) + 1) + min;
                kvClient.connect("localhost", 50000+randomNum);
                System.out.println("Client " + i + ": Connected");
            } catch (Exception e) {
                System.out.println("Client " + i + ": Cannot connect");
            }

        }

        @Override
        public KVMessage call() {
            String key = "foo" + i;
            String value = "bar" + i;
            KVMessage response = null;
            Exception ex = null;

            try {
                response = kvClient.put(key, value);
            } catch (Exception e) {
                ex = e;
            }
            return response;
        }
    }
}
