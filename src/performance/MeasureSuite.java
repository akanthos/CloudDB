package performance;

import app_kvEcs.ECSImpl;
import client.KVStore;
import common.messages.KVMessage;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

/**
 * Created by akanthos on 24.11.15.
 */
public class MeasureSuite {

    private static final int MAX_SERVERS = 3;
    private static final int SERVER_COUNT_STEP = 1;
    private static final int MAX_CLIENTS = 50;
    private static final int CLIENT_COUNT_STEP = 10;
    private static ExecutorService threadpool;
    private static PrintWriter resultsFile;


    private static ECSImpl ecs;
    private static double difference;

    public static void main (String[] args) {

        try {
            /* Output file and client threadpool initialization */
            SimpleDateFormat currentTimeAndDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            resultsFile = new PrintWriter("results_" +  currentTimeAndDate.format(new Date()).toString() + ".txt" , "UTF-8");
            threadpool = Executors.newCachedThreadPool();

            /* Service administration initialization */
            ecs = new ECSImpl("ecs.config.small");

            /* Titles of CSV output file */
            resultsFile.println("result, servers, clients, cacheSize, strategy, time (sec)");

            /* Start measurements */
            for (int serverCount = 3 ; serverCount <= MAX_SERVERS ; serverCount+=SERVER_COUNT_STEP ) {
                for (String strategy : new String[]{"FIFO", "LRU", "LFU"}) {
                    for (int cacheSize : new int[]{10, 30, 50}) {
                        for (int clientCount = 1 ; clientCount <= MAX_CLIENTS; clientCount += CLIENT_COUNT_STEP) {

                            /*  Deleting old data.store files */
                            File folder = new File(".");
                            File[] files = folder.listFiles( new FilenameFilter() {
                                @Override
                                public boolean accept( final File dir,
                                                       final String name ) {
                                    return name.matches( "data\\.store.*" );
                                }
                            } );
                            for ( File file : files ) {
                                if ( !file.delete() ) {
                                    System.err.println( "Can't remove " + file.getAbsolutePath() );
                                }
                            }

                            /* Initialize servers */
                            ecs.initService(serverCount, cacheSize, strategy);
                            ecs.start();

                            /* Run the clients and time the performance */
                            runClients(serverCount, clientCount, cacheSize, strategy);

                            /* Shut down the servers */
                            ecs.shutdown();

                            if (clientCount==1) { clientCount=0; }
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            /* Close results file and shut down client thread pool */
            resultsFile.close();
            threadpool.shutdownNow();
        }
    }

    private static void runClients(int serverCount, int clientCount, int cacheSize, String strategy) {
        List<Callable<KVMessage>> tasks = new ArrayList<>();
        try {
            // Create clients
            for(int i = 0; i < clientCount; i++)
            {
                tasks.add(new ClientResult(i));
            }

            // Timer start
            long start_time = System.nanoTime();

            // Invoke clients
            List<Future<KVMessage>> futures = threadpool.invokeAll(tasks);

            // Wait for results
            for (Future<KVMessage> f : futures) {
                if (f.get().getStatus() != KVMessage.StatusType.PUT_SUCCESS) {
                    System.out.println("error");
                }
            }

            // Timer stop
            long end_time = System.nanoTime();
            difference = (end_time - start_time)/1e9;

            // Output to results file
            resultsFile.println("result, " +
                    serverCount + " , " +
                    clientCount + " , " +
                    cacheSize + " , " +
                    strategy + " , " +
                    difference);



        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


    static class ClientResult implements Callable<KVMessage> {
        protected KVStore kvClient;
        protected Integer i;

        public ClientResult(Integer i) throws Exception{
            this.i = i;
            kvClient = new KVStore();
            try {
                int max = 2;
                int min = 0;
                Random rand = new Random();
                int randomNum = rand.nextInt((max - min) + 1) + min;
                kvClient.connect("localhost", 60000+randomNum);
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
