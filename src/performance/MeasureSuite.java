package performance;

import app_kvEcs.ECSCore;
import client.KVStore;
import common.ServerInfo;
import common.messages.KVMessage;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by akanthos on 24.11.15.
 */
public class MeasureSuite {

    private static final int MAX_SERVERS = 10;
    private static final int SERVER_COUNT_STEP = 5;
    private static final int MAX_CLIENTS = 200;
    private static final int CLIENT_COUNT_STEP = 200;
    private static ExecutorService threadpool;
    private static PrintWriter resultsFile;
    private static HashMap<String, String> randomKVs;
    private static final int MAP_SIZE = 1000;
    private static RandomKeyValue randomKeyValue;


    private static ECSCore ecs;
    private static double difference;

    public static void main (String[] args) {



        /****************************************************************************/
        /*                      MEASURING PUT THROUGHPUT                            */
        /****************************************************************************/
        measurePuts();

        /****************************************************************************/
        /*                      MEASURING GET THROUGHPUT                            */
        /****************************************************************************/
        createRandomKeyValues();
        measureGets();


    }

    private static void measureGets() {
        try {
            /* Output file and client threadpool initialization */
            SimpleDateFormat currentTimeAndDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            resultsFile = new PrintWriter("GET_results_" + currentTimeAndDate.format(new Date()) + ".txt" , "UTF-8");
            threadpool = Executors.newCachedThreadPool();

            /* Service administration initialization */
            ecs = new ECSCore("ecs.config");

            /* Titles of CSV output file */
            resultsFile.println("result, servers, clients, cacheSize, strategy, time (sec), throughput");

            /* Start measurements */
            for (int serverCount = 1 ; serverCount <= MAX_SERVERS ; serverCount+=SERVER_COUNT_STEP ) {
                for (String strategy : new String[]{"FIFO", "LRU", "LFU"}) {
                    for (int cacheSize : new int[]{20, 200}) {
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
                        insertKVsToService();

                        for (int clientCount = MAX_CLIENTS ; clientCount <= MAX_CLIENTS; clientCount += CLIENT_COUNT_STEP) {
                            /* Insert random key values to service */

                            /* Run the clients and time the performance */
                            runGetClients(serverCount, clientCount, cacheSize, strategy);

                            threadpool.shutdownNow();
                            threadpool = Executors.newCachedThreadPool();
                            if (clientCount==1) { clientCount=0; }
                        }
                        /* Shut down the servers */
                        ecs.shutdown();
                    }
                }
                if (serverCount==1) { serverCount=0; }
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
    private static void measurePuts() {
        try {
            /* Output file and client threadpool initialization */
            SimpleDateFormat currentTimeAndDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            resultsFile = new PrintWriter("PUT_results_" + currentTimeAndDate.format(new Date()) + ".txt" , "UTF-8");
            threadpool = Executors.newCachedThreadPool();

            /* Service administration initialization */
            ecs = new ECSCore("ecs.config");

            /* Titles of CSV output file */
            resultsFile.println("result, servers, clients, cacheSize, strategy, time (sec), throughput");

            /* Start measurements */
            for (int serverCount = 1 ; serverCount <= MAX_SERVERS ; serverCount+=SERVER_COUNT_STEP ) {
                for (String strategy : new String[]{"FIFO", "LRU", "LFU"}) {
                    for (int cacheSize : new int[]{20, 200}) {
                        for (int clientCount = MAX_CLIENTS ; clientCount <= MAX_CLIENTS; clientCount += CLIENT_COUNT_STEP) {

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
                            runPutClients(serverCount, clientCount, cacheSize, strategy);

                            /* Shut down the servers */
                            ecs.shutdown();


//                            Process proc;
//                            Runtime run = Runtime.getRuntime();
//                            try {
//                                Thread.sleep(2000);
//                                proc = run.exec("./javakill.sh");
//                            } catch (IOException e) {
//                                e.printStackTrace();
//                            } catch (InterruptedException e) {
//                                e.printStackTrace();
//                            }

                            threadpool.shutdownNow();
                            threadpool = Executors.newCachedThreadPool();
                            if (clientCount==1) { clientCount=0; }
                        }
                    }
                }
                if (serverCount==1) { serverCount=0; }
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

    private static void runGetClients(int serverCount, int clientCount, int cacheSize, String strategy) {
        ServerInfo entryServer = ecs.getEntryServer();
        List<Callable<KVMessage>> tasks = new ArrayList<>();
        try {
            // Create clients
            for(int i = 0; i < clientCount; i++)
            {
                tasks.add(new ClientGetter(i, entryServer.getServerPort()));
            }

            // Timer start
            long start_time = System.nanoTime();

            // Invoke clients
            List<Future<KVMessage>> futures = threadpool.invokeAll(tasks);

            // Wait for results
            for (Future<KVMessage> f : futures) {
                if (f.get().getStatus() != KVMessage.StatusType.GET_SUCCESS) {
//                    System.out.println("error");
                }
            }

            // Timer stop
            long end_time = System.nanoTime();
            difference = (end_time - start_time)/1e9;
            double throughput = clientCount / difference;

            // Output to results file
            resultsFile.println("result, " +
                    serverCount + " , " +
                    clientCount + " , " +
                    cacheSize + " , " +
                    strategy + " , " +
                    difference + " , " +
                    throughput
            );



        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void runPutClients(int serverCount, int clientCount, int cacheSize, String strategy) {
        ServerInfo entryServer = ecs.getEntryServer();
        List<Callable<KVMessage>> tasks = new ArrayList<>();
        try {
            // Create clients
            for(int i = 0; i < clientCount; i++)
            {
                tasks.add(new ClientPutter(i, entryServer.getServerPort()));
            }

            // Timer start
            long start_time = System.nanoTime();

            // Invoke clients
            List<Future<KVMessage>> futures = threadpool.invokeAll(tasks);

            // Wait for results
            for (Future<KVMessage> f : futures) {
                KVMessage.StatusType status = f.get().getStatus();
                if ( status != KVMessage.StatusType.PUT_SUCCESS
                        && status != KVMessage.StatusType.PUT_UPDATE
                        && status != KVMessage.StatusType.DELETE_SUCCESS) {
                    System.out.println("error");
                }
            }

            // Timer stop
            long end_time = System.nanoTime();
            difference = (end_time - start_time)/1e9;
            double throughput = clientCount / difference;

            // Output to results file
            resultsFile.println("result, " +
                    serverCount + " , " +
                    clientCount + " , " +
                    cacheSize + " , " +
                    strategy + " , " +
                    difference + " , " +
                    throughput
            );



        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    static class ClientPutter implements Callable<KVMessage> {
        protected KVStore kvClient;
        protected Integer i;

        public ClientPutter(Integer i, int port) throws Exception{
            this.i = i;
            kvClient = new KVStore();
            try {
                kvClient.connect("127.0.0.1", port);
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
    static class ClientGetter implements Callable<KVMessage> {
        protected KVStore kvClient;
        protected Integer i;

        public ClientGetter(Integer i, int port) throws Exception{
            this.i = i;
            kvClient = new KVStore();
            try {
                kvClient.connect("127.0.0.1", port);
                System.out.println("Client " + i + ": Connected");
            } catch (Exception e) {
                System.out.println("Client " + i + ": Cannot connect");
            }

        }

        @Override
        public KVMessage call() {

            String key = randomKeyValue.getRandomKey();
            KVMessage response = null;
            Exception ex = null;

            try {
                response = kvClient.get(key);
            } catch (Exception e) {
                ex = e;
            }

//            if (response.getStatus() != KVMessage.StatusType.GET_SUCCESS) {
//                System.out.println("ERROR");
//            }
//            else {
//                System.out.println("SUCCESS");
//            }

            return response;
        }
    }

    private static void insertKVsToService() {

        ServerInfo entryServer = ecs.getEntryServer();

        KVStore kvClient = new KVStore();
        KVMessage reply;
        try {
            kvClient.connect("127.0.0.1", entryServer.getServerPort());
            System.out.println("Client for inserting random keys connected");
            for (int i=0; i<MAP_SIZE; i++) {
                String randomKey = randomKeyValue.getRandomKey();
                String randomValue = randomKVs.get(randomKey);
                reply = kvClient.put(randomKey, randomValue);
                if (reply.getStatus() != KVMessage.StatusType.PUT_SUCCESS
                        && reply.getStatus() != KVMessage.StatusType.PUT_UPDATE
                        ) {
                    System.out.println("Key not inserted!!!");
                }
            }
        } catch (Exception e) {

        }
    }
    private static void createRandomKeyValues() {
        RandomString randomStringGenerator = new RandomString(5);
        randomKVs = new HashMap<>();
        for (int i=0; i<MAP_SIZE; i++) {
            String key = randomStringGenerator.nextString();
            String value = randomStringGenerator.nextString();
            randomKVs.put(key, value);
            //System.out.println("Inserted pair: (" +key+","+value+")");
        }
        randomKeyValue = new RandomKeyValue(randomKVs);
    }

}

