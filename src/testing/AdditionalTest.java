package testing;

import app_kvServer.KVCache;
import client.KVStore;
import common.messages.KVMessage;
import org.junit.Test;

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

public class AdditionalTest extends TestCase {

	private ExecutorService threadpool;

	public void setUp() {

		threadpool = Executors.newFixedThreadPool(15);
	}

	public void tearDown() {
		threadpool.shutdownNow();
	}


	/**
	 * This test creates 500 clients and they all try to connect concurrently
	 * to the server.
	 */
	@Test
	public void testConnectManyClients() {
		final Integer NUMBER_OF_CLIENTS = 100;
		Exception[] ex = new Exception[NUMBER_OF_CLIENTS];

		for(int i = 0; i < NUMBER_OF_CLIENTS; i++) {
			KVStore client = new KVStore("localhost", 50000);
			try {
				client.connect();
			} catch (Exception e) {
				ex[i] = e;
			}
		}
		for(int i = 0; i < NUMBER_OF_CLIENTS; i++) {
			assertNull(ex[i]);

		}
	}


	/**
	 * This test creates 500 clients and they all send a put command
	 * to the server. The results returned by the server need to be
	 * all PUT_SUCCESS.
	 */
	@Test
	public void testPutResultManyClients() {
		final Integer NUMBER_OF_CLIENTS = 500;
		List<Callable<KVMessage>> tasks = new ArrayList<>();
		try {
			for(int i = 0; i < NUMBER_OF_CLIENTS; i++)
			{
				tasks.add(new ClientResult(i));
			}
			List<Future<KVMessage>> futures = threadpool.invokeAll(tasks);
			for (Future<KVMessage> f : futures) {
				assertTrue(f.get(8, TimeUnit.SECONDS).getStatus() == KVMessage.StatusType.PUT_SUCCESS);
			}
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
			kvClient = new KVStore("localhost", 50000);
			try {
				kvClient.connect();
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

